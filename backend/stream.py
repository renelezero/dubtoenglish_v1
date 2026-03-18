import asyncio
import logging
import os
import tempfile
from typing import AsyncGenerator

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.15
_WAV_HEADER_SIZE = 44

LIVE_STREAMS = [
    {
        "id": "alarabiya_live",
        "name": "Al Arabiya LIVE",
        "url": "https://live.alarabiya.net/alarabiapublish/alarabiya.smil/playlist.m3u8",
    },
    {
        "id": "aljazeera_live",
        "name": "Al Jazeera LIVE",
        "url": "https://live-hls-apps-aja-fa.getaj.net/AJA/index.m3u8",
    },
    {
        "id": "alghad_live",
        "name": "Al Ghad LIVE",
        "url": "https://eazyvwqssi.erbvr.com/alghadtv/alghadtv.m3u8",
    },
    {
        "id": "alaraby_live",
        "name": "Al Araby LIVE",
        "url": "https://live.kwikmotion.com/alaraby1live/alaraby_abr/playlist.m3u8",
    },
]


async def _drain_stderr(stderr: asyncio.StreamReader) -> list[str]:
    lines: list[str] = []
    async for raw in stderr:
        text = raw.decode(errors="replace").rstrip()
        if text:
            lines.append(text)
            logger.warning("ffmpeg: %s", text)
    return lines


async def capture_audio_chunks(
    hls_url: str,
    chunk_duration: float = 5.0,
    output_dir: str | None = None,
) -> AsyncGenerator[str, None]:
    """
    Capture audio from an HLS live stream using a single persistent
    ffmpeg process with the segment muxer. Yields file paths of completed
    WAV chunks (mono 16 kHz) as they become available.
    """
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="hls_chunks_")
    os.makedirs(output_dir, exist_ok=True)

    chunk_pattern = os.path.join(output_dir, "chunk_%06d.wav")

    cmd = [
        "ffmpeg",
        "-loglevel", "error",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", hls_url,
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-f", "segment",
        "-segment_time", str(chunk_duration),
        "-segment_format", "wav",
        chunk_pattern,
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    stderr_task = asyncio.create_task(_drain_stderr(proc.stderr))
    chunk_index = 0

    try:
        while True:
            chunk_path = os.path.join(
                output_dir, f"chunk_{chunk_index:06d}.wav"
            )
            next_chunk_path = os.path.join(
                output_dir, f"chunk_{chunk_index + 1:06d}.wav"
            )

            while not os.path.exists(next_chunk_path):
                if proc.returncode is not None:
                    if (
                        os.path.exists(chunk_path)
                        and os.path.getsize(chunk_path) > _WAV_HEADER_SIZE
                    ):
                        yield chunk_path
                    stderr_lines = await stderr_task
                    detail = (
                        "; ".join(stderr_lines[-5:])
                        if stderr_lines
                        else "no output"
                    )
                    raise RuntimeError(
                        f"ffmpeg exited with code {proc.returncode}: {detail}"
                    )
                await asyncio.sleep(_POLL_INTERVAL)

            if os.path.getsize(chunk_path) > _WAV_HEADER_SIZE:
                yield chunk_path
            chunk_index += 1
    finally:
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
        stderr_task.cancel()


def cleanup_chunk(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
