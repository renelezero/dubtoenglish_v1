import asyncio
import logging
import os
import tempfile
from typing import AsyncGenerator

import yt_dlp

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.15
_WAV_HEADER_SIZE = 44  # bytes; anything at or below this is an empty WAV


def extract_audio_url(youtube_url: str) -> str:
    """Use yt-dlp to extract the direct audio stream URL from a YouTube Live URL."""
    ydl_opts = {
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(youtube_url, download=False)

        if info.get("url"):
            return info["url"]

        # Fallback: pick the best audio format explicitly from the formats list
        formats = info.get("formats", [])
        audio_only = [
            f
            for f in formats
            if f.get("acodec") != "none"
            and f.get("vcodec") in ("none", None)
        ]
        candidates = audio_only or [
            f for f in formats if f.get("acodec") != "none"
        ]
        if not candidates:
            raise RuntimeError("No audio stream found for this URL")
        best = max(candidates, key=lambda f: f.get("abr") or 0)
        return best["url"]


async def _drain_stderr(stderr: asyncio.StreamReader) -> list[str]:
    """Read stderr lines in the background so the pipe buffer never fills."""
    lines: list[str] = []
    async for raw in stderr:
        text = raw.decode(errors="replace").rstrip()
        if text:
            lines.append(text)
            logger.warning("ffmpeg: %s", text)
    return lines


async def capture_audio_chunks(
    audio_url: str,
    chunk_duration: float = 5.0,
    output_dir: str | None = None,
) -> AsyncGenerator[str, None]:
    """
    Continuously capture audio from a live stream using a single persistent
    ffmpeg process with the segment muxer.

    Yields the file path of each completed WAV chunk (~chunk_duration seconds,
    mono 16 kHz — optimal for Whisper) as it becomes available.
    """
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="dub_chunks_")
    os.makedirs(output_dir, exist_ok=True)

    chunk_pattern = os.path.join(output_dir, "chunk_%06d.wav")

    cmd = [
        "ffmpeg",
        "-loglevel", "error",
        "-i", audio_url,
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

            # A segment is fully written once ffmpeg opens the next one.
            while not os.path.exists(next_chunk_path):
                if proc.returncode is not None:
                    # ffmpeg exited — yield the final chunk if valid, then stop.
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
    """Remove a processed audio chunk from disk."""
    try:
        os.remove(path)
    except OSError:
        pass
