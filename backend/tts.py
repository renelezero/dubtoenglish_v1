import tempfile
import os

from openai import AsyncOpenAI

_client: AsyncOpenAI | None = None

VOICES = ("alloy", "ash", "ballad", "coral", "echo", "fable", "nova", "onyx", "sage", "shimmer")
DEFAULT_VOICE = "nova"


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


async def synthesize_speech(
    text: str,
    voice: str = DEFAULT_VOICE,
    speed: float = 1.25,
    output_dir: str | None = None,
) -> str:
    """
    Convert English text to speech using the OpenAI TTS API.

    Returns the file path of the generated MP3 audio.
    """
    if not text.strip():
        return ""

    if voice not in VOICES:
        voice = DEFAULT_VOICE

    client = _get_client()

    response = await client.audio.speech.create(
        model="tts-1",
        voice=voice,
        input=text,
        response_format="mp3",
        speed=speed,
    )

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="dub_tts_")
    os.makedirs(output_dir, exist_ok=True)

    audio_path = os.path.join(output_dir, "tts_output.mp3")
    response.write_to_file(audio_path)

    return audio_path


async def synthesize_speech_bytes(
    text: str,
    voice: str = DEFAULT_VOICE,
    speed: float = 1.25,
) -> bytes:
    """
    Convert English text to speech and return raw MP3 bytes.

    Useful for streaming audio directly over a WebSocket without
    writing to disk.
    """
    if not text.strip():
        return b""

    if voice not in VOICES:
        voice = DEFAULT_VOICE

    client = _get_client()

    response = await client.audio.speech.create(
        model="tts-1",
        voice=voice,
        input=text,
        response_format="mp3",
        speed=speed,
    )

    return response.read()


def audio_to_base64(audio_bytes: bytes) -> str:
    """Encode raw audio bytes as a base64 string for WebSocket transport."""
    import base64
    return base64.b64encode(audio_bytes).decode("utf-8")
