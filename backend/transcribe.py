from openai import AsyncOpenAI

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


async def transcribe_audio(audio_path: str, language: str = "ar") -> str:
    """
    Send an audio file to the OpenAI Whisper API and return the transcribed text.
    Defaults to Arabic language hint for better accuracy on Arabic news.
    """
    client = _get_client()
    with open(audio_path, "rb") as f:
        response = await client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            language=language,
            response_format="text",
        )
    return response.strip()
