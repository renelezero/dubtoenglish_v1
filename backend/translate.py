from openai import AsyncOpenAI

_client: AsyncOpenAI | None = None

SYSTEM_PROMPT = (
    "You are a professional Arabic-to-English news translator. "
    "Translate the following Arabic text into clear, natural English suitable for "
    "a live news broadcast. Preserve the meaning, tone, and any proper nouns "
    "(transliterate names and places). Keep the translation concise — do not add "
    "commentary or explanation. If the input is empty or unintelligible, respond "
    "with an empty string."
)


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


async def translate_text(arabic_text: str, context: str = "") -> str:
    """
    Translate Arabic text to English using GPT-4o.

    An optional `context` parameter can carry the previous few translated
    sentences so the model can maintain coherence across chunks.
    """
    if not arabic_text.strip():
        return ""

    client = _get_client()

    messages: list[dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT}]

    if context:
        messages.append(
            {
                "role": "system",
                "content": f"Recent translation context (for coherence):\n{context}",
            }
        )

    messages.append({"role": "user", "content": arabic_text})

    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        temperature=0.3,
        max_tokens=1024,
    )

    return (response.choices[0].message.content or "").strip()
