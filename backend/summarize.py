from openai import AsyncOpenAI

_client: AsyncOpenAI | None = None

INCREMENTAL_SYSTEM_PROMPT = """\
You are an expert news analyst monitoring multiple Arabic news streams translated to English in real-time.

You will receive:
1. New translated transcript chunks from the last ~30 seconds
2. A log of what you have ALREADY reported (if any)

Your job: produce a SHORT incremental update covering ONLY genuinely new information.

Rules:
- Do NOT repeat anything already in the previous log
- Only mention new developments, new facts, new quotes, or meaningful changes
- If the new transcripts contain no meaningfully new information (just repetition, filler, or ads), respond with exactly: NO_UPDATE
- Use 1-3 concise bullet points max
- After each bullet, note the source in parentheses, e.g. (Al Arabiya)
- Be factual and neutral — no commentary
- Write in a style suitable for reading aloud as a spoken briefing"""

LOOKBACK_SYSTEM_PROMPT = """\
You are an expert news analyst monitoring multiple Arabic news streams translated to English.

Given a collection of translated transcript chunks from the last {hours} hour(s), produce a clear, concise intelligence briefing in English.

Format:
- Use bullet points grouped by topic/story
- Each bullet should be 1-2 sentences max
- After each bullet, note the source stream in parentheses, e.g. (Al Arabiya)
- If multiple sources cover the same story, consolidate and cite all sources
- Order by importance/prominence
- At the top, include a 1-sentence overall summary
- If there are recurring/developing stories, flag them

Keep the entire summary under 500 words. Be factual and neutral — no commentary."""


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


def _format_transcripts(transcripts: list[dict]) -> str:
    chunks: list[str] = []
    for t in transcripts:
        ts = t.get("timestamp", "")
        src = t.get("stream_name", "Unknown")
        eng = t.get("english", "")
        if eng:
            chunks.append(f"[{ts}] ({src}): {eng}")
    combined = "\n".join(chunks)
    if len(combined) > 80000:
        combined = combined[-80000:]
    return combined


async def generate_summary(transcripts: list[dict], hours: float) -> str:
    """Full lookback summary for the HTTP endpoint."""
    if not transcripts:
        return ""

    combined = _format_transcripts(transcripts)
    client = _get_client()

    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": LOOKBACK_SYSTEM_PROMPT.format(hours=hours)},
            {"role": "user", "content": f"Here are the translated transcript chunks:\n\n{combined}"},
        ],
        temperature=0.3,
        max_tokens=2048,
    )
    return (response.choices[0].message.content or "").strip()


async def generate_incremental_update(
    transcripts: list[dict],
    previous_log: str,
) -> str | None:
    """
    Generate an incremental summary update.
    Returns the new bullet points, or None if there's nothing new.
    """
    if not transcripts:
        return None

    combined = _format_transcripts(transcripts)
    client = _get_client()

    user_content = f"NEW TRANSCRIPTS:\n{combined}"
    if previous_log:
        user_content += f"\n\n---\nALREADY REPORTED (do not repeat):\n{previous_log}"

    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": INCREMENTAL_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        temperature=0.3,
        max_tokens=512,
    )
    text = (response.choices[0].message.content or "").strip()

    if not text or text == "NO_UPDATE":
        return None

    return text
