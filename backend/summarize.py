from openai import AsyncOpenAI

_client: AsyncOpenAI | None = None

INCREMENTAL_PROMPT = """\
You are an intelligence analyst monitoring Arabic news sources in real-time.

You will receive:
1. Recent translated news headlines and summaries from the last few minutes
2. A log of what you have ALREADY reported (if any)

Produce a SHORT spoken briefing covering ONLY new developments.

Rules:
- Do NOT repeat anything from the previous log
- 1-3 concise bullet points max
- After each bullet, note the source in parentheses
- If nothing is genuinely new, respond with exactly: NO_UPDATE
- Write for reading aloud — natural spoken English, like a news anchor
- Be factual and neutral"""

LOOKBACK_PROMPT = """\
You are an intelligence analyst. Given translated news items from Arabic sources \
over the last {hours} hour(s), produce a concise briefing.

Format:
- Group by topic/region
- 1-2 sentence bullets, source in parentheses
- Consolidate duplicate stories across sources
- Order by severity/importance
- Start with a 1-sentence overview
- Under 400 words total"""


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


async def generate_incremental_update(headlines: str, previous_log: str) -> str | None:
    if not headlines.strip():
        return None

    client = _get_client()
    user = f"NEW ITEMS:\n{headlines}"
    if previous_log:
        user += f"\n\n---\nALREADY REPORTED:\n{previous_log}"

    resp = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": INCREMENTAL_PROMPT},
            {"role": "user", "content": user},
        ],
        temperature=0.3,
        max_tokens=512,
    )
    text = (resp.choices[0].message.content or "").strip()
    if not text or text == "NO_UPDATE":
        return None
    return text


async def generate_summary(headlines: str, hours: float) -> str:
    if not headlines.strip():
        return ""

    client = _get_client()
    resp = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": LOOKBACK_PROMPT.format(hours=hours)},
            {"role": "user", "content": headlines},
        ],
        temperature=0.3,
        max_tokens=2048,
    )
    return (resp.choices[0].message.content or "").strip()
