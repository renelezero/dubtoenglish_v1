"""
AI analyst agent.

Answers free-form questions ("who is fighting whom in Kirkuk?", "recent
arrests", "which militias are most active?") by grounding an LLM on the current
actor graph plus recent translated headlines. Read-only analysis over public
data — it explains what the collected reporting shows and cites nothing it was
not given.
"""

import logging

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

_client: AsyncOpenAI | None = None

ANALYST_MODEL = "gpt-4o"

ANALYST_PROMPT = """\
You are the analyst for an open-source intelligence platform monitoring Iraq, \
Kurdistan and the wider region. You answer questions using ONLY the CONTEXT \
provided below, which is derived entirely from public news reporting the \
platform has collected.

The context contains:
1. A snapshot of the actor relationship graph (top actors and key relations).
2. Recent translated headlines.

Guidance:
- Answer concisely and analytically, like a desk officer briefing a principal.
- Ground every claim in the context. If the context does not support an answer, \
say what is missing rather than speculating.
- When relevant, name the specific actors and describe relationships (who is \
aligned with / in conflict with whom).
- Distinguish clearly between what is reported and what is inference.
- Do NOT provide operational guidance for surveilling, targeting, or harming \
specific individuals. Keep the analysis at the level of open-source situational \
awareness.
- Neutral, factual tone. No fabricated figures or sources."""


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


async def answer_question(question: str, graph_context: str, headlines: str) -> str:
    if not question.strip():
        return "Ask a question about the current situation, actors, or relationships."

    context = "=== ACTOR GRAPH SNAPSHOT ===\n"
    context += graph_context or "(no actors mapped yet)"
    context += "\n\n=== RECENT HEADLINES ===\n"
    context += headlines or "(no recent headlines)"
    if len(context) > 14000:
        context = context[:14000]

    user = f"CONTEXT:\n{context}\n\n---\nQUESTION: {question.strip()}"

    client = _get_client()
    try:
        resp = await client.chat.completions.create(
            model=ANALYST_MODEL,
            messages=[
                {"role": "system", "content": ANALYST_PROMPT},
                {"role": "user", "content": user},
            ],
            temperature=0.3,
            max_tokens=900,
            timeout=90,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception:
        logger.exception("Analyst answer failed")
        return "The analyst is temporarily unavailable (model call failed)."
