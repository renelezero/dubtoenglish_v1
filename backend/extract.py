"""
Actor / relation extraction.

Reads a batch of already-translated news events and asks the model to pull out
the actors involved and the relations between them (who did what to whom). This
is what feeds the growing actor knowledge graph.

Operates only on public reporting that the platform has already collected.
"""

import json
import logging

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

_client: AsyncOpenAI | None = None

EXTRACT_MODEL = "gpt-4o-mini"

EXTRACT_PROMPT = """\
You are an OSINT analyst building a relationship graph of the actors shaping \
Iraq and the wider region (Baghdad, Erbil/Kurdistan, Kirkuk, Basra, etc.) from \
PUBLIC news reporting.

You will receive a numbered list of news items (English headline + summary). \
For EACH item, extract the actors and the relations between them.

Return ONLY a JSON object of this exact shape:
{
  "results": [
    {
      "ref": "<the ref id given for the item>",
      "actors": [
        {
          "name": "Canonical actor name (transliterate consistently)",
          "type": "person|tribe|militia|party|government|security|religious|company|state|organization|other",
          "dimensions": ["political","economic","military","religious"]
        }
      ],
      "relations": [
        {
          "source": "actor name (the one taking the action)",
          "target": "actor name (the one acted upon / other party)",
          "type": "clash|attack|arrest|sanction|protest|threat|ally|support|negotiate|meet|deal|other",
          "severity": "critical|high|medium|low",
          "snippet": "short phrase describing the interaction"
        }
      ]
    }
  ]
}

Rules:
- Actors are organizations, groups, tribes, militias, parties, government/security \
bodies, states, companies, religious institutions, or named individuals.
- Use the SAME canonical spelling for an actor everywhere so it de-duplicates \
(e.g. always "Popular Mobilization Forces", not sometimes "PMF").
- dimensions: political = governance/elections/parties; economic = trade, oil, \
finance, companies, sanctions on business; military = armed force, clashes, \
security ops; religious = clerical, sectarian, religious institutions. An actor \
may have several.
- Only include relations that the text actually reports. If an item is a generic \
headline with no interaction between two actors, give it actors but an empty \
relations list.
- Directionality: "source" is the actor performing the action (e.g. the force \
that attacked, the government that arrested).
- If an item has no identifiable actors, return empty "actors" and "relations" \
for that ref.
- Output ONLY the JSON object, nothing else."""


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


def _build_input(events: list[dict]) -> str:
    lines = []
    for ev in events:
        ref = ev.get("id", "")
        headline = ev.get("headline_en") or ev.get("headline_ar") or ""
        summary = ev.get("summary_en") or ""
        text = headline
        if summary:
            text += f". {summary}"
        lines.append(f"[ref={ref}] {text}")
    return "\n".join(lines)


async def extract_graph_batch(events: list[dict]) -> dict[str, dict]:
    """
    Extract actors + relations for a batch of events.
    Returns a mapping: event_id -> {"actors": [...], "relations": [...]}.
    """
    events = [e for e in events if (e.get("headline_en") or e.get("headline_ar"))]
    if not events:
        return {}

    user_text = _build_input(events)
    if len(user_text) > 12000:
        user_text = user_text[:12000]

    client = _get_client()
    try:
        resp = await client.chat.completions.create(
            model=EXTRACT_MODEL,
            messages=[
                {"role": "system", "content": EXTRACT_PROMPT},
                {"role": "user", "content": user_text},
            ],
            temperature=0.1,
            max_tokens=3000,
            response_format={"type": "json_object"},
            timeout=90,
        )
        raw = resp.choices[0].message.content or "{}"
        parsed = json.loads(raw)
    except Exception:
        logger.exception("Graph extraction failed")
        return {}

    results = parsed.get("results", [])
    if not isinstance(results, list):
        return {}

    out: dict[str, dict] = {}
    for r in results:
        ref = r.get("ref", "")
        if not ref:
            continue
        out[str(ref)] = {
            "actors": r.get("actors", []) or [],
            "relations": r.get("relations", []) or [],
        }
    return out
