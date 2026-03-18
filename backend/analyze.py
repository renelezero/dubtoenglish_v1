import json
import logging

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

_client: AsyncOpenAI | None = None

SYSTEM_PROMPT = """\
You are an Arabic news analyst. Given an Arabic news headline and optional body text, \
return a JSON object with these exact keys:

{
  "headline_en": "Accurate English translation of the headline",
  "summary_en": "1-2 sentence English summary",
  "locations": [{"name": "Place name", "lat": 33.3, "lng": 44.4}],
  "topics": ["politics"],
  "people": ["Person Name"],
  "severity": "medium"
}

Rules:
- Translate accurately; transliterate proper nouns (names, places)
- Extract ALL geographic locations mentioned with approximate lat/lng
- topics: pick from military, politics, economy, humanitarian, diplomacy, protest, terrorism, other
- severity: critical = active conflict/casualties, high = major political event, \
  medium = significant development, low = routine news
- If the input is already English (from GDELT), just summarize it
- Always return valid JSON, nothing else"""


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


async def analyze_item(item: dict) -> dict | None:
    """
    Run a single GPT-4o call to translate, extract entities, and classify.
    Returns structured event dict or None on failure.
    """
    headline = item.get("headline_ar", "")
    body = item.get("body_ar", "")
    if not headline and not body:
        return None

    user_text = f"Headline: {headline}"
    if body:
        user_text += f"\n\nBody: {body[:600]}"

    client = _get_client()
    try:
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            temperature=0.2,
            max_tokens=512,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or "{}"
        parsed = json.loads(raw)
    except Exception:
        logger.exception("Analyze failed for: %s", headline[:80])
        return None

    pre_lat = item.get("pre_lat")
    pre_lng = item.get("pre_lng")
    locations = parsed.get("locations", [])
    if pre_lat is not None and pre_lng is not None and not locations:
        locations = [{"name": parsed.get("headline_en", "Event"), "lat": pre_lat, "lng": pre_lng}]

    return {
        "id": item["id"],
        "source_id": item.get("source_id", ""),
        "source_name": item.get("source_name", ""),
        "headline_ar": headline,
        "headline_en": parsed.get("headline_en", headline),
        "summary_en": parsed.get("summary_en", ""),
        "locations": locations,
        "topics": parsed.get("topics", []),
        "people": parsed.get("people", []),
        "severity": parsed.get("severity", "low"),
        "url": item.get("url", ""),
        "origin": item.get("origin", "rss"),
    }
