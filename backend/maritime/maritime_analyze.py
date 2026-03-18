"""
GPT-4o maritime intelligence analysis.

Cross-references vessel traffic data with Arabic news to produce
maritime situational awareness briefings.
"""

import json
import logging
from typing import Optional, List

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

_client: Optional[AsyncOpenAI] = None

MARITIME_SYSTEM_PROMPT = """\
You are a maritime intelligence analyst specializing in the Strait of Hormuz and Persian Gulf.
You cross-reference vessel traffic data with Arabic news to produce actionable intelligence.

Given vessel traffic stats, anomalies, and recent Arabic news headlines, return a JSON object:

{
  "maritime_briefing": "2-4 sentence situational summary of current Hormuz strait activity",
  "threat_level": "normal|elevated|high|critical",
  "threat_reasoning": "Brief explanation of threat assessment",
  "correlations": [
    {
      "news_headline": "The headline that correlates",
      "maritime_observation": "What the vessel data shows",
      "significance": "Why this correlation matters",
      "confidence": "high|medium|low"
    }
  ],
  "key_observations": [
    "Notable vessel traffic patterns",
    "Unusual movements or AIS gaps",
    "Tanker flow changes"
  ],
  "watchlist": [
    {
      "vessel_name": "Name if known",
      "mmsi": 123456789,
      "reason": "Why this vessel is notable",
      "ship_type": "tanker"
    }
  ]
}

Rules:
- Focus on geopolitical significance: sanctions, military posturing, oil supply disruptions
- Flag AIS gaps on tankers as potential sanctions evasion (dark fleet activity)
- Military vessel presence near the strait is always notable
- Correlate tanker flow direction changes with news about oil markets or Gulf tensions
- Iranian, North Korean, and Venezuelan-flagged tankers merit extra scrutiny
- Always return valid JSON, nothing else"""


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI()
    return _client


async def analyze_maritime_situation(
    vessel_stats: dict,
    tanker_flow: dict,
    anomalies: List[dict],
    recent_news: str,
    active_vessel_count: int,
) -> Optional[dict]:
    """
    Run GPT-4o analysis correlating vessel data with news.
    Returns structured maritime intelligence assessment.
    """
    user_text = f"""## Current Vessel Traffic (Strait of Hormuz)
- Active vessels: {active_vessel_count}
- Tankers: {vessel_stats.get('tanker_count', 0)}
- Military: {vessel_stats.get('military_count', 0)}
- Average speed: {vessel_stats.get('avg_speed_knots', 0)} knots
- Vessel types: {json.dumps(vessel_stats.get('vessel_types', {}))}
- Top flags: {json.dumps(vessel_stats.get('top_flags', {}))}
- Top destinations: {json.dumps(vessel_stats.get('top_destinations', {}))}

## Tanker Flow
- Inbound (into Gulf): {tanker_flow.get('inbound_tankers', 0)}
- Outbound (from Gulf): {tanker_flow.get('outbound_tankers', 0)}
- Total moving: {tanker_flow.get('total_moving_tankers', 0)}

## Detected Anomalies ({len(anomalies)} total)
"""

    for a in anomalies[:10]:
        user_text += f"- [{a.get('type', '')}] {a.get('description', '')} (severity: {a.get('severity', 'low')})\n"

    user_text += f"""
## Recent Arabic News Headlines
{recent_news or 'No recent news available'}
"""

    client = _get_client()
    try:
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": MARITIME_SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            temperature=0.3,
            max_tokens=1024,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or "{}"
        return json.loads(raw)
    except Exception:
        logger.exception("Maritime analysis failed")
        return None


MARITIME_NEWS_SYSTEM_PROMPT = """\
You are an Arabic news analyst focused on maritime and shipping news.
Given an Arabic headline and body about maritime/shipping/naval topics,
return a JSON object:

{
  "headline_en": "English translation",
  "summary_en": "1-2 sentence summary focused on maritime significance",
  "locations": [{"name": "Place", "lat": 25.0, "lng": 56.0}],
  "topics": ["maritime"],
  "vessel_names": ["Any vessel names mentioned"],
  "port_names": ["Any port names mentioned"],
  "is_hormuz_relevant": true,
  "severity": "medium"
}

Rules:
- is_hormuz_relevant: true if the news could affect Strait of Hormuz traffic
- severity: critical = naval confrontation/seizure, high = military deployment/sanctions,
  medium = trade disruption, low = routine maritime news
- Extract vessel names and port names when mentioned
- Always return valid JSON"""


async def analyze_maritime_news(item: dict) -> Optional[dict]:
    """Analyze a maritime-specific news item."""
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
                {"role": "system", "content": MARITIME_NEWS_SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            temperature=0.2,
            max_tokens=512,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or "{}"
        parsed = json.loads(raw)
    except Exception:
        logger.exception("Maritime news analysis failed: %s", headline[:80])
        return None

    return {
        "id": item["id"],
        "source_id": item.get("source_id", ""),
        "source_name": item.get("source_name", ""),
        "headline_ar": headline,
        "headline_en": parsed.get("headline_en", headline),
        "summary_en": parsed.get("summary_en", ""),
        "locations": parsed.get("locations", []),
        "topics": parsed.get("topics", []),
        "vessel_names": parsed.get("vessel_names", []),
        "port_names": parsed.get("port_names", []),
        "is_hormuz_relevant": parsed.get("is_hormuz_relevant", False),
        "severity": parsed.get("severity", "low"),
        "url": item.get("url", ""),
        "origin": item.get("origin", "gdelt_maritime"),
    }
