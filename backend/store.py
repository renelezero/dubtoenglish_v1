import logging
import threading
from collections import Counter, deque
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_events: deque[dict] = deque(maxlen=5000)
_summaries: deque[dict] = deque(maxlen=500)


def add_event(event: dict) -> dict:
    """Store an analyzed event. Returns the stored entry with ISO timestamp."""
    entry = {**event, "timestamp": datetime.now(timezone.utc)}
    with _lock:
        _events.append(entry)
    return _serialize_event(entry)


def _serialize_event(e: dict) -> dict:
    return {
        "id": e.get("id", ""),
        "timestamp": e["timestamp"].isoformat() if hasattr(e["timestamp"], "isoformat") else str(e["timestamp"]),
        "source_id": e.get("source_id", ""),
        "source_name": e.get("source_name", ""),
        "headline_ar": e.get("headline_ar", ""),
        "headline_en": e.get("headline_en", ""),
        "summary_en": e.get("summary_en", ""),
        "locations": e.get("locations", []),
        "topics": e.get("topics", []),
        "people": e.get("people", []),
        "severity": e.get("severity", "low"),
        "url": e.get("url", ""),
        "origin": e.get("origin", "rss"),
    }


def query_events(hours: float = 1.0) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    with _lock:
        return [_serialize_event(e) for e in _events if e["timestamp"] >= cutoff]


def get_event_count(hours: float = 1.0) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    with _lock:
        return sum(1 for e in _events if e["timestamp"] >= cutoff)


def get_stats(hours: float = 1.0) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    sources: Counter[str] = Counter()
    topics: Counter[str] = Counter()
    locations: Counter[str] = Counter()
    severities: Counter[str] = Counter()
    total = 0

    with _lock:
        for e in _events:
            if e["timestamp"] < cutoff:
                continue
            total += 1
            sources[e.get("source_name", "Unknown")] += 1
            severities[e.get("severity", "low")] += 1
            for t in e.get("topics", []):
                topics[t] += 1
            for loc in e.get("locations", []):
                locations[loc.get("name", "")] += 1

    return {
        "total": total,
        "sources": dict(sources.most_common(10)),
        "topics": dict(topics.most_common(10)),
        "locations": dict(locations.most_common(10)),
        "severities": dict(severities),
    }


def get_recent_headlines(hours: float = 0.5) -> str:
    """Format recent events as text for the AI summarizer."""
    events = query_events(hours)
    lines = []
    for e in events:
        src = e.get("source_name", "")
        headline = e.get("headline_en", "")
        summary = e.get("summary_en", "")
        if headline:
            lines.append(f"({src}) {headline}. {summary}")
    combined = "\n".join(lines)
    return combined[-8000:] if len(combined) > 8000 else combined


# ---- Summary storage ----

def add_summary(summary_type: str, text: str, event_count: int = 0) -> dict:
    entry = {
        "timestamp": datetime.now(timezone.utc),
        "type": summary_type,
        "text": text,
        "event_count": event_count,
    }
    with _lock:
        _summaries.append(entry)
    return {
        "timestamp": entry["timestamp"].isoformat(),
        "type": entry["type"],
        "text": entry["text"],
        "event_count": entry["event_count"],
    }


def query_summaries(hours: float = 6.0) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    with _lock:
        return [
            {
                "timestamp": e["timestamp"].isoformat(),
                "type": e["type"],
                "text": e["text"],
                "event_count": e.get("event_count", 0),
            }
            for e in _summaries
            if e["timestamp"] >= cutoff
        ]


def get_running_log(hours: float = 6.0) -> str:
    entries = query_summaries(hours)
    return "\n".join(e["text"] for e in entries if e["text"])
