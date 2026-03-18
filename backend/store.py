import json
import logging
import os
import sqlite3
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "data", "ein.db"))

_local = threading.local()


def _get_db() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        _local.conn = conn
    return _local.conn


def _init_db():
    db = _get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            source_id TEXT,
            source_name TEXT,
            headline_ar TEXT,
            headline_en TEXT,
            summary_en TEXT,
            locations TEXT,
            topics TEXT,
            people TEXT,
            severity TEXT DEFAULT 'low',
            url TEXT,
            origin TEXT DEFAULT 'rss'
        );
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_events_severity ON events(severity);

        CREATE TABLE IF NOT EXISTS summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            type TEXT,
            text TEXT,
            event_count INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_summaries_ts ON summaries(timestamp);
    """)
    db.commit()


_init_db()


def add_event(event: dict) -> dict:
    ts = datetime.now(timezone.utc).isoformat()
    eid = event.get("id", "")
    db = _get_db()
    try:
        db.execute(
            """INSERT OR IGNORE INTO events
               (id, timestamp, source_id, source_name, headline_ar, headline_en,
                summary_en, locations, topics, people, severity, url, origin)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                eid, ts,
                event.get("source_id", ""),
                event.get("source_name", ""),
                event.get("headline_ar", ""),
                event.get("headline_en", ""),
                event.get("summary_en", ""),
                json.dumps(event.get("locations", []), ensure_ascii=False),
                json.dumps(event.get("topics", []), ensure_ascii=False),
                json.dumps(event.get("people", []), ensure_ascii=False),
                event.get("severity", "low"),
                event.get("url", ""),
                event.get("origin", "rss"),
            ),
        )
        db.commit()
    except Exception:
        logger.exception("DB insert failed for event %s", eid)

    return {
        "id": eid,
        "timestamp": ts,
        "source_id": event.get("source_id", ""),
        "source_name": event.get("source_name", ""),
        "headline_ar": event.get("headline_ar", ""),
        "headline_en": event.get("headline_en", ""),
        "summary_en": event.get("summary_en", ""),
        "locations": event.get("locations", []),
        "topics": event.get("topics", []),
        "people": event.get("people", []),
        "severity": event.get("severity", "low"),
        "url": event.get("url", ""),
        "origin": event.get("origin", "rss"),
    }


def _row_to_event(row: sqlite3.Row) -> dict:
    d = dict(row)
    for field in ("locations", "topics", "people"):
        try:
            d[field] = json.loads(d.get(field) or "[]")
        except (json.JSONDecodeError, TypeError):
            d[field] = []
    return d


def query_events(hours: float = 1.0) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    db = _get_db()
    rows = db.execute(
        "SELECT * FROM events WHERE timestamp >= ? ORDER BY timestamp",
        (cutoff,),
    ).fetchall()
    return [_row_to_event(r) for r in rows]


def get_event_count(hours: float = 1.0) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    db = _get_db()
    row = db.execute("SELECT COUNT(*) FROM events WHERE timestamp >= ?", (cutoff,)).fetchone()
    return row[0] if row else 0


def get_stats(hours: float = 1.0) -> dict:
    events = query_events(hours)
    sources: Counter[str] = Counter()
    topics: Counter[str] = Counter()
    locations: Counter[str] = Counter()
    severities: Counter[str] = Counter()

    for e in events:
        sources[e.get("source_name", "Unknown")] += 1
        severities[e.get("severity", "low")] += 1
        for t in e.get("topics", []):
            topics[t] += 1
        for loc in e.get("locations", []):
            locations[loc.get("name", "")] += 1

    return {
        "total": len(events),
        "sources": dict(sources.most_common(10)),
        "topics": dict(topics.most_common(10)),
        "locations": dict(locations.most_common(10)),
        "severities": dict(severities),
    }


def get_recent_headlines(hours: float = 0.5) -> str:
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
    ts = datetime.now(timezone.utc).isoformat()
    db = _get_db()
    db.execute(
        "INSERT INTO summaries (timestamp, type, text, event_count) VALUES (?,?,?,?)",
        (ts, summary_type, text, event_count),
    )
    db.commit()
    return {"timestamp": ts, "type": summary_type, "text": text, "event_count": event_count}


def query_summaries(hours: float = 6.0) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    db = _get_db()
    rows = db.execute(
        "SELECT timestamp, type, text, event_count FROM summaries WHERE timestamp >= ? ORDER BY timestamp",
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_running_log(hours: float = 6.0) -> str:
    entries = query_summaries(hours)
    return "\n".join(e["text"] for e in entries if e["text"])
