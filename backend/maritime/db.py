"""
SQLite persistence layer for maritime vessel data.

Every AIS position update is timestamped and saved permanently.
Provides historical queries for vessel tracks, traffic patterns, and anomalies.
"""

import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional, List

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "maritime.db")

_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA synchronous=NORMAL")
        _local.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    return _local.conn


def init_db():
    """Create tables if they don't exist."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS vessels (
            mmsi INTEGER PRIMARY KEY,
            name TEXT DEFAULT '',
            imo INTEGER,
            callsign TEXT DEFAULT '',
            ship_type TEXT DEFAULT 'other',
            ship_type_code INTEGER DEFAULT 0,
            flag TEXT DEFAULT '',
            destination TEXT DEFAULT '',
            length REAL DEFAULT 0,
            width REAL DEFAULT 0,
            draught REAL,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            last_lat REAL,
            last_lng REAL,
            last_speed REAL,
            last_course REAL,
            last_heading REAL,
            position_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mmsi INTEGER NOT NULL,
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            speed REAL,
            course REAL,
            heading REAL,
            nav_status INTEGER,
            ship_type TEXT DEFAULT 'other',
            msg_type TEXT DEFAULT '',
            timestamp TEXT NOT NULL,
            recorded_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            mmsi INTEGER NOT NULL,
            name TEXT DEFAULT '',
            ship_type TEXT DEFAULT '',
            flag TEXT DEFAULT '',
            lat REAL,
            lng REAL,
            severity TEXT DEFAULT 'low',
            description TEXT DEFAULT '',
            extra TEXT DEFAULT '',
            timestamp TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_positions_mmsi ON positions(mmsi);
        CREATE INDEX IF NOT EXISTS idx_positions_timestamp ON positions(timestamp);
        CREATE INDEX IF NOT EXISTS idx_positions_mmsi_ts ON positions(mmsi, timestamp);
        CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies(timestamp);
        CREATE INDEX IF NOT EXISTS idx_vessels_type ON vessels(ship_type);
        CREATE INDEX IF NOT EXISTS idx_vessels_last_seen ON vessels(last_seen);
    """)
    conn.commit()
    logger.info("Maritime DB initialized at %s", DB_PATH)


def save_position(data: dict):
    """Save a position update — both to positions log and vessels table."""
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    mmsi = data["mmsi"]
    ts = data.get("timestamp") or now

    if data.get("is_static"):
        conn.execute("""
            INSERT INTO vessels (mmsi, name, imo, callsign, ship_type, ship_type_code,
                                flag, destination, length, width, draught, first_seen, last_seen,
                                position_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(mmsi) DO UPDATE SET
                name = CASE WHEN excluded.name != '' THEN excluded.name ELSE vessels.name END,
                imo = COALESCE(excluded.imo, vessels.imo),
                callsign = CASE WHEN excluded.callsign != '' THEN excluded.callsign ELSE vessels.callsign END,
                ship_type = CASE WHEN excluded.ship_type != 'other' THEN excluded.ship_type ELSE vessels.ship_type END,
                ship_type_code = CASE WHEN excluded.ship_type_code != 0 THEN excluded.ship_type_code ELSE vessels.ship_type_code END,
                flag = CASE WHEN excluded.flag != '' THEN excluded.flag ELSE vessels.flag END,
                destination = CASE WHEN excluded.destination != '' THEN excluded.destination ELSE vessels.destination END,
                length = CASE WHEN excluded.length > 0 THEN excluded.length ELSE vessels.length END,
                width = CASE WHEN excluded.width > 0 THEN excluded.width ELSE vessels.width END,
                draught = COALESCE(excluded.draught, vessels.draught),
                last_seen = excluded.last_seen
        """, (
            mmsi,
            data.get("name", ""),
            data.get("imo"),
            data.get("callsign", ""),
            data.get("ship_type", "other"),
            data.get("ship_type_code", 0),
            data.get("flag", ""),
            data.get("destination", ""),
            data.get("length", 0),
            data.get("width", 0),
            data.get("draught"),
            now, now,
        ))
        conn.commit()
        return

    conn.execute("""
        INSERT INTO positions (mmsi, lat, lng, speed, course, heading, nav_status,
                              ship_type, msg_type, timestamp, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        mmsi,
        data.get("lat"),
        data.get("lng"),
        data.get("speed"),
        data.get("course"),
        data.get("heading"),
        data.get("nav_status"),
        data.get("ship_type", "other"),
        data.get("msg_type", ""),
        ts,
        now,
    ))

    conn.execute("""
        INSERT INTO vessels (mmsi, name, ship_type, ship_type_code, flag, first_seen, last_seen,
                            last_lat, last_lng, last_speed, last_course, last_heading, position_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(mmsi) DO UPDATE SET
            name = CASE WHEN excluded.name != '' THEN excluded.name ELSE vessels.name END,
            ship_type = CASE WHEN excluded.ship_type != 'other' THEN excluded.ship_type ELSE vessels.ship_type END,
            ship_type_code = CASE WHEN excluded.ship_type_code != 0 THEN excluded.ship_type_code ELSE vessels.ship_type_code END,
            flag = CASE WHEN excluded.flag != '' THEN excluded.flag ELSE vessels.flag END,
            last_seen = excluded.last_seen,
            last_lat = excluded.last_lat,
            last_lng = excluded.last_lng,
            last_speed = excluded.last_speed,
            last_course = excluded.last_course,
            last_heading = excluded.last_heading,
            position_count = vessels.position_count + 1
    """, (
        mmsi,
        data.get("name", ""),
        data.get("ship_type", "other"),
        data.get("ship_type_code", 0),
        data.get("flag", ""),
        now, now,
        data.get("lat"),
        data.get("lng"),
        data.get("speed"),
        data.get("course"),
        data.get("heading"),
    ))

    conn.commit()


def save_anomaly(anomaly: dict):
    """Persist a detected anomaly."""
    conn = _get_conn()
    conn.execute("""
        INSERT INTO anomalies (type, mmsi, name, ship_type, flag, lat, lng,
                              severity, description, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        anomaly.get("type", ""),
        anomaly.get("mmsi", 0),
        anomaly.get("name", ""),
        anomaly.get("ship_type", ""),
        anomaly.get("flag", ""),
        anomaly.get("lat"),
        anomaly.get("lng"),
        anomaly.get("severity", "low"),
        anomaly.get("description", ""),
        anomaly.get("timestamp", datetime.now(timezone.utc)).isoformat()
        if hasattr(anomaly.get("timestamp"), "isoformat")
        else str(anomaly.get("timestamp", "")),
    ))
    conn.commit()


# ---- Historical queries ----

def get_vessel_history(mmsi: int, hours: Optional[float] = None, limit: int = 5000) -> List[dict]:
    """Get full position history for a vessel."""
    conn = _get_conn()
    if hours:
        rows = conn.execute("""
            SELECT * FROM positions
            WHERE mmsi = ? AND timestamp >= datetime('now', ?)
            ORDER BY timestamp ASC LIMIT ?
        """, (mmsi, f"-{int(hours * 3600)} seconds", limit)).fetchall()
    else:
        rows = conn.execute("""
            SELECT * FROM positions WHERE mmsi = ?
            ORDER BY timestamp ASC LIMIT ?
        """, (mmsi, limit)).fetchall()
    return [dict(r) for r in rows]


def get_all_vessels_db(active_hours: Optional[float] = None) -> List[dict]:
    """Get all known vessels, optionally filtered by recent activity."""
    conn = _get_conn()
    if active_hours:
        rows = conn.execute("""
            SELECT * FROM vessels
            WHERE last_seen >= datetime('now', ?)
            ORDER BY last_seen DESC
        """, (f"-{int(active_hours * 3600)} seconds",)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM vessels ORDER BY last_seen DESC").fetchall()
    return [dict(r) for r in rows]


def get_vessel_db(mmsi: int) -> Optional[dict]:
    conn = _get_conn()
    row = conn.execute("SELECT * FROM vessels WHERE mmsi = ?", (mmsi,)).fetchone()
    return dict(row) if row else None


def get_traffic_summary(hours: float = 1.0) -> dict:
    """Aggregate traffic stats from the database."""
    conn = _get_conn()
    cutoff = f"-{int(hours * 3600)} seconds"

    total = conn.execute(
        "SELECT COUNT(DISTINCT mmsi) FROM positions WHERE timestamp >= datetime('now', ?)",
        (cutoff,)
    ).fetchone()[0]

    type_counts = conn.execute("""
        SELECT ship_type, COUNT(DISTINCT mmsi) as cnt
        FROM positions WHERE timestamp >= datetime('now', ?)
        GROUP BY ship_type ORDER BY cnt DESC
    """, (cutoff,)).fetchall()

    position_count = conn.execute(
        "SELECT COUNT(*) FROM positions WHERE timestamp >= datetime('now', ?)",
        (cutoff,)
    ).fetchone()[0]

    return {
        "unique_vessels": total,
        "position_records": position_count,
        "vessel_types": {r["ship_type"]: r["cnt"] for r in type_counts},
        "window_hours": hours,
    }


def get_db_stats() -> dict:
    """Overall database statistics."""
    conn = _get_conn()
    total_vessels = conn.execute("SELECT COUNT(*) FROM vessels").fetchone()[0]
    total_positions = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    total_anomalies = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]

    oldest = conn.execute("SELECT MIN(timestamp) FROM positions").fetchone()[0]
    newest = conn.execute("SELECT MAX(timestamp) FROM positions").fetchone()[0]

    db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0

    return {
        "total_vessels_ever_seen": total_vessels,
        "total_position_records": total_positions,
        "total_anomalies": total_anomalies,
        "oldest_record": oldest,
        "newest_record": newest,
        "db_size_mb": round(db_size / (1024 * 1024), 2),
    }


def search_vessels(query: str, limit: int = 50) -> List[dict]:
    """Search vessels by name, MMSI, IMO, flag, or destination."""
    conn = _get_conn()
    q = f"%{query}%"
    rows = conn.execute("""
        SELECT * FROM vessels
        WHERE name LIKE ? OR CAST(mmsi AS TEXT) LIKE ? OR flag LIKE ?
              OR destination LIKE ? OR callsign LIKE ?
        ORDER BY last_seen DESC LIMIT ?
    """, (q, q, q, q, q, limit)).fetchall()
    return [dict(r) for r in rows]
