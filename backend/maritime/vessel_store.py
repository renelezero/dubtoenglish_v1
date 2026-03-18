"""
In-memory vessel position store with tracking, analytics, and SQLite persistence.

In-memory for speed, SQLite for permanent history.
Every position update is saved to disk with a timestamp.
"""

import logging
import threading
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict

from maritime.db import save_position, save_anomaly

logger = logging.getLogger(__name__)

_lock = threading.Lock()

_vessels: Dict[int, dict] = {}          # MMSI -> latest vessel state
_position_log: deque = deque(maxlen=100_000)
_static_data: Dict[int, dict] = {}     # MMSI -> static info
_anomalies: deque = deque(maxlen=1_000)


def update_vessel(data: dict) -> dict:
    """Update vessel position or static data. Persists to SQLite. Returns merged state."""
    mmsi = data["mmsi"]

    # Persist every update to SQLite (runs in same thread, WAL mode keeps it fast)
    try:
        save_position(data)
    except Exception:
        logger.exception("DB save failed for MMSI %s", mmsi)

    with _lock:
        if data.get("is_static"):
            existing = _static_data.get(mmsi, {})
            existing.update({k: v for k, v in data.items() if v})
            _static_data[mmsi] = existing

            if mmsi in _vessels:
                _vessels[mmsi].update({
                    "name": existing.get("name") or _vessels[mmsi].get("name", ""),
                    "destination": existing.get("destination", ""),
                    "length": existing.get("length", 0),
                    "width": existing.get("width", 0),
                    "imo": existing.get("imo"),
                    "callsign": existing.get("callsign", ""),
                })
            return existing

        existing = _vessels.get(mmsi, {})

        prev_lat = existing.get("lat")
        prev_lng = existing.get("lng")
        prev_time = existing.get("last_seen")

        entry = {
            "mmsi": mmsi,
            "name": data.get("name") or existing.get("name", ""),
            "lat": data.get("lat"),
            "lng": data.get("lng"),
            "speed": data.get("speed"),
            "course": data.get("course"),
            "heading": data.get("heading"),
            "nav_status": data.get("nav_status"),
            "ship_type_code": data.get("ship_type_code") or existing.get("ship_type_code", 0),
            "ship_type": data.get("ship_type") or existing.get("ship_type", "other"),
            "flag": data.get("flag") or existing.get("flag", ""),
            "destination": existing.get("destination", ""),
            "length": existing.get("length", 0),
            "width": existing.get("width", 0),
            "imo": existing.get("imo"),
            "callsign": existing.get("callsign", ""),
            "last_seen": datetime.now(timezone.utc),
            "first_seen": existing.get("first_seen", datetime.now(timezone.utc)),
            "position_count": existing.get("position_count", 0) + 1,
        }

        _vessels[mmsi] = entry

        _position_log.append({
            "mmsi": mmsi,
            "lat": data.get("lat"),
            "lng": data.get("lng"),
            "speed": data.get("speed"),
            "course": data.get("course"),
            "ship_type": entry["ship_type"],
            "timestamp": datetime.now(timezone.utc),
        })

        anomaly = _detect_anomaly(entry, prev_lat, prev_lng, prev_time)
        if anomaly:
            _anomalies.append(anomaly)
            try:
                save_anomaly(anomaly)
            except Exception:
                logger.exception("DB anomaly save failed")

    return _serialize_vessel(entry)


def _detect_anomaly(vessel: dict, prev_lat, prev_lng, prev_time) -> Optional[dict]:
    """Flag suspicious behavior: AIS gaps, sudden speed changes, loitering."""
    now = datetime.now(timezone.utc)

    # AIS gap: vessel was seen before but gap > 30 minutes
    if prev_time and (now - prev_time).total_seconds() > 1800:
        return {
            "type": "ais_gap",
            "mmsi": vessel["mmsi"],
            "name": vessel["name"],
            "ship_type": vessel["ship_type"],
            "flag": vessel["flag"],
            "gap_minutes": round((now - prev_time).total_seconds() / 60),
            "lat": vessel["lat"],
            "lng": vessel["lng"],
            "timestamp": now,
            "severity": "high" if vessel["ship_type"] == "tanker" else "medium",
            "description": (
                f"{vessel['name'] or 'Unknown'} (MMSI:{vessel['mmsi']}) "
                f"reappeared after {round((now - prev_time).total_seconds() / 60)}min AIS gap"
            ),
        }

    # Loitering: vessel has many position reports but very low speed
    if (vessel["position_count"] > 20
            and vessel.get("speed") is not None
            and vessel["speed"] < 0.5
            and vessel["ship_type"] in ("tanker", "cargo")):
        return {
            "type": "loitering",
            "mmsi": vessel["mmsi"],
            "name": vessel["name"],
            "ship_type": vessel["ship_type"],
            "flag": vessel["flag"],
            "lat": vessel["lat"],
            "lng": vessel["lng"],
            "timestamp": now,
            "severity": "low",
            "description": (
                f"{vessel['name'] or 'Unknown'} ({vessel['ship_type']}) "
                f"loitering near {vessel['lat']:.3f}, {vessel['lng']:.3f}"
            ),
        }

    return None


def _serialize_vessel(v: dict) -> dict:
    return {
        "mmsi": v["mmsi"],
        "name": v.get("name", ""),
        "lat": v.get("lat"),
        "lng": v.get("lng"),
        "speed": v.get("speed"),
        "course": v.get("course"),
        "heading": v.get("heading"),
        "ship_type": v.get("ship_type", "other"),
        "ship_type_code": v.get("ship_type_code", 0),
        "flag": v.get("flag", ""),
        "destination": v.get("destination", ""),
        "length": v.get("length", 0),
        "width": v.get("width", 0),
        "imo": v.get("imo"),
        "last_seen": v["last_seen"].isoformat() if hasattr(v.get("last_seen", ""), "isoformat") else "",
        "position_count": v.get("position_count", 0),
    }


def get_active_vessels(minutes: int = 30) -> List[dict]:
    """Return vessels seen within the last N minutes."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    with _lock:
        return [
            _serialize_vessel(v) for v in _vessels.values()
            if v.get("last_seen") and v["last_seen"] >= cutoff
        ]


def get_vessel(mmsi: int) -> Optional[dict]:
    with _lock:
        v = _vessels.get(mmsi)
        return _serialize_vessel(v) if v else None


def get_vessel_track(mmsi: int, minutes: int = 60) -> List[dict]:
    """Return position history for a specific vessel."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    with _lock:
        return [
            {
                "lat": p["lat"],
                "lng": p["lng"],
                "speed": p["speed"],
                "course": p["course"],
                "timestamp": p["timestamp"].isoformat(),
            }
            for p in _position_log
            if p["mmsi"] == mmsi and p["timestamp"] >= cutoff
        ]


def get_maritime_stats(minutes: int = 30) -> dict:
    """Aggregate stats for active vessels in the strait."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    types: Counter = Counter()
    flags: Counter = Counter()
    destinations: Counter = Counter()
    speeds: List[float] = []
    total = 0
    tanker_count = 0
    military_count = 0

    with _lock:
        for v in _vessels.values():
            if not v.get("last_seen") or v["last_seen"] < cutoff:
                continue
            total += 1
            types[v.get("ship_type", "other")] += 1
            if v.get("flag"):
                flags[v["flag"]] += 1
            if v.get("destination"):
                destinations[v["destination"]] += 1
            if v.get("speed") is not None:
                speeds.append(v["speed"])
            if v.get("ship_type") == "tanker":
                tanker_count += 1
            if v.get("ship_type") == "military":
                military_count += 1

    return {
        "total_vessels": total,
        "tanker_count": tanker_count,
        "military_count": military_count,
        "vessel_types": dict(types.most_common(10)),
        "top_flags": dict(flags.most_common(15)),
        "top_destinations": dict(destinations.most_common(15)),
        "avg_speed_knots": round(sum(speeds) / len(speeds), 1) if speeds else 0,
        "max_speed_knots": round(max(speeds), 1) if speeds else 0,
    }


def get_anomalies(minutes: int = 60) -> List[dict]:
    """Return detected anomalies within the time window."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    with _lock:
        return [
            {
                "type": a["type"],
                "mmsi": a["mmsi"],
                "name": a.get("name", ""),
                "ship_type": a.get("ship_type", ""),
                "flag": a.get("flag", ""),
                "lat": a.get("lat"),
                "lng": a.get("lng"),
                "severity": a.get("severity", "low"),
                "description": a.get("description", ""),
                "timestamp": a["timestamp"].isoformat(),
            }
            for a in _anomalies
            if a["timestamp"] >= cutoff
        ]


def get_tanker_flow(minutes: int = 60) -> dict:
    """Estimate tanker throughput by direction (inbound vs outbound)."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    inbound = 0    # heading ~270-360 or 0-90 (into the Gulf)
    outbound = 0   # heading ~90-270 (out of the Gulf)

    with _lock:
        for v in _vessels.values():
            if v.get("ship_type") != "tanker":
                continue
            if not v.get("last_seen") or v["last_seen"] < cutoff:
                continue
            if v.get("speed", 0) < 1.0:
                continue
            course = v.get("course")
            if course is None:
                continue
            if 90 < course < 270:
                outbound += 1
            else:
                inbound += 1

    return {
        "inbound_tankers": inbound,
        "outbound_tankers": outbound,
        "total_moving_tankers": inbound + outbound,
        "window_minutes": minutes,
    }
