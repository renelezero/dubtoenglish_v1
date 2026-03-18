import logging
import os
import threading
from collections import deque
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_COLLECTION = "transcripts"
_SUMMARY_COLLECTION = "summaries"
_USE_FIRESTORE = False
_db = None

# ---- In-memory fallback ----

_mem_lock = threading.Lock()
_mem_store: deque[dict] = deque(maxlen=5000)
_mem_summaries: deque[dict] = deque(maxlen=1000)


def _init_firebase():
    global _USE_FIRESTORE, _db
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore

        cred_path = os.path.join(os.path.dirname(__file__), "firebase-service-account.json")
        if not os.path.exists(cred_path):
            logger.warning("Firebase credentials not found at %s — using in-memory storage", cred_path)
            return

        if not firebase_admin._apps:
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred)

        client = firestore.client()
        list(client.collection(_COLLECTION).limit(1).stream())
        _db = client
        _USE_FIRESTORE = True
        logger.info("Firestore connected successfully")
    except Exception as exc:
        logger.warning("Firestore unavailable (%s) — using in-memory storage", exc)


_init_firebase()


# ---- Transcript storage ----

def add_transcript(stream_id: str, stream_name: str, arabic: str, english: str) -> None:
    entry = {
        "timestamp": datetime.now(timezone.utc),
        "stream_id": stream_id,
        "stream_name": stream_name,
        "arabic": arabic,
        "english": english,
    }

    if _USE_FIRESTORE:
        try:
            _db.collection(_COLLECTION).add(entry)
            return
        except Exception:
            logger.exception("Firestore write failed, saving to memory")

    with _mem_lock:
        _mem_store.append(entry)


def query_transcripts(hours: float) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    if _USE_FIRESTORE:
        try:
            docs = (
                _db.collection(_COLLECTION)
                .where("timestamp", ">=", cutoff)
                .order_by("timestamp")
                .stream()
            )
            results = []
            for doc in docs:
                d = doc.to_dict()
                results.append({
                    "timestamp": d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else str(d["timestamp"]),
                    "stream_id": d.get("stream_id", ""),
                    "stream_name": d.get("stream_name", ""),
                    "arabic": d.get("arabic", ""),
                    "english": d.get("english", ""),
                })
            return results
        except Exception:
            logger.exception("Firestore query failed, falling back to memory")

    with _mem_lock:
        results = []
        for entry in _mem_store:
            if entry["timestamp"] >= cutoff:
                results.append({
                    "timestamp": entry["timestamp"].isoformat(),
                    "stream_id": entry["stream_id"],
                    "stream_name": entry["stream_name"],
                    "arabic": entry["arabic"],
                    "english": entry["english"],
                })
        return results


# ---- Summary storage ----

def add_summary(summary_type: str, text: str, transcript_count: int = 0) -> dict:
    """
    Store a summary entry. summary_type is 'lookback_6h', 'lookback_1h', or 'incremental'.
    Returns the stored entry dict.
    """
    entry = {
        "timestamp": datetime.now(timezone.utc),
        "type": summary_type,
        "text": text,
        "transcript_count": transcript_count,
    }

    if _USE_FIRESTORE:
        try:
            _db.collection(_SUMMARY_COLLECTION).add(entry)
        except Exception:
            logger.exception("Firestore summary write failed, saving to memory")
            with _mem_lock:
                _mem_summaries.append(entry)
    else:
        with _mem_lock:
            _mem_summaries.append(entry)

    return {
        "timestamp": entry["timestamp"].isoformat(),
        "type": entry["type"],
        "text": entry["text"],
        "transcript_count": entry["transcript_count"],
    }


def query_summaries(hours: float = 6.0, limit: int = 200) -> list[dict]:
    """Retrieve stored summary entries from the last N hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    if _USE_FIRESTORE:
        try:
            docs = (
                _db.collection(_SUMMARY_COLLECTION)
                .where("timestamp", ">=", cutoff)
                .order_by("timestamp")
                .limit(limit)
                .stream()
            )
            results = []
            for doc in docs:
                d = doc.to_dict()
                results.append({
                    "timestamp": d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else str(d["timestamp"]),
                    "type": d.get("type", ""),
                    "text": d.get("text", ""),
                    "transcript_count": d.get("transcript_count", 0),
                })
            return results
        except Exception:
            logger.exception("Firestore summary query failed, falling back to memory")

    with _mem_lock:
        results = []
        for entry in _mem_summaries:
            if entry["timestamp"] >= cutoff:
                results.append({
                    "timestamp": entry["timestamp"].isoformat(),
                    "type": entry["type"],
                    "text": entry["text"],
                    "transcript_count": entry["transcript_count"],
                })
        if len(results) > limit:
            results = results[-limit:]
        return results


def get_running_log(hours: float = 6.0) -> str:
    """Return the concatenated text of all stored summaries as a running log."""
    entries = query_summaries(hours=hours)
    return "\n".join(e["text"] for e in entries if e["text"])
