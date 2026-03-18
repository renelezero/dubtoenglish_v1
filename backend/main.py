import asyncio
import base64
import json
import logging
import os
import time
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from stream import extract_audio_url, capture_audio_chunks, cleanup_chunk
from transcribe import transcribe_audio
from translate import translate_text
from tts import synthesize_speech_bytes
from store import (
    add_transcript, query_transcripts,
    add_summary, query_summaries, get_running_log,
)
from summarize import generate_summary, generate_incremental_update

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")

# ---- Stream configuration ----

STREAMS = [
    {"id": "alarabiya", "name": "Al Arabiya Baghdad", "url": "https://www.youtube.com/watch?v=n7eQejkXbnM"},
    {"id": "aljazeera", "name": "Al Jazeera Arabic", "url": "https://www.youtube.com/watch?v=bNyUyrR0PHo"},
    {"id": "alaghad", "name": "Alaghad", "url": "https://www.youtube.com/watch?v=4N5jTVWB7vA"},
    {"id": "alaraby", "name": "Alaraby TV", "url": "https://www.youtube.com/watch?v=e2RgSa1Wt5o"},
]

INCREMENTAL_INTERVAL = 7

LOOKBACK_SCHEDULE = [
    {"hours": 3.0,  "label": "3h",  "interval": 600},
    {"hours": 6.0,  "label": "6h",  "interval": 1800},
    {"hours": 12.0, "label": "12h", "interval": 3600},
]

# ---- Broadcast hub ----

class Broadcaster:
    """Fan-out messages to all connected WebSocket clients."""

    def __init__(self):
        self._clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._clients.add(ws)
        logger.info("Client connected (%d total)", len(self._clients))

    def disconnect(self, ws: WebSocket):
        self._clients.discard(ws)
        logger.info("Client disconnected (%d total)", len(self._clients))

    async def broadcast(self, message: dict):
        dead = []
        for ws in self._clients:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._clients.discard(ws)


hub = Broadcaster()

# ---- Background stream pipeline ----

CHUNK_DURATION = 3.0


def _latest_chunk_index(chunk_dir: str) -> int:
    """Return the highest chunk index present in the output directory."""
    try:
        names = [f for f in os.listdir(chunk_dir)
                 if f.startswith("chunk_") and f.endswith(".wav")]
        if not names:
            return -1
        return max(int(n[6:12]) for n in names)
    except (OSError, ValueError):
        return -1


async def _tts_and_broadcast(text: str, stream_id: str, stream_name: str):
    """Generate TTS and broadcast audio in the background."""
    try:
        audio_bytes = await synthesize_speech_bytes(text)
        audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
        await hub.broadcast({
            "type": "translation_audio",
            "stream_id": stream_id,
            "audio": audio_b64,
        })
    except Exception:
        logger.exception("[%s] Background TTS failed", stream_name)


async def run_stream_pipeline(stream_cfg: dict):
    """Continuously process a single stream and broadcast results."""
    sid = stream_cfg["id"]
    name = stream_cfg["name"]
    url = stream_cfg["url"]
    recent: list[str] = []

    while True:
        try:
            logger.info("[%s] Extracting audio URL...", name)
            await hub.broadcast({"type": "status", "stream_id": sid, "message": "Connecting..."})
            audio_url = await asyncio.to_thread(extract_audio_url, url)
            await hub.broadcast({"type": "status", "stream_id": sid, "message": "Live"})

            async for chunk_path in capture_audio_chunks(audio_url, chunk_duration=CHUNK_DURATION):
                try:
                    # Skip stale chunks so latency never grows
                    chunk_dir = os.path.dirname(chunk_path)
                    current_idx = int(os.path.basename(chunk_path)[6:12])
                    latest_idx = _latest_chunk_index(chunk_dir)
                    if latest_idx - current_idx > 2:
                        logger.info("[%s] Skipping chunk %d (latest: %d)",
                                    name, current_idx, latest_idx)
                        continue

                    arabic = await transcribe_audio(chunk_path)
                    if not arabic:
                        continue

                    await hub.broadcast({"type": "arabic", "stream_id": sid, "text": arabic})

                    context = " ".join(recent[-3:])
                    english = await translate_text(arabic, context=context)

                    # Broadcast text immediately — don't wait for TTS
                    await hub.broadcast({
                        "type": "translation",
                        "stream_id": sid,
                        "stream_name": name,
                        "text": english,
                        "audio": None,
                    })

                    # Fire TTS in background so the next chunk can start processing
                    if english:
                        asyncio.create_task(_tts_and_broadcast(english, sid, name))

                    if arabic and english:
                        await asyncio.to_thread(add_transcript, sid, name, arabic, english)

                    if english:
                        recent.append(english)
                        if len(recent) > 6:
                            recent = recent[-6:]
                finally:
                    cleanup_chunk(chunk_path)

        except Exception:
            logger.exception("[%s] Pipeline error, restarting in 10s...", name)
            await hub.broadcast({"type": "status", "stream_id": sid, "message": "Error — restarting..."})
            await asyncio.sleep(10)


# ---- Background summary task ----

async def _make_tts_b64(text: str) -> str | None:
    if not text:
        return None
    try:
        audio_bytes = await synthesize_speech_bytes(text)
        return base64.b64encode(audio_bytes).decode("utf-8")
    except Exception:
        logger.exception("Summary TTS failed")
        return None


async def run_summary_loop():
    """
    Every ~7 seconds, check for new transcripts. If new data arrived,
    ask GPT for an incremental update. If there's genuinely new info,
    broadcast it and have TTS speak only the new part. Skip entirely
    when nothing has changed (no API call wasted).
    """
    await asyncio.sleep(15)

    last_transcript_ts = ""

    while True:
        t0 = time.monotonic()
        try:
            transcripts = await asyncio.to_thread(query_transcripts, 0.01)

            if transcripts:
                latest_ts = transcripts[-1].get("timestamp", "")

                if latest_ts != last_transcript_ts:
                    last_transcript_ts = latest_ts

                    previous_log = await asyncio.to_thread(get_running_log, 6.0)
                    update = await generate_incremental_update(
                        transcripts, previous_log
                    )

                    if update:
                        entry = await asyncio.to_thread(
                            add_summary, "incremental", update, len(transcripts)
                        )

                        audio_b64 = await _make_tts_b64(update)

                        await hub.broadcast({
                            "type": "summary_entry",
                            "entry": entry,
                            "audio": audio_b64,
                        })
                        logger.info("Incremental update broadcast (%d transcripts)", len(transcripts))

        except Exception:
            logger.exception("Incremental summary error")

        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, INCREMENTAL_INTERVAL - elapsed))


async def run_lookback_loop():
    """
    Periodically generate full lookback summaries on a staggered schedule:
    3h every 10 min, 6h every 30 min, 12h every 60 min.
    """
    await asyncio.sleep(60)

    last_run: dict[str, float] = {}

    while True:
        for window in LOOKBACK_SCHEDULE:
            label = window["label"]
            since_last = time.monotonic() - last_run.get(label, 0)

            if since_last < window["interval"]:
                continue

            try:
                transcripts = await asyncio.to_thread(query_transcripts, window["hours"])
                if not transcripts or len(transcripts) < 2:
                    continue

                summary_text = await generate_summary(transcripts, window["hours"])
                if not summary_text:
                    continue

                await asyncio.to_thread(
                    add_summary, f"lookback_{label}", summary_text, len(transcripts)
                )

                audio_b64 = await _make_tts_b64(summary_text)

                await hub.broadcast({
                    "type": "lookback",
                    "window": label,
                    "text": summary_text,
                    "audio": audio_b64,
                    "transcript_count": len(transcripts),
                })

                last_run[label] = time.monotonic()
                logger.info("Lookback %s broadcast (%d transcripts)", label, len(transcripts))

            except Exception:
                logger.exception("Lookback %s failed", label)

        await asyncio.sleep(30)


# ---- App lifecycle ----

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = []
    for stream_cfg in STREAMS:
        tasks.append(asyncio.create_task(run_stream_pipeline(stream_cfg)))
    tasks.append(asyncio.create_task(run_summary_loop()))
    tasks.append(asyncio.create_task(run_lookback_loop()))
    logger.info("Started %d stream pipelines + summary + lookback loops", len(STREAMS))
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="DubToEnglish", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.websocket("/ws/feed")
async def websocket_feed(ws: WebSocket):
    """Clients subscribe to the shared broadcast feed."""
    await hub.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(ws)


@app.get("/api/summaries")
async def get_summaries(hours: float = Query(default=6.0)):
    """Return stored summary log entries from the last N hours."""
    try:
        entries = await asyncio.to_thread(query_summaries, hours)
    except Exception:
        logger.exception("Failed to query summaries")
        return {"entries": [], "message": "Database unavailable."}
    return {"entries": entries}


@app.get("/api/summary")
async def get_summary(window: str = Query(default="1h")):
    """On-demand full lookback summary with optional TTS."""
    hours_map = {"1h": 1.0, "3h": 3.0, "6h": 6.0, "12h": 12.0}
    hours = hours_map.get(window, 1.0)

    try:
        transcripts = await asyncio.to_thread(query_transcripts, hours)
    except Exception:
        logger.exception("Failed to query transcripts for summary")
        return {"window": window, "summary": None, "message": "Database unavailable.", "transcript_count": 0}

    if not transcripts:
        return {"window": window, "summary": None, "message": f"No transcripts in the last {window}.", "transcript_count": 0}

    try:
        summary = await generate_summary(transcripts, hours)
    except Exception:
        logger.exception("Failed to generate summary")
        return {"window": window, "summary": None, "message": "Summary generation failed.", "transcript_count": len(transcripts)}

    audio_b64 = await _make_tts_b64(summary)

    await asyncio.to_thread(add_summary, f"lookback_{window}", summary, len(transcripts))

    return {"window": window, "summary": summary, "transcript_count": len(transcripts), "audio": audio_b64}


@app.get("/api/streams")
async def get_streams():
    return {"streams": STREAMS}


if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
