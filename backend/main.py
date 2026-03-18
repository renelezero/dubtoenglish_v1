import asyncio
import base64
import logging
import os
import time
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from scraper import fetch_rss_feeds, fetch_gdelt_events
from stream import LIVE_STREAMS, capture_audio_chunks, cleanup_chunk
from transcribe import transcribe_audio
from analyze import analyze_item
from tts import synthesize_speech_bytes
from store import (
    add_event, query_events, get_event_count, get_stats,
    get_recent_headlines, add_summary, query_summaries, get_running_log,
)
from summarize import generate_incremental_update, generate_summary

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")

RSS_INTERVAL = 60
GDELT_INTERVAL = 900
BRIEFING_INTERVAL = 20


# ---- Broadcast hub ----

class Broadcaster:
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


# ---- Background: RSS scraper ----

async def run_rss_loop():
    await asyncio.sleep(5)
    while True:
        t0 = time.monotonic()
        try:
            items = await asyncio.to_thread(fetch_rss_feeds)
            if items:
                logger.info("RSS: %d new items", len(items))
                for item in items[:15]:
                    try:
                        analyzed = await analyze_item(item)
                        if analyzed:
                            entry = await asyncio.to_thread(add_event, analyzed)
                            await hub.broadcast({"type": "event", "event": entry})
                    except Exception:
                        logger.exception("Analyze failed for item")
                    await asyncio.sleep(2)
        except Exception:
            logger.exception("RSS loop error")
        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, RSS_INTERVAL - elapsed))


# ---- Background: GDELT scraper ----

async def run_gdelt_loop():
    await asyncio.sleep(20)
    while True:
        t0 = time.monotonic()
        try:
            items = await fetch_gdelt_events()
            if items:
                logger.info("GDELT: %d new events", len(items))
                for item in items[:10]:
                    analyzed = await analyze_item(item)
                    if analyzed:
                        entry = await asyncio.to_thread(add_event, analyzed)
                        await hub.broadcast({"type": "event", "event": entry})
        except Exception:
            logger.exception("GDELT loop error")
        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, GDELT_INTERVAL - elapsed))


# ---- Background: Live HLS stream pipeline ----

CHUNK_DURATION = 5.0

async def run_stream_pipeline(stream_cfg: dict):
    """
    Capture audio from an HLS stream, transcribe with Whisper, and push
    the raw Arabic transcription as a headline event. No per-chunk GPT call —
    the AI summarizer batches everything every 20s instead.
    """
    sid = stream_cfg["id"]
    name = stream_cfg["name"]
    url = stream_cfg["url"]

    while True:
        try:
            logger.info("[%s] Connecting to HLS stream...", name)
            await hub.broadcast({"type": "stream_status", "stream_id": sid, "status": "connecting"})

            async for chunk_path in capture_audio_chunks(url, chunk_duration=CHUNK_DURATION):
                try:
                    arabic = await transcribe_audio(chunk_path)
                    if not arabic or len(arabic.strip()) < 5:
                        continue

                    entry = await asyncio.to_thread(add_event, {
                        "id": f"{sid}_{int(time.time())}",
                        "source_id": sid,
                        "source_name": name,
                        "headline_ar": arabic[:200],
                        "headline_en": arabic[:200],
                        "summary_en": "",
                        "locations": [],
                        "topics": [],
                        "people": [],
                        "severity": "medium",
                        "url": "",
                        "origin": "live",
                    })
                    await hub.broadcast({"type": "event", "event": entry})
                    await hub.broadcast({"type": "stream_status", "stream_id": sid, "status": "live"})
                    logger.info("[%s] Transcription: %s", name, arabic[:60])

                finally:
                    cleanup_chunk(chunk_path)

        except Exception:
            logger.exception("[%s] Stream error, retrying in 15s...", name)
            await hub.broadcast({"type": "stream_status", "stream_id": sid, "status": "error"})
            await asyncio.sleep(15)


# ---- Background: AI briefing ----

async def _make_tts_b64(text: str) -> str | None:
    if not text:
        return None
    try:
        audio_bytes = await synthesize_speech_bytes(text)
        return base64.b64encode(audio_bytes).decode("utf-8")
    except Exception:
        logger.exception("TTS failed")
        return None


async def run_briefing_loop():
    await asyncio.sleep(30)
    last_event_count = 0

    while True:
        t0 = time.monotonic()
        try:
            current_count = await asyncio.to_thread(get_event_count, 0.5)

            if current_count > last_event_count:
                last_event_count = current_count
                headlines = await asyncio.to_thread(get_recent_headlines, 0.15)
                previous_log = await asyncio.to_thread(get_running_log, 6.0)
                update = await generate_incremental_update(headlines, previous_log)

                if update:
                    entry = await asyncio.to_thread(
                        add_summary, "incremental", update, current_count
                    )
                    audio_b64 = await _make_tts_b64(update)
                    await hub.broadcast({
                        "type": "briefing",
                        "entry": entry,
                        "audio": audio_b64,
                    })
                    logger.info("Briefing broadcast (%d events)", current_count)

            stats = await asyncio.to_thread(get_stats, 1.0)
            await hub.broadcast({"type": "stats", "stats": stats})

        except Exception:
            logger.exception("Briefing loop error")

        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, BRIEFING_INTERVAL - elapsed))


# ---- Lifecycle ----

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(run_rss_loop()),
        asyncio.create_task(run_gdelt_loop()),
        asyncio.create_task(run_briefing_loop()),
    ]
    for stream_cfg in LIVE_STREAMS:
        tasks.append(asyncio.create_task(run_stream_pipeline(stream_cfg)))
    logger.info("Started RSS + GDELT + %d live streams + briefing", len(LIVE_STREAMS))
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="Ein Americy", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.websocket("/ws/feed")
async def websocket_feed(ws: WebSocket):
    await hub.connect(ws)
    try:
        # Send stored data immediately so new clients see everything
        events = await asyncio.to_thread(query_events, 6.0)
        if events:
            await ws.send_json({"type": "history", "events": events})

        summaries = await asyncio.to_thread(query_summaries, 6.0)
        if summaries:
            await ws.send_json({"type": "briefing_history", "entries": summaries})

        stats = await asyncio.to_thread(get_stats, 1.0)
        await ws.send_json({"type": "stats", "stats": stats})

        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(ws)


@app.get("/api/events")
async def get_events(hours: float = Query(default=1.0)):
    events = await asyncio.to_thread(query_events, hours)
    return {"events": events, "count": len(events)}


@app.get("/api/stats")
async def get_stats_endpoint(hours: float = Query(default=1.0)):
    stats = await asyncio.to_thread(get_stats, hours)
    return stats


@app.get("/api/summary")
async def get_summary_endpoint(window: str = Query(default="1h")):
    hours_map = {"1h": 1.0, "3h": 3.0, "6h": 6.0, "12h": 12.0}
    hours = hours_map.get(window, 1.0)

    headlines = await asyncio.to_thread(get_recent_headlines, hours)
    if not headlines:
        return {"summary": None, "message": f"No events in the last {window}."}

    try:
        summary = await generate_summary(headlines, hours)
    except Exception:
        logger.exception("Summary generation failed")
        return {"summary": None, "message": "Generation failed."}

    audio_b64 = await _make_tts_b64(summary)
    await asyncio.to_thread(add_summary, f"lookback_{window}", summary, 0)

    return {"summary": summary, "audio": audio_b64}


@app.get("/api/sources")
async def get_sources():
    from scraper import RSS_FEEDS
    return {"sources": RSS_FEEDS}


if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
