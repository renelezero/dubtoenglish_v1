import asyncio
import base64
import json
import logging
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from openai import AsyncOpenAI

from scraper import fetch_rss_feeds, fetch_gdelt_events
from stream import LIVE_STREAMS, capture_audio_chunks, cleanup_chunk
from transcribe import transcribe_audio
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
DIGEST_INTERVAL = 15

_oai: AsyncOpenAI | None = None

def _get_oai() -> AsyncOpenAI:
    global _oai
    if _oai is None:
        _oai = AsyncOpenAI()
    return _oai


# ---- Raw intake buffer ----
# All sources dump raw text here. The AI digest loop reads and clears it.

_buf_lock = threading.Lock()
_raw_buffer: deque[dict] = deque(maxlen=500)


def _push_raw(source: str, text: str, origin: str = "rss"):
    """Push a raw Arabic/English snippet into the intake buffer."""
    with _buf_lock:
        _raw_buffer.append({
            "source": source,
            "text": text[:500],
            "origin": origin,
            "ts": time.time(),
        })


def _drain_raw() -> list[dict]:
    """Drain all items from the buffer (called by the digest loop)."""
    with _buf_lock:
        items = list(_raw_buffer)
        _raw_buffer.clear()
        return items


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


# ---- Background: RSS collector (silent, no broadcast) ----

async def run_rss_collector():
    await asyncio.sleep(3)
    while True:
        t0 = time.monotonic()
        try:
            items = await asyncio.to_thread(fetch_rss_feeds)
            if items:
                logger.info("RSS: %d new items buffered", len(items))
                for item in items:
                    _push_raw(
                        item.get("source_name", "RSS"),
                        item.get("headline_ar", "") + " " + item.get("body_ar", ""),
                        "rss",
                    )
        except Exception:
            logger.exception("RSS collector error")
        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, RSS_INTERVAL - elapsed))


# ---- Background: GDELT collector (silent) ----

async def run_gdelt_collector():
    await asyncio.sleep(15)
    while True:
        t0 = time.monotonic()
        try:
            items = await fetch_gdelt_events()
            if items:
                logger.info("GDELT: %d events buffered", len(items))
                for item in items:
                    _push_raw(
                        item.get("source_name", "GDELT"),
                        item.get("headline_ar", ""),
                        "gdelt",
                    )
        except Exception:
            logger.exception("GDELT collector error")
        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, GDELT_INTERVAL - elapsed))


# ---- Background: Live HLS stream collector (silent) ----

CHUNK_DURATION = 5.0

async def run_stream_collector(stream_cfg: dict):
    sid = stream_cfg["id"]
    name = stream_cfg["name"]
    url = stream_cfg["url"]

    while True:
        try:
            logger.info("[%s] Connecting to HLS...", name)
            await hub.broadcast({"type": "stream_status", "stream_id": sid, "status": "connecting"})

            async for chunk_path in capture_audio_chunks(url, chunk_duration=CHUNK_DURATION):
                try:
                    arabic = await transcribe_audio(chunk_path)
                    if not arabic or len(arabic.strip()) < 5:
                        continue
                    _push_raw(name, arabic, "live")
                    await hub.broadcast({"type": "stream_status", "stream_id": sid, "status": "live"})
                    logger.info("[%s] Buffered: %s", name, arabic[:60])
                finally:
                    cleanup_chunk(chunk_path)

        except Exception:
            logger.exception("[%s] Stream error, retrying in 15s...", name)
            await hub.broadcast({"type": "stream_status", "stream_id": sid, "status": "error"})
            await asyncio.sleep(15)


# ---- AI Digest loop: every 15s, summarize buffer into headlines ----

DIGEST_PROMPT = """\
You are an intelligence analyst. You receive raw Arabic news text from multiple sources \
(RSS headlines, live broadcast transcriptions, GDELT events).

Produce a JSON array of English-language intelligence headlines. Each item:
{
  "headline_en": "Clear English headline",
  "summary_en": "1-2 sentence summary",
  "source": "Source name from the input",
  "locations": [{"name": "Place", "lat": 33.3, "lng": 44.4}],
  "severity": "critical|high|medium|low",
  "topics": ["military","politics","economy","humanitarian","diplomacy","protest","other"]
}

Rules:
- Translate and consolidate — merge duplicate stories across sources
- Only include genuinely newsworthy items (skip ads, channel promos, filler)
- Extract locations with approximate lat/lng
- severity: critical=active conflict/casualties, high=major event, medium=significant, low=routine
- Output ONLY a JSON array, nothing else
- If the input contains nothing newsworthy, return: []"""


async def run_digest_loop():
    await asyncio.sleep(20)

    while True:
        t0 = time.monotonic()
        try:
            raw_items = await asyncio.to_thread(_drain_raw)

            if raw_items:
                raw_text = "\n".join(
                    f"[{r['source']}] ({r['origin']}): {r['text']}"
                    for r in raw_items
                )
                if len(raw_text) > 12000:
                    raw_text = raw_text[-12000:]

                logger.info("Digest: processing %d raw items...", len(raw_items))

                resp = await _get_oai().chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": DIGEST_PROMPT},
                        {"role": "user", "content": raw_text},
                    ],
                    temperature=0.2,
                    max_tokens=2048,
                    response_format={"type": "json_object"},
                    timeout=60,
                )

                content = resp.choices[0].message.content or "[]"
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, dict):
                        parsed = parsed.get("headlines", parsed.get("items", []))
                    if not isinstance(parsed, list):
                        parsed = []
                except json.JSONDecodeError:
                    parsed = []

                for i, item in enumerate(parsed):
                    event = {
                        "id": f"digest_{int(time.time())}_{i}",
                        "source_id": "digest",
                        "source_name": item.get("source", "Intel"),
                        "headline_ar": "",
                        "headline_en": item.get("headline_en", ""),
                        "summary_en": item.get("summary_en", ""),
                        "locations": item.get("locations", []),
                        "topics": item.get("topics", []),
                        "people": item.get("people", []),
                        "severity": item.get("severity", "medium"),
                        "url": "",
                        "origin": "digest",
                    }
                    entry = await asyncio.to_thread(add_event, event)
                    await hub.broadcast({"type": "event", "event": entry})

                logger.info("Digest: %d headlines produced", len(parsed))

            # Broadcast stats every cycle
            stats = await asyncio.to_thread(get_stats, 1.0)
            await hub.broadcast({"type": "stats", "stats": stats})

        except Exception:
            logger.exception("Digest loop error")

        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, DIGEST_INTERVAL - elapsed))


# ---- AI Briefing voice (reads new headlines aloud) ----

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
    last_count = 0

    while True:
        t0 = time.monotonic()
        try:
            count = await asyncio.to_thread(get_event_count, 0.5)
            if count > last_count:
                last_count = count
                headlines = await asyncio.to_thread(get_recent_headlines, 0.1)
                prev = await asyncio.to_thread(get_running_log, 6.0)
                update = await generate_incremental_update(headlines, prev)

                if update:
                    entry = await asyncio.to_thread(add_summary, "incremental", update, count)
                    audio_b64 = await _make_tts_b64(update)
                    await hub.broadcast({
                        "type": "briefing",
                        "entry": entry,
                        "audio": audio_b64,
                    })
                    logger.info("Briefing broadcast (%d events)", count)
        except Exception:
            logger.exception("Briefing loop error")

        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, 25 - elapsed))


# ---- Lifecycle ----

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(run_rss_collector()),
        asyncio.create_task(run_gdelt_collector()),
        asyncio.create_task(run_digest_loop()),
        asyncio.create_task(run_briefing_loop()),
    ]
    for s in LIVE_STREAMS:
        tasks.append(asyncio.create_task(run_stream_collector(s)))
    logger.info("Started collectors + digest + briefing (%d live streams)", len(LIVE_STREAMS))
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
    return await asyncio.to_thread(get_stats, hours)


@app.get("/api/summaries")
async def get_summaries(hours: float = Query(default=6.0)):
    entries = await asyncio.to_thread(query_summaries, hours)
    return {"entries": entries}


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
