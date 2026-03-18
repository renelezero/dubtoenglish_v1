import asyncio
import base64
import json
import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from stream import extract_audio_url, capture_audio_chunks, cleanup_chunk
from transcribe import transcribe_audio
from translate import translate_text
from tts import synthesize_speech_bytes

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="DubToEnglish")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")


@app.websocket("/ws/dub")
async def websocket_dub(ws: WebSocket):
    await ws.accept()
    logger.info("WebSocket connected")

    try:
        init_msg = await ws.receive_text()
        data = json.loads(init_msg)
        youtube_url = data["url"]
        enable_tts = data.get("tts", False)

        logger.info("Extracting audio URL for %s", youtube_url)
        await ws.send_json({"type": "status", "message": "Extracting stream audio..."})

        audio_url = await asyncio.to_thread(extract_audio_url, youtube_url)
        await ws.send_json({"type": "status", "message": "Stream connected. Listening..."})

        recent_translations: list[str] = []

        async for chunk_path in capture_audio_chunks(audio_url):
            try:
                arabic = await transcribe_audio(chunk_path)
                if not arabic:
                    continue

                await ws.send_json({"type": "arabic", "text": arabic})

                context = " ".join(recent_translations[-3:])
                english = await translate_text(arabic, context=context)
                payload: dict = {"type": "translation", "text": english}

                if enable_tts and english:
                    audio_bytes = await synthesize_speech_bytes(english)
                    payload["audio"] = base64.b64encode(audio_bytes).decode("utf-8")

                await ws.send_json(payload)

                if english:
                    recent_translations.append(english)
                    if len(recent_translations) > 6:
                        recent_translations = recent_translations[-6:]

            finally:
                cleanup_chunk(chunk_path)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception:
        logger.exception("Error in dub pipeline")
        try:
            await ws.send_json({"type": "error", "message": "Pipeline error — see server logs."})
        except Exception:
            pass


if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
