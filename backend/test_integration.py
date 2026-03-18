"""
End-to-end integration test for the DubToEnglish pipeline.

Tests each stage of the pipeline individually then runs a full
end-to-end pass against a live Al Arabiya stream.

Usage:
    python test_integration.py                  # Run all tests
    python test_integration.py --stage extract   # Test only yt-dlp extraction
    python test_integration.py --stage chunk     # Test only ffmpeg chunking
    python test_integration.py --stage transcribe # Test only Whisper
    python test_integration.py --stage translate  # Test only GPT-4o
    python test_integration.py --stage tts        # Test only TTS
    python test_integration.py --stage pipeline   # Full end-to-end
    python test_integration.py --stage server     # Test WebSocket server
"""

import argparse
import asyncio
import base64
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

AL_ARABIYA_LIVE = "https://www.youtube.com/watch?v=n7eQejkXbnM"

PASS = "\033[92m✓ PASS\033[0m"
FAIL = "\033[91m✗ FAIL\033[0m"
SKIP = "\033[93m⊘ SKIP\033[0m"
INFO = "\033[94mℹ\033[0m"


def has_api_key() -> bool:
    return bool(os.environ.get("OPENAI_API_KEY"))


def print_header(name: str):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")


# ---------- Stage 1: yt-dlp extraction ----------

def test_extract_audio_url():
    print_header("Stage 1: yt-dlp — Extract Audio URL")
    from stream import extract_audio_url

    t0 = time.time()
    try:
        audio_url = extract_audio_url(AL_ARABIYA_LIVE)
        elapsed = time.time() - t0
        assert audio_url and audio_url.startswith("http"), "URL must be a valid HTTP URL"
        print(f"  {PASS}  Extracted audio URL in {elapsed:.1f}s")
        print(f"  {INFO}  URL prefix: {audio_url[:100]}...")
        return audio_url
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  {FAIL}  Extraction failed after {elapsed:.1f}s: {e}")
        return None


# ---------- Stage 2: ffmpeg chunking ----------

async def test_capture_chunk(audio_url: str):
    print_header("Stage 2: ffmpeg — Capture Audio Chunk")
    from stream import capture_audio_chunks, cleanup_chunk

    if not audio_url:
        print(f"  {SKIP}  No audio URL (Stage 1 failed)")
        return None

    t0 = time.time()
    try:
        chunk_path = None
        async for path in capture_audio_chunks(audio_url, chunk_duration=5.0):
            chunk_path = path
            break  # only capture one chunk for testing

        elapsed = time.time() - t0
        assert chunk_path and os.path.isfile(chunk_path), "Chunk file must exist"
        size = os.path.getsize(chunk_path)
        assert size > 1000, f"Chunk too small ({size} bytes)"
        print(f"  {PASS}  Captured chunk in {elapsed:.1f}s — {size:,} bytes")
        print(f"  {INFO}  Path: {chunk_path}")
        return chunk_path
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  {FAIL}  Chunk capture failed after {elapsed:.1f}s: {e}")
        return None


# ---------- Stage 3: Whisper transcription ----------

async def test_transcribe(chunk_path: str):
    print_header("Stage 3: Whisper — Arabic Transcription")
    from transcribe import transcribe_audio

    if not has_api_key():
        print(f"  {SKIP}  No OPENAI_API_KEY set")
        return None
    if not chunk_path:
        print(f"  {SKIP}  No audio chunk (Stage 2 failed)")
        return None

    t0 = time.time()
    try:
        arabic_text = await transcribe_audio(chunk_path)
        elapsed = time.time() - t0
        assert isinstance(arabic_text, str), "Transcription must return a string"
        print(f"  {PASS}  Transcribed in {elapsed:.1f}s — {len(arabic_text)} chars")
        print(f"  {INFO}  Arabic: {arabic_text[:200]}")
        return arabic_text
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  {FAIL}  Transcription failed after {elapsed:.1f}s: {e}")
        return None


# ---------- Stage 4: GPT-4o translation ----------

async def test_translate(arabic_text: str):
    print_header("Stage 4: GPT-4o — Arabic → English Translation")
    from translate import translate_text

    if not has_api_key():
        print(f"  {SKIP}  No OPENAI_API_KEY set")
        return None
    if not arabic_text:
        print(f"  {SKIP}  No Arabic text (Stage 3 failed or returned empty)")
        return None

    t0 = time.time()
    try:
        english_text = await translate_text(arabic_text)
        elapsed = time.time() - t0
        assert isinstance(english_text, str) and english_text, "Translation must return non-empty string"
        print(f"  {PASS}  Translated in {elapsed:.1f}s — {len(english_text)} chars")
        print(f"  {INFO}  English: {english_text[:200]}")
        return english_text
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  {FAIL}  Translation failed after {elapsed:.1f}s: {e}")
        return None


# ---------- Stage 5: TTS synthesis ----------

async def test_tts(english_text: str):
    print_header("Stage 5: OpenAI TTS — English Speech Synthesis")
    from tts import synthesize_speech_bytes

    if not has_api_key():
        print(f"  {SKIP}  No OPENAI_API_KEY set")
        return None
    if not english_text:
        print(f"  {SKIP}  No English text (Stage 4 failed)")
        return None

    t0 = time.time()
    try:
        audio_bytes = await synthesize_speech_bytes(english_text)
        elapsed = time.time() - t0
        assert isinstance(audio_bytes, bytes) and len(audio_bytes) > 100, "TTS must return audio bytes"
        b64 = base64.b64encode(audio_bytes).decode("utf-8")
        print(f"  {PASS}  Synthesized in {elapsed:.1f}s — {len(audio_bytes):,} bytes audio")
        print(f"  {INFO}  Base64 length: {len(b64):,} chars")
        return audio_bytes
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  {FAIL}  TTS failed after {elapsed:.1f}s: {e}")
        return None


# ---------- Stage 6: Full pipeline ----------

async def test_full_pipeline():
    print_header("Stage 6: Full End-to-End Pipeline")
    from stream import extract_audio_url, capture_audio_chunks, cleanup_chunk
    from transcribe import transcribe_audio
    from translate import translate_text
    from tts import synthesize_speech_bytes

    if not has_api_key():
        print(f"  {SKIP}  No OPENAI_API_KEY set")
        return

    t_total = time.time()
    try:
        print(f"  {INFO}  Extracting audio URL...")
        audio_url = await asyncio.to_thread(extract_audio_url, AL_ARABIYA_LIVE)
        print(f"  {INFO}  Capturing 5s audio chunk...")

        chunk_path = None
        async for path in capture_audio_chunks(audio_url, chunk_duration=5.0):
            chunk_path = path
            break

        print(f"  {INFO}  Transcribing Arabic...")
        arabic = await transcribe_audio(chunk_path)
        print(f"  {INFO}  Arabic: {arabic[:150]}")

        print(f"  {INFO}  Translating to English...")
        english = await translate_text(arabic)
        print(f"  {INFO}  English: {english[:150]}")

        print(f"  {INFO}  Synthesizing speech...")
        audio_bytes = await synthesize_speech_bytes(english)

        cleanup_chunk(chunk_path)

        elapsed = time.time() - t_total
        print(f"\n  {PASS}  Full pipeline completed in {elapsed:.1f}s")
        print(f"  {INFO}  Arabic chars: {len(arabic)}")
        print(f"  {INFO}  English chars: {len(english)}")
        print(f"  {INFO}  Audio bytes: {len(audio_bytes):,}")

    except Exception as e:
        elapsed = time.time() - t_total
        print(f"  {FAIL}  Pipeline failed after {elapsed:.1f}s: {e}")
        import traceback
        traceback.print_exc()


# ---------- Stage 7: WebSocket server ----------

async def test_server():
    print_header("Stage 7: WebSocket Server (quick connection test)")
    import websockets

    if not has_api_key():
        print(f"  {SKIP}  No OPENAI_API_KEY set (server needs it for API calls)")
        return

    print(f"  {INFO}  Starting uvicorn in background...")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "uvicorn", "main:app",
        "--host", "127.0.0.1", "--port", "8765",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await asyncio.sleep(2)

    try:
        async with websockets.connect("ws://127.0.0.1:8765/ws/dub") as ws_conn:
            print(f"  {PASS}  WebSocket connected to ws://127.0.0.1:8765/ws/dub")

            await ws_conn.send(json.dumps({"url": AL_ARABIYA_LIVE, "tts": False}))
            print(f"  {INFO}  Sent start message, waiting for responses...")

            messages_received = 0
            try:
                while messages_received < 4:
                    msg = await asyncio.wait_for(ws_conn.recv(), timeout=60)
                    data = json.loads(msg)
                    messages_received += 1
                    print(f"  {INFO}  [{messages_received}] type={data['type']}: {str(data.get('text', data.get('message', '')))[:100]}")
            except asyncio.TimeoutError:
                print(f"  {INFO}  Timed out after {messages_received} messages (may be normal if stream is slow)")

            if messages_received > 0:
                print(f"  {PASS}  Received {messages_received} messages from server")
            else:
                print(f"  {FAIL}  No messages received")

    except Exception as e:
        print(f"  {FAIL}  Server test failed: {e}")
    finally:
        proc.terminate()
        await proc.wait()
        print(f"  {INFO}  Server stopped")


# ---------- Runner ----------

async def run_all():
    print("\n" + "=" * 60)
    print("  DubToEnglish — End-to-End Integration Test")
    print(f"  Stream: {AL_ARABIYA_LIVE}")
    print(f"  API Key: {'configured' if has_api_key() else 'NOT SET (some tests will be skipped)'}")
    print("=" * 60)

    audio_url = test_extract_audio_url()
    chunk_path = await test_capture_chunk(audio_url)
    arabic_text = await test_transcribe(chunk_path)
    english_text = await test_translate(arabic_text)
    await test_tts(english_text)

    if chunk_path:
        from stream import cleanup_chunk
        cleanup_chunk(chunk_path)

    await test_full_pipeline()
    await test_server()

    print("\n" + "=" * 60)
    print("  Integration test complete")
    print("=" * 60 + "\n")


async def run_stage(stage: str):
    if stage == "extract":
        test_extract_audio_url()
    elif stage == "chunk":
        audio_url = test_extract_audio_url()
        await test_capture_chunk(audio_url)
    elif stage == "transcribe":
        audio_url = test_extract_audio_url()
        chunk_path = await test_capture_chunk(audio_url)
        await test_transcribe(chunk_path)
        if chunk_path:
            from stream import cleanup_chunk
            cleanup_chunk(chunk_path)
    elif stage == "translate":
        audio_url = test_extract_audio_url()
        chunk_path = await test_capture_chunk(audio_url)
        arabic = await test_transcribe(chunk_path)
        await test_translate(arabic)
        if chunk_path:
            from stream import cleanup_chunk
            cleanup_chunk(chunk_path)
    elif stage == "tts":
        audio_url = test_extract_audio_url()
        chunk_path = await test_capture_chunk(audio_url)
        arabic = await test_transcribe(chunk_path)
        english = await test_translate(arabic)
        await test_tts(english)
        if chunk_path:
            from stream import cleanup_chunk
            cleanup_chunk(chunk_path)
    elif stage == "pipeline":
        await test_full_pipeline()
    elif stage == "server":
        await test_server()
    else:
        print(f"Unknown stage: {stage}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="DubToEnglish integration test")
    parser.add_argument("--stage", type=str, default=None,
                        help="Run a specific stage: extract, chunk, transcribe, translate, tts, pipeline, server")
    parser.add_argument("--url", type=str, default=None,
                        help="Override the YouTube Live URL to test with")
    args = parser.parse_args()

    if args.url:
        global AL_ARABIYA_LIVE
        AL_ARABIYA_LIVE = args.url

    if args.stage:
        asyncio.run(run_stage(args.stage))
    else:
        asyncio.run(run_all())


if __name__ == "__main__":
    main()
