# DubToEnglish

Real-time AI-powered Arabic-to-English dubbing for YouTube Live streams. Captures audio from live broadcasts, transcribes Arabic speech, translates to English, and optionally generates English voice dubbing — all streamed live to your browser.

## How it works

```
YouTube Live → yt-dlp (extract audio) → ffmpeg (5s chunks) → Whisper (Arabic STT) → GPT-4o (translate) → TTS (English voice)
```

## Setup

**Prerequisites:** Python 3.11+, ffmpeg (`brew install ffmpeg`), an [OpenAI API key](https://platform.openai.com/api-keys)

```bash
# Clone and enter the repo
git clone https://github.com/YOUR_USERNAME/dubtoenglish_v1.git
cd dubtoenglish_v1

# Create virtualenv and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt

# Set your OpenAI API key
cp .env.example .env
# Edit .env and paste your key

# Run the server
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000 in your browser. The app comes with two hardcoded Arabic news streams and a custom URL input — hit **Start All** and watch the live translations roll in.

## Cost

Roughly ~$0.04–0.05 per minute of live stream (~$2.50/hour) across Whisper, GPT-4o, and TTS.
