# Ein Americy — MENA Open-Source Intelligence

A real-time OSINT platform for monitoring Iraq, Kurdistan and the wider region
**entirely from public reporting**. It ingests Arabic news, geolocated event
data and live broadcasts, uses AI to translate and structure them into events on
a live map, and builds a growing **actor knowledge graph** so the biggest players
— political, economic, military and religious — and the relationships between
them surface automatically.

> Scope: this tool aggregates and analyzes **public** information (news feeds,
> GDELT, public broadcasts). It is for situational awareness and analysis, not
> covert surveillance or targeting of individuals.

## How it works

```
Arabic RSS  ─┐
GDELT geo   ─┼─► raw intake buffer ─► GPT digest ─► geolocated events ─► SQLite
Live TV STT ─┘                                             │
                                                           ├─► live map + feed (WebSocket)
                                                           ├─► AI briefing (spoken, incremental)
                                                           └─► actor/relation extraction ─► knowledge graph
                                                                                              │
                                                                          network map + ranked actors + analyst Q&A
```

### Components

- **Collectors** — Arabic RSS feeds, the GDELT GEO API, and live HLS TV audio
  transcribed with Whisper. All silent background loops feeding a shared buffer.
- **AI digest** — consolidates raw text into translated, geolocated, classified
  events (severity + topics) shown on the live map and feed.
- **AI briefing** — periodic spoken briefing of *new* developments (TTS).
- **Actor knowledge graph** (`graph.py`, `extract.py`) — extracts actors
  (people, tribes, militias, parties, government/security bodies, religious and
  economic actors) and the relations between them (clash / attack / arrest /
  sanction / ally / negotiate …). Weights and influence accumulate over time, so
  the graph "grows and learns" as more reporting arrives.
- **AI analyst** (`analyst.py`) — ask free-form questions ("who is fighting whom
  in Kirkuk?", "recent arrests", "most active militias") answered over the
  current graph + recent headlines.

### Frontend

- **Map** tab — geolocated events with severity-coded, decaying pins.
- **Actor Network** tab — force-directed graph; node size = influence, color =
  dimension (political/economic/military/religious), edges colored by relation
  type (hostile vs. cooperative). Filter by dimension.
- **Ask the Analyst** — question box under the AI Briefing panel.

## Setup

**Prerequisites:** Python 3.11+, ffmpeg, an [OpenAI API key](https://platform.openai.com/api-keys)

```bash
pip install -r backend/requirements.txt
cp .env.example .env      # paste your OPENAI_API_KEY

cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

Open http://localhost:8000. Collectors, digest, briefing and the graph builder
start automatically. The actor network fills in over the first few minutes as
events are processed.

## API

| Endpoint | Description |
|---|---|
| `GET /api/events?hours=` | Recent geolocated events |
| `GET /api/stats?hours=` | Source / topic / location / severity counts |
| `GET /api/summaries?hours=` | AI briefing history |
| `GET /api/network?hours=&min_weight=&limit=` | Actor graph (nodes + edges) |
| `GET /api/actors?hours=&limit=&dimension=` | Ranked actors by influence |
| `GET /api/graph_stats` | Actor / relation counts |
| `POST /api/analyst` `{ "question": "...", "hours": 336 }` | Analyst answer |

## Cost

Digest and graph extraction run on `gpt-4o-mini`; the analyst and briefing use
`gpt-4o`. Live-stream transcription (Whisper + TTS) is the main cost driver if
enabled (~$2.50/hour per live stream).
