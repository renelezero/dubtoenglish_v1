"""
Standalone maritime intelligence server.

Run independently from the main Ein Americy app:
    cd backend && python -m maritime.run_standalone

Provides:
- Live AIS vessel tracking via WebSocket
- REST API for vessel positions, stats, anomalies
- Port scraping endpoints
- Maritime news + analysis pipeline
- Maritime intelligence briefings correlating vessels + news
"""

import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from maritime.ais_client import AISClient
from maritime.db import (
    init_db, get_vessel_history, get_all_vessels_db, get_vessel_db,
    get_traffic_summary, get_db_stats, search_vessels,
)
from maritime.vessel_store import (
    update_vessel, get_active_vessels, get_vessel, get_vessel_track,
    get_maritime_stats, get_anomalies, get_tanker_flow,
)
from maritime.maritime_scraper import (
    fetch_maritime_news, scrape_all_ports, get_port_metadata,
)
from maritime.maritime_analyze import analyze_maritime_situation, analyze_maritime_news

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

NEWS_INTERVAL = 300       # maritime news every 5 min
ANALYSIS_INTERVAL = 600   # full analysis every 10 min
PORT_SCRAPE_INTERVAL = 3600

ais = AISClient()


# ---- WebSocket broadcast hub ----

class MaritimeHub:
    def __init__(self):
        self._clients = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._clients.add(ws)
        logger.info("Maritime WS client connected (%d total)", len(self._clients))

    def disconnect(self, ws: WebSocket):
        self._clients.discard(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self._clients:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._clients.discard(ws)


hub = MaritimeHub()


# ---- Background tasks ----

_maritime_news_cache = []       # type: list
_latest_analysis = None         # type: dict or None
_port_data = {}                 # type: dict


async def run_ais_stream():
    """Consume AIS stream, update store, broadcast positions."""
    count = 0
    async for data in ais.stream():
        vessel = update_vessel(data)

        count += 1
        if count % 10 == 0:
            await hub.broadcast({"type": "vessel_update", "vessel": vessel})

        if count % 100 == 0:
            stats = get_maritime_stats()
            await hub.broadcast({"type": "maritime_stats", "stats": stats})
            logger.info(
                "AIS: %d messages, %d active vessels, %d tankers",
                ais.stats["messages_received"],
                stats["total_vessels"],
                stats["tanker_count"],
            )


async def run_maritime_news_loop():
    """Fetch and analyze maritime-specific Arabic news."""
    global _maritime_news_cache
    await asyncio.sleep(10)

    while True:
        t0 = time.monotonic()
        try:
            items = await fetch_maritime_news()
            for item in items:
                analyzed = await analyze_maritime_news(item)
                if analyzed:
                    _maritime_news_cache.append(analyzed)
                    if len(_maritime_news_cache) > 200:
                        _maritime_news_cache = _maritime_news_cache[-200:]
                    await hub.broadcast({"type": "maritime_news", "item": analyzed})
        except Exception:
            logger.exception("Maritime news loop error")

        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, NEWS_INTERVAL - elapsed))


async def run_analysis_loop():
    """Periodic full maritime intelligence analysis."""
    global _latest_analysis
    await asyncio.sleep(60)

    while True:
        t0 = time.monotonic()
        try:
            stats = get_maritime_stats()
            flow = get_tanker_flow()
            anomaly_list = get_anomalies(60)
            vessels = get_active_vessels()

            news_text = "\n".join(
                f"- ({n.get('source_name', '')}) {n.get('headline_en', '')}"
                for n in _maritime_news_cache[-20:]
            )

            if stats["total_vessels"] > 0 or news_text:
                result = await analyze_maritime_situation(
                    vessel_stats=stats,
                    tanker_flow=flow,
                    anomalies=anomaly_list,
                    recent_news=news_text,
                    active_vessel_count=len(vessels),
                )
                if result:
                    _latest_analysis = {
                        **result,
                        "timestamp": asyncio.get_event_loop().time(),
                        "vessel_count": len(vessels),
                    }
                    await hub.broadcast({"type": "maritime_analysis", "analysis": _latest_analysis})
                    logger.info(
                        "Maritime analysis: threat=%s, %d correlations",
                        result.get("threat_level", "unknown"),
                        len(result.get("correlations", [])),
                    )
        except Exception:
            logger.exception("Analysis loop error")

        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, ANALYSIS_INTERVAL - elapsed))


async def run_port_scrape_loop():
    """Periodic port vessel scraping."""
    global _port_data
    await asyncio.sleep(30)

    while True:
        t0 = time.monotonic()
        try:
            _port_data = await scrape_all_ports()
            total = sum(len(v) for v in _port_data.values())
            logger.info("Port scrape complete: %d vessels across %d ports", total, len(_port_data))
        except Exception:
            logger.exception("Port scrape error")

        elapsed = time.monotonic() - t0
        await asyncio.sleep(max(0, PORT_SCRAPE_INTERVAL - elapsed))


# ---- Lifecycle ----

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    tasks = [
        asyncio.create_task(run_ais_stream()),
        asyncio.create_task(run_maritime_news_loop()),
        asyncio.create_task(run_analysis_loop()),
        asyncio.create_task(run_port_scrape_loop()),
    ]
    logger.info("Maritime intelligence server started (4 background tasks, SQLite persistence ON)")
    yield
    ais.stop()
    for t in tasks:
        t.cancel()


app = FastAPI(title="Maritime Intelligence — Strait of Hormuz", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- API Endpoints ----

@app.get("/api/maritime/vessels")
async def api_vessels(minutes: int = Query(default=30)):
    vessels = get_active_vessels(minutes)
    return {"vessels": vessels, "count": len(vessels)}


@app.get("/api/maritime/vessel/{mmsi}")
async def api_vessel(mmsi: int):
    vessel = get_vessel(mmsi)
    if not vessel:
        return {"error": "Vessel not found"}
    track = get_vessel_track(mmsi, 60)
    return {"vessel": vessel, "track": track}


@app.get("/api/maritime/stats")
async def api_stats(minutes: int = Query(default=30)):
    stats = get_maritime_stats(minutes)
    flow = get_tanker_flow(minutes)
    return {"stats": stats, "tanker_flow": flow}


@app.get("/api/maritime/anomalies")
async def api_anomalies(minutes: int = Query(default=60)):
    anomalies = get_anomalies(minutes)
    return {"anomalies": anomalies, "count": len(anomalies)}


@app.get("/api/maritime/analysis")
async def api_analysis():
    return {"analysis": _latest_analysis}


@app.get("/api/maritime/news")
async def api_news(limit: int = Query(default=20)):
    return {"news": _maritime_news_cache[-limit:], "count": len(_maritime_news_cache)}


@app.get("/api/maritime/ports")
async def api_ports():
    return {
        "ports": get_port_metadata(),
        "port_vessels": {k: len(v) for k, v in _port_data.items()},
    }


@app.get("/api/maritime/ports/{port_id}/vessels")
async def api_port_vessels(port_id: str):
    vessels = _port_data.get(port_id, [])
    return {"port_id": port_id, "vessels": vessels, "count": len(vessels)}


@app.get("/api/maritime/ais_status")
async def api_ais_status():
    return {"ais": ais.stats}


# ---- Historical / Database endpoints ----

@app.get("/api/maritime/history/vessel/{mmsi}")
async def api_vessel_history(mmsi: int, hours: float = Query(default=None)):
    """Full position history for a vessel from SQLite."""
    history = await asyncio.to_thread(get_vessel_history, mmsi, hours)
    vessel = await asyncio.to_thread(get_vessel_db, mmsi)
    return {"vessel": vessel, "positions": history, "count": len(history)}


@app.get("/api/maritime/history/all")
async def api_all_vessels_history(hours: float = Query(default=None)):
    """All vessels ever seen, optionally filtered by recent activity."""
    vessels = await asyncio.to_thread(get_all_vessels_db, hours)
    return {"vessels": vessels, "count": len(vessels)}


@app.get("/api/maritime/history/traffic")
async def api_traffic_history(hours: float = Query(default=1.0)):
    """Traffic summary from database."""
    summary = await asyncio.to_thread(get_traffic_summary, hours)
    return summary


@app.get("/api/maritime/history/search")
async def api_search_vessels(q: str = Query(...), limit: int = Query(default=50)):
    """Search vessels by name, MMSI, flag, destination, or callsign."""
    results = await asyncio.to_thread(search_vessels, q, limit)
    return {"results": results, "count": len(results)}


@app.get("/api/maritime/db_stats")
async def api_db_stats():
    """Database size and record counts."""
    stats = await asyncio.to_thread(get_db_stats)
    return stats


@app.websocket("/ws/maritime")
async def websocket_maritime(ws: WebSocket):
    await hub.connect(ws)
    try:
        # Send initial state on connect
        vessels = get_active_vessels()
        stats = get_maritime_stats()
        flow = get_tanker_flow()
        await ws.send_json({
            "type": "initial_state",
            "vessels": vessels,
            "stats": stats,
            "tanker_flow": flow,
            "ports": get_port_metadata(),
            "analysis": _latest_analysis,
        })
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(ws)


# ---- Standalone dashboard ----

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hormuz Maritime Intelligence</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'SF Mono', 'Fira Code', monospace; background: #0a0e17; color: #c8d6e5; }
  .header { background: #0d1321; padding: 12px 20px; display: flex; justify-content: space-between;
            align-items: center; border-bottom: 1px solid #1a2332; }
  .header h1 { font-size: 16px; color: #00d4ff; letter-spacing: 1px; }
  .threat-badge { padding: 4px 12px; border-radius: 4px; font-size: 12px; font-weight: bold; text-transform: uppercase; }
  .threat-normal { background: #065f46; color: #6ee7b7; }
  .threat-elevated { background: #78350f; color: #fbbf24; }
  .threat-high { background: #7c2d12; color: #fb923c; }
  .threat-critical { background: #7f1d1d; color: #fca5a5; }
  .layout { display: grid; grid-template-columns: 1fr 340px; grid-template-rows: auto 1fr; height: calc(100vh - 49px); }
  .stats-bar { grid-column: 1 / -1; display: flex; gap: 24px; padding: 10px 20px;
              background: #0d1321; border-bottom: 1px solid #1a2332; }
  .stat { text-align: center; }
  .stat-value { font-size: 22px; font-weight: bold; color: #00d4ff; }
  .stat-label { font-size: 10px; color: #5a6b7f; text-transform: uppercase; letter-spacing: 1px; }
  #map { width: 100%; height: 100%; }
  .sidebar { background: #0d1321; border-left: 1px solid #1a2332; overflow-y: auto; padding: 12px; }
  .panel { margin-bottom: 16px; }
  .panel-title { font-size: 11px; color: #5a6b7f; text-transform: uppercase; letter-spacing: 1px;
                margin-bottom: 8px; padding-bottom: 4px; border-bottom: 1px solid #1a2332; }
  .vessel-item { padding: 6px 8px; margin-bottom: 4px; background: #111827; border-radius: 4px;
                font-size: 12px; border-left: 3px solid #1e3a5f; }
  .vessel-item.tanker { border-left-color: #f59e0b; }
  .vessel-item.military { border-left-color: #ef4444; }
  .vessel-name { font-weight: bold; color: #e2e8f0; }
  .vessel-meta { color: #5a6b7f; font-size: 10px; margin-top: 2px; }
  .anomaly-item { padding: 6px 8px; margin-bottom: 4px; background: #1c1107; border-radius: 4px;
                 font-size: 11px; border-left: 3px solid #f59e0b; }
  .anomaly-item.high { border-left-color: #ef4444; background: #1c0707; }
  .news-item { padding: 6px 8px; margin-bottom: 4px; background: #111827; border-radius: 4px; font-size: 11px; }
  .news-source { color: #00d4ff; font-size: 10px; }
  .briefing-box { padding: 10px; background: #111827; border-radius: 6px; font-size: 12px; line-height: 1.5; }
  .conn-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
  .conn-dot.live { background: #22c55e; box-shadow: 0 0 6px #22c55e; }
  .conn-dot.off { background: #ef4444; }
  #connStatus { font-size: 11px; color: #5a6b7f; }
</style>
</head>
<body>
<div class="header">
  <h1>HORMUZ MARITIME INTELLIGENCE</h1>
  <div>
    <span class="conn-dot off" id="connDot"></span>
    <span id="connStatus">Connecting...</span>
    <span class="threat-badge threat-normal" id="threatBadge">NORMAL</span>
  </div>
</div>
<div class="layout">
  <div class="stats-bar">
    <div class="stat"><div class="stat-value" id="sTotal">0</div><div class="stat-label">Vessels</div></div>
    <div class="stat"><div class="stat-value" id="sTankers">0</div><div class="stat-label">Tankers</div></div>
    <div class="stat"><div class="stat-value" id="sMilitary">0</div><div class="stat-label">Military</div></div>
    <div class="stat"><div class="stat-value" id="sInbound">0</div><div class="stat-label">Inbound</div></div>
    <div class="stat"><div class="stat-value" id="sOutbound">0</div><div class="stat-label">Outbound</div></div>
    <div class="stat"><div class="stat-value" id="sSpeed">0</div><div class="stat-label">Avg Knots</div></div>
    <div class="stat"><div class="stat-value" id="sAnomalies">0</div><div class="stat-label">Anomalies</div></div>
    <div class="stat"><div class="stat-value" id="sAIS">0</div><div class="stat-label">AIS Msgs</div></div>
  </div>
  <div id="map"></div>
  <div class="sidebar">
    <div class="panel">
      <div class="panel-title">AI Briefing</div>
      <div class="briefing-box" id="briefingBox">Waiting for data...</div>
    </div>
    <div class="panel">
      <div class="panel-title">Anomalies</div>
      <div id="anomalyList"><div style="color:#5a6b7f;font-size:11px;">Monitoring...</div></div>
    </div>
    <div class="panel">
      <div class="panel-title">Maritime News</div>
      <div id="newsList"><div style="color:#5a6b7f;font-size:11px;">Fetching...</div></div>
    </div>
    <div class="panel">
      <div class="panel-title">Recent Vessels</div>
      <div id="vesselList"></div>
    </div>
  </div>
</div>
<script>
(function(){
  const map = L.map('map',{center:[26.3,56.5],zoom:8,zoomControl:false,attributionControl:false});
  L.control.zoom({position:'bottomright'}).addTo(map);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',{maxZoom:18}).addTo(map);

  const vesselMarkers = {};
  const portMarkers = L.layerGroup().addTo(map);

  const TYPE_COLORS = {
    tanker:'#f59e0b', cargo:'#3b82f6', passenger:'#8b5cf6',
    military:'#ef4444', fishing:'#22c55e', other:'#6b7280'
  };

  function addPort(p){
    L.circleMarker([p.lat,p.lng],{radius:5,fillColor:'#00d4ff',color:'#00d4ff',weight:1,fillOpacity:0.8})
      .bindPopup('<b>'+p.name+'</b><br>'+p.country).addTo(portMarkers);
  }

  function updateVessel(v){
    if(!v.lat||!v.lng) return;
    const color = TYPE_COLORS[v.ship_type]||TYPE_COLORS.other;
    if(vesselMarkers[v.mmsi]){
      vesselMarkers[v.mmsi].setLatLng([v.lat,v.lng]);
      vesselMarkers[v.mmsi].setStyle({fillColor:color,color:color});
    } else {
      const m = L.circleMarker([v.lat,v.lng],{radius:4,fillColor:color,color:color,weight:1,fillOpacity:0.8});
      m.bindPopup('<b>'+(v.name||'MMSI:'+v.mmsi)+'</b><br>Type: '+v.ship_type
        +'<br>Flag: '+(v.flag||'?')+'<br>Speed: '+(v.speed||0)+' kn'
        +'<br>Dest: '+(v.destination||'?'));
      m.addTo(map);
      vesselMarkers[v.mmsi]=m;
    }
  }

  function updateStats(s,f){
    document.getElementById('sTotal').textContent=s.total_vessels||0;
    document.getElementById('sTankers').textContent=s.tanker_count||0;
    document.getElementById('sMilitary').textContent=s.military_count||0;
    document.getElementById('sSpeed').textContent=s.avg_speed_knots||0;
    if(f){
      document.getElementById('sInbound').textContent=f.inbound_tankers||0;
      document.getElementById('sOutbound').textContent=f.outbound_tankers||0;
    }
  }

  function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}

  function addAnomalyItem(a){
    const el=document.getElementById('anomalyList');
    if(el.querySelector('div[style]'))el.innerHTML='';
    const d=document.createElement('div');
    d.className='anomaly-item'+(a.severity==='high'?' high':'');
    d.innerHTML='<div>'+esc(a.description||'')+'</div>';
    el.prepend(d);
    while(el.children.length>20)el.removeChild(el.lastChild);
    document.getElementById('sAnomalies').textContent=el.children.length;
  }

  function addNewsItem(n){
    const el=document.getElementById('newsList');
    if(el.querySelector('div[style]'))el.innerHTML='';
    const d=document.createElement('div');
    d.className='news-item';
    d.innerHTML='<div class="news-source">'+esc(n.source_name||'')+'</div>'
      +'<div>'+esc(n.headline_en||n.headline_ar||'')+'</div>';
    el.prepend(d);
    while(el.children.length>30)el.removeChild(el.lastChild);
  }

  function addVesselItem(v){
    const el=document.getElementById('vesselList');
    const d=document.createElement('div');
    d.className='vessel-item'+(v.ship_type==='tanker'?' tanker':'')+(v.ship_type==='military'?' military':'');
    d.innerHTML='<div class="vessel-name">'+esc(v.name||'MMSI:'+v.mmsi)+'</div>'
      +'<div class="vessel-meta">'+v.ship_type+' | '+v.flag+' | '+(v.speed||0)+' kn</div>';
    el.prepend(d);
    while(el.children.length>40)el.removeChild(el.lastChild);
  }

  function updateBriefing(a){
    const box=document.getElementById('briefingBox');
    box.textContent=a.maritime_briefing||'No analysis yet';
    const badge=document.getElementById('threatBadge');
    const lvl=a.threat_level||'normal';
    badge.textContent=lvl.toUpperCase();
    badge.className='threat-badge threat-'+lvl;
  }

  const proto=location.protocol==='https:'?'wss:':'ws:';
  const ws=new WebSocket(proto+'//'+location.host+'/ws/maritime');

  ws.onopen=function(){
    document.getElementById('connDot').className='conn-dot live';
    document.getElementById('connStatus').textContent='Live';
  };
  ws.onclose=function(){
    document.getElementById('connDot').className='conn-dot off';
    document.getElementById('connStatus').textContent='Disconnected';
    setTimeout(()=>location.reload(),5000);
  };
  ws.onmessage=function(e){
    let msg;try{msg=JSON.parse(e.data)}catch{return}
    switch(msg.type){
      case 'initial_state':
        (msg.ports||[]).forEach(addPort);
        (msg.vessels||[]).forEach(v=>{updateVessel(v);addVesselItem(v);});
        if(msg.stats)updateStats(msg.stats,msg.tanker_flow);
        if(msg.analysis)updateBriefing(msg.analysis);
        break;
      case 'vessel_update':
        if(msg.vessel){updateVessel(msg.vessel);addVesselItem(msg.vessel);}
        break;
      case 'maritime_stats':
        if(msg.stats)updateStats(msg.stats);
        break;
      case 'maritime_news':
        if(msg.item)addNewsItem(msg.item);
        break;
      case 'maritime_analysis':
        if(msg.analysis)updateBriefing(msg.analysis);
        break;
    }
  };

  // Poll AIS message count
  setInterval(async()=>{
    try{
      const r=await fetch('/api/maritime/ais_status');
      const d=await r.json();
      document.getElementById('sAIS').textContent=d.ais?.messages_received||0;
    }catch{}
  },5000);

  // Poll anomalies
  setInterval(async()=>{
    try{
      const r=await fetch('/api/maritime/anomalies?minutes=60');
      const d=await r.json();
      if(d.anomalies){
        document.getElementById('anomalyList').innerHTML='';
        d.anomalies.slice(-10).forEach(addAnomalyItem);
      }
    }catch{}
  },30000);
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("MARITIME_PORT", "8001"))
    logger.info("Starting Maritime Intelligence server on port %d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
