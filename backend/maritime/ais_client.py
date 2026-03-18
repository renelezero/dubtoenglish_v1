"""
AIS WebSocket client for real-time vessel tracking in the Strait of Hormuz.

Uses AISStream.io free tier — live AIS position reports filtered to a
bounding box around the strait and nearby Gulf ports.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import websockets

logger = logging.getLogger(__name__)

AISSTREAM_WS_URL = "wss://stream.aisstream.io/v0/stream"

# Bounding boxes: [[lat_min, lng_min], [lat_max, lng_max]]
# Box 1: Strait of Hormuz core passage
# Box 2: Extended Persian Gulf approach (Fujairah, Bandar Abbas, UAE coast)
HORMUZ_BOUNDING_BOXES = [
    [[25.3, 55.5], [27.2, 57.5]],   # strait core
    [[24.5, 53.5], [27.5, 58.5]],   # wider gulf mouth + Fujairah anchorage
]

# AIS ship type codes
VESSEL_TYPE_MAP = {
    range(60, 70): "passenger",
    range(70, 80): "cargo",
    range(80, 90): "tanker",
    range(30, 36): "fishing",
    range(40, 50): "high_speed",
    range(50, 60): "special",
    range(36, 38): "sailing",
}

def classify_vessel_type(ship_type_code: int) -> str:
    for type_range, label in VESSEL_TYPE_MAP.items():
        if ship_type_code in type_range:
            return label
    if ship_type_code in (35, 55):
        return "military"
    return "other"


class AISClient:
    """Connects to AISStream.io and yields parsed vessel position updates."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("AISSTREAM_API_KEY", "")
        self._running = False
        self._message_count = 0
        self._connect_time: float | None = None
        self._callbacks: list = []

    def on_position(self, callback):
        """Register a callback for position updates: callback(vessel_data: dict)"""
        self._callbacks.append(callback)

    @property
    def stats(self) -> dict:
        uptime = time.time() - self._connect_time if self._connect_time else 0
        return {
            "connected": self._running,
            "messages_received": self._message_count,
            "uptime_seconds": round(uptime),
        }

    def _build_subscribe_message(self) -> str:
        return json.dumps({
            "APIKey": self.api_key,
            "BoundingBoxes": HORMUZ_BOUNDING_BOXES,
            "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        })

    def _parse_position_report(self, msg: dict) -> dict | None:
        meta = msg.get("MetaData", {})
        position = msg.get("Message", {}).get("PositionReport", {})
        if not position:
            return None

        mmsi = meta.get("MMSI", 0)
        if not mmsi:
            return None

        return {
            "mmsi": mmsi,
            "name": (meta.get("ShipName") or "").strip(),
            "lat": position.get("Latitude"),
            "lng": position.get("Longitude"),
            "speed": position.get("Sog"),     # speed over ground (knots)
            "course": position.get("Cog"),     # course over ground (degrees)
            "heading": position.get("TrueHeading"),
            "nav_status": position.get("NavigationalStatus"),
            "timestamp": meta.get("time_utc") or datetime.now(timezone.utc).isoformat(),
            "ship_type_code": meta.get("ShipType", 0),
            "ship_type": classify_vessel_type(meta.get("ShipType", 0)),
            "flag": (meta.get("Flag") or "").strip(),
        }

    def _parse_static_data(self, msg: dict) -> dict | None:
        meta = msg.get("MetaData", {})
        static = msg.get("Message", {}).get("ShipStaticData", {})
        if not static:
            return None

        mmsi = meta.get("MMSI", 0)
        if not mmsi:
            return None

        dim = static.get("Dimension", {})

        return {
            "mmsi": mmsi,
            "name": (static.get("Name") or meta.get("ShipName") or "").strip(),
            "imo": static.get("ImoNumber"),
            "callsign": (static.get("CallSign") or "").strip(),
            "ship_type_code": static.get("Type", 0),
            "ship_type": classify_vessel_type(static.get("Type", 0)),
            "destination": (static.get("Destination") or "").strip(),
            "eta_month": static.get("Eta", {}).get("Month"),
            "eta_day": static.get("Eta", {}).get("Day"),
            "eta_hour": static.get("Eta", {}).get("Hour"),
            "length": (dim.get("A", 0) or 0) + (dim.get("B", 0) or 0),
            "width": (dim.get("C", 0) or 0) + (dim.get("D", 0) or 0),
            "draught": static.get("MaximumStaticDraught"),
            "flag": (meta.get("Flag") or "").strip(),
            "timestamp": meta.get("time_utc") or datetime.now(timezone.utc).isoformat(),
            "is_static": True,
        }

    async def stream(self):
        """Connect and yield parsed vessel data dicts. Reconnects on failure."""
        if not self.api_key:
            logger.error("AISSTREAM_API_KEY not set — cannot connect")
            return

        self._running = True
        backoff = 2

        while self._running:
            try:
                logger.info("Connecting to AISStream.io...")
                async with websockets.connect(AISSTREAM_WS_URL) as ws:
                    await ws.send(self._build_subscribe_message())
                    logger.info("AISStream subscribed to Hormuz bounding boxes")
                    self._connect_time = time.time()
                    backoff = 2

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        self._message_count += 1
                        msg_type = msg.get("MessageType", "")

                        if msg_type == "PositionReport":
                            parsed = self._parse_position_report(msg)
                        elif msg_type == "ShipStaticData":
                            parsed = self._parse_static_data(msg)
                        else:
                            continue

                        if parsed:
                            for cb in self._callbacks:
                                try:
                                    cb(parsed)
                                except Exception:
                                    logger.exception("Position callback error")
                            yield parsed

            except websockets.ConnectionClosed as e:
                logger.warning("AISStream connection closed: %s", e)
            except Exception:
                logger.exception("AISStream error")

            if self._running:
                logger.info("Reconnecting in %ds...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def stop(self):
        self._running = False
