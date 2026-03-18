"""
AIS WebSocket client for real-time vessel tracking in the Strait of Hormuz.

Uses AISStream.io free tier — live AIS position reports filtered to
bounding boxes covering the strait, Persian Gulf, and Gulf of Oman approaches.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import websockets

logger = logging.getLogger(__name__)

AISSTREAM_WS_URL = "wss://stream.aisstream.io/v0/stream"

# Bounding boxes: [[lat_corner1, lng_corner1], [lat_corner2, lng_corner2]]
# Full Persian Gulf + Gulf of Oman coverage for maximum vessel capture
HORMUZ_BOUNDING_BOXES = [
    [[23.5, 48.0], [30.5, 57.0]],   # full Persian Gulf (Kuwait to Hormuz)
    [[22.0, 56.0], [26.5, 62.0]],   # Gulf of Oman + Fujairah + approaches
]

# AIS ship type codes — military checked first
MILITARY_CODES = {35, 55}

VESSEL_TYPE_MAP = [
    (range(60, 70), "passenger"),
    (range(70, 80), "cargo"),
    (range(80, 90), "tanker"),
    (range(40, 50), "high_speed"),
    (range(50, 60), "special"),
    (range(30, 35), "fishing"),
    (range(36, 38), "sailing"),
]

def classify_vessel_type(ship_type_code: int) -> str:
    if ship_type_code in MILITARY_CODES:
        return "military"
    for type_range, label in VESSEL_TYPE_MAP:
        if ship_type_code in type_range:
            return label
    return "other"


class AISClient:
    """Connects to AISStream.io and yields parsed vessel position updates."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("AISSTREAM_API_KEY", "")
        self._running = False
        self._message_count = 0
        self._connect_time = None  # type: Optional[float]
        self._callbacks = []       # type: list

    def on_position(self, callback):
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
            # Capture ALL position-carrying message types:
            # Class A: PositionReport (msg 1,2,3), ShipStaticData (msg 5)
            # Class B: StandardClassBPositionReport (msg 18), ExtendedClassBPositionReport (msg 19)
            # Class B static: StaticDataReport (msg 24)
            # Long range: LongRangeAisBroadcastMessage (msg 27)
            # SAR aircraft: StandardSearchAndRescueAircraftReport (msg 9)
            "FilterMessageTypes": [
                "PositionReport",
                "ShipStaticData",
                "StandardClassBPositionReport",
                "ExtendedClassBPositionReport",
                "StaticDataReport",
                "LongRangeAisBroadcastMessage",
                "StandardSearchAndRescueAircraftReport",
            ],
        })

    def _parse_position_report(self, msg: dict, msg_type: str) -> Optional[dict]:
        """Parse Class A or Class B position reports."""
        meta = msg.get("MetaData", {})
        message_body = msg.get("Message", {})

        position = (
            message_body.get("PositionReport")
            or message_body.get("StandardClassBPositionReport")
            or message_body.get("ExtendedClassBPositionReport")
            or message_body.get("LongRangeAisBroadcastMessage")
            or message_body.get("StandardSearchAndRescueAircraftReport")
        )
        if not position:
            return None

        mmsi = meta.get("MMSI") or position.get("UserID", 0)
        if not mmsi:
            return None

        lat = position.get("Latitude") or meta.get("latitude")
        lng = position.get("Longitude") or meta.get("longitude")
        if lat is None or lng is None:
            return None

        ship_type_code = position.get("Type", 0) or meta.get("ShipType", 0) or 0

        return {
            "mmsi": mmsi,
            "name": (meta.get("ShipName") or position.get("Name", "") or "").strip(),
            "lat": lat,
            "lng": lng,
            "speed": position.get("Sog"),
            "course": position.get("Cog"),
            "heading": position.get("TrueHeading"),
            "nav_status": position.get("NavigationalStatus"),
            "timestamp": meta.get("time_utc") or datetime.now(timezone.utc).isoformat(),
            "ship_type_code": ship_type_code,
            "ship_type": classify_vessel_type(ship_type_code),
            "flag": (meta.get("Flag") or "").strip(),
            "msg_type": msg_type,
        }

    def _parse_static_data(self, msg: dict, msg_type: str) -> Optional[dict]:
        """Parse Class A ShipStaticData or Class B StaticDataReport."""
        meta = msg.get("MetaData", {})
        message_body = msg.get("Message", {})

        static = message_body.get("ShipStaticData")
        static_b = message_body.get("StaticDataReport")

        mmsi = meta.get("MMSI", 0)
        if not mmsi:
            return None

        if static:
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

        if static_b:
            report_a = static_b.get("ReportA", {})
            report_b = static_b.get("ReportB", {})
            name = ""
            ship_type_code = 0
            callsign = ""
            dim = {}

            if report_a.get("Valid"):
                name = (report_a.get("Name") or "").strip()
            if report_b.get("Valid"):
                callsign = (report_b.get("CallSign") or "").strip()
                ship_type_code = report_b.get("ShipType", 0)
                dim = report_b.get("Dimension", {})

            return {
                "mmsi": mmsi,
                "name": name or (meta.get("ShipName") or "").strip(),
                "imo": None,
                "callsign": callsign,
                "ship_type_code": ship_type_code,
                "ship_type": classify_vessel_type(ship_type_code),
                "destination": "",
                "length": (dim.get("A", 0) or 0) + (dim.get("B", 0) or 0),
                "width": (dim.get("C", 0) or 0) + (dim.get("D", 0) or 0),
                "flag": (meta.get("Flag") or "").strip(),
                "timestamp": meta.get("time_utc") or datetime.now(timezone.utc).isoformat(),
                "is_static": True,
            }

        return None

    # Message types that carry position data
    _POSITION_TYPES = {
        "PositionReport",
        "StandardClassBPositionReport",
        "ExtendedClassBPositionReport",
        "LongRangeAisBroadcastMessage",
        "StandardSearchAndRescueAircraftReport",
    }

    _STATIC_TYPES = {"ShipStaticData", "StaticDataReport"}

    async def stream(self):
        """Connect and yield parsed vessel data dicts. Reconnects on failure."""
        if not self.api_key or self.api_key in ("", "YOUR_KEY_HERE", "your_key_here"):
            logger.error(
                "AISSTREAM_API_KEY not set — sign up at https://aisstream.io, "
                "generate a key, and add it to your .env file"
            )
            return

        self._running = True
        backoff = 2

        while self._running:
            try:
                logger.info("Connecting to AISStream.io...")
                async with websockets.connect(
                    AISSTREAM_WS_URL,
                    open_timeout=15,
                    close_timeout=5,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    await ws.send(self._build_subscribe_message())
                    logger.info(
                        "AISStream subscribed — bounding boxes cover full Persian Gulf + Gulf of Oman"
                    )
                    self._connect_time = time.time()
                    backoff = 2

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        self._message_count += 1
                        msg_type = msg.get("MessageType", "")

                        if msg_type in self._POSITION_TYPES:
                            parsed = self._parse_position_report(msg, msg_type)
                        elif msg_type in self._STATIC_TYPES:
                            parsed = self._parse_static_data(msg, msg_type)
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
