import hashlib
import logging
from datetime import datetime, timezone

import feedparser
import httpx

logger = logging.getLogger(__name__)

RSS_FEEDS = [
    {"id": "aljazeera", "name": "Al Jazeera", "url": "https://www.aljazeera.net/rss"},
    {"id": "alarabiya", "name": "Al Arabiya", "url": "https://www.alarabiya.net/feed/rss2"},
]

GDELT_GEO_URL = (
    "https://api.gdeltproject.org/api/v2/geo/geo"
    "?query=sourcelang:arabic&mode=PointData&format=GeoJSON&timespan=15min"
)

_seen: set[str] = set()
_MAX_SEEN = 10_000


def _item_id(source_id: str, url: str, title: str) -> str:
    raw = f"{source_id}:{url or title}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _is_new(item_id: str) -> bool:
    if item_id in _seen:
        return False
    _seen.add(item_id)
    if len(_seen) > _MAX_SEEN:
        to_remove = list(_seen)[:_MAX_SEEN // 2]
        for k in to_remove:
            _seen.discard(k)
    return True


def fetch_rss_feeds() -> list[dict]:
    """Fetch all RSS feeds and return new (unseen) items."""
    items: list[dict] = []
    for feed_cfg in RSS_FEEDS:
        try:
            parsed = feedparser.parse(feed_cfg["url"])
            for entry in parsed.entries[:15]:
                url = getattr(entry, "link", "") or ""
                title = getattr(entry, "title", "") or ""
                iid = _item_id(feed_cfg["id"], url, title)
                if not _is_new(iid):
                    continue
                summary_ar = getattr(entry, "summary", "") or ""
                items.append({
                    "id": iid,
                    "source_id": feed_cfg["id"],
                    "source_name": feed_cfg["name"],
                    "headline_ar": title,
                    "body_ar": summary_ar[:800],
                    "url": url,
                    "origin": "rss",
                })
        except Exception:
            logger.exception("RSS fetch failed for %s", feed_cfg["name"])
    return items


async def fetch_gdelt_events() -> list[dict]:
    """Fetch geolocated events from the GDELT GEO API."""
    items: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(GDELT_GEO_URL)
            resp.raise_for_status()
            data = resp.json()

        for feature in data.get("features", [])[:30]:
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [])
            url = props.get("url", "")
            name = props.get("name", "")
            iid = _item_id("gdelt", url, name)
            if not _is_new(iid):
                continue
            lng = coords[0] if len(coords) > 0 else None
            lat = coords[1] if len(coords) > 1 else None
            items.append({
                "id": iid,
                "source_id": "gdelt",
                "source_name": props.get("domain", "GDELT"),
                "headline_ar": name,
                "body_ar": "",
                "url": url,
                "origin": "gdelt",
                "pre_lat": lat,
                "pre_lng": lng,
            })
    except Exception:
        logger.exception("GDELT fetch failed")
    return items
