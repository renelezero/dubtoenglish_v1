import hashlib
import logging
from datetime import datetime, timezone

import feedparser
import httpx

logger = logging.getLogger(__name__)

RSS_FEEDS = [
    {"id": "aljazeera", "name": "Al Jazeera", "url": "https://www.aljazeera.net/rss"},
    {"id": "alarabiya", "name": "Al Arabiya", "url": "https://www.alarabiya.net/feed/rss2"},
    {"id": "skynews_ar", "name": "Sky News Arabia", "url": "https://www.skynewsarabia.com/web/rss"},
    {"id": "bbc_arabic", "name": "BBC Arabic", "url": "https://feeds.bbci.co.uk/arabic/rss.xml"},
    {"id": "france24_ar", "name": "France 24 Arabic", "url": "https://www.france24.com/ar/rss"},
    {"id": "rt_arabic", "name": "RT Arabic", "url": "https://arabic.rt.com/rss/"},
    {"id": "newarab", "name": "The New Arab", "url": "https://www.newarab.com/rss"},
    {"id": "alsharq", "name": "Al Sharq", "url": "https://al-sharq.com/rss/latestNews"},
    {"id": "middleeasteye", "name": "Middle East Eye", "url": "https://www.middleeasteye.net/rss"},
    {"id": "almayadeen", "name": "Al Mayadeen", "url": "https://www.almayadeen.net/rss/all"},
    {"id": "anadolu_ar", "name": "Anadolu Arabic", "url": "https://www.aa.com.tr/ar/rss/default?cat=site"},
    {"id": "dw_arabic", "name": "DW Arabic", "url": "https://rss.dw.com/xml/rss-ar-all"},
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


def _fetch_single_feed(feed_cfg: dict) -> list[dict]:
    """Fetch one RSS feed with a hard timeout."""
    items: list[dict] = []
    try:
        with httpx.Client(timeout=10, follow_redirects=True) as client:
            resp = client.get(feed_cfg["url"], headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            parsed = feedparser.parse(resp.text)

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
        if items:
            logger.info("RSS [%s]: %d new items", feed_cfg["name"], len(items))
    except Exception:
        logger.warning("RSS [%s] failed (timeout or error)", feed_cfg["name"])
    return items


def fetch_rss_feeds() -> list[dict]:
    """Fetch all RSS feeds and return new (unseen) items."""
    all_items: list[dict] = []
    for feed_cfg in RSS_FEEDS:
        all_items.extend(_fetch_single_feed(feed_cfg))
    return all_items


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
