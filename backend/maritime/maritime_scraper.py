"""
Maritime data scraper for Strait of Hormuz port and vessel intelligence.

Pulls from public sources:
- MarineVesselTraffic port pages (static port metadata)
- GDELT maritime-filtered queries
- Public vessel databases
"""

import hashlib
import logging
import re
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HORMUZ_PORTS = [
    {"id": "bandar_abbas", "name": "Bandar Abbas", "country": "Iran",
     "lat": 27.19, "lng": 56.27, "url": "https://www.marinevesseltraffic.com/ships-in-port/BANDAR%20ABBAS/ir/type-Port"},
    {"id": "fujairah", "name": "Fujairah", "country": "UAE",
     "lat": 25.12, "lng": 56.33, "url": "https://www.marinevesseltraffic.com/ships-in-port/FUJAIRAH/ae/type-Port"},
    {"id": "khor_fakkan", "name": "Khor Fakkan", "country": "UAE",
     "lat": 25.34, "lng": 56.35, "url": "https://www.marinevesseltraffic.com/ships-in-port/KHOR%20FAKKAN/ae/type-Port"},
    {"id": "mina_saqr", "name": "Mina Saqr", "country": "UAE",
     "lat": 25.69, "lng": 56.02, "url": "https://www.marinevesseltraffic.com/ships-in-port/MINA%20SAQR/ae/type-Port"},
    {"id": "dubai", "name": "Dubai", "country": "UAE",
     "lat": 25.27, "lng": 55.28, "url": "https://www.marinevesseltraffic.com/ships-in-port/DUBAI/ae/type-Port"},
    {"id": "jask", "name": "Jask", "country": "Iran",
     "lat": 25.64, "lng": 57.77, "url": "https://www.marinevesseltraffic.com/ships-in-port/JASK/ir/type-Port"},
    {"id": "sohar", "name": "Sohar", "country": "Oman",
     "lat": 24.36, "lng": 56.73, "url": "https://www.marinevesseltraffic.com/ships-in-port/SOHAR/om/type-Port"},
    {"id": "ras_laffan", "name": "Ras Laffan", "country": "Qatar",
     "lat": 25.92, "lng": 51.55, "url": "https://www.marinevesseltraffic.com/ships-in-port/RAS%20LAFFAN/qa/type-Port"},
    {"id": "al_basrah", "name": "Al Basrah", "country": "Iraq",
     "lat": 30.52, "lng": 47.78, "url": "https://www.marinevesseltraffic.com/ships-in-port/AL%20BASRAH/iq/type-Port"},
    {"id": "dammam", "name": "Dammam", "country": "Saudi Arabia",
     "lat": 26.45, "lng": 50.10, "url": "https://www.marinevesseltraffic.com/ships-in-port/DAMMAM/sa/type-Port"},
]

GDELT_MARITIME_URL = (
    "https://api.gdeltproject.org/api/v2/doc/doc"
    "?query=(hormuz OR tanker OR naval OR vessel OR maritime OR strait) sourcelang:arabic"
    "&mode=ArtList&maxrecords=30&format=json&timespan=1h"
)

_seen_articles: set[str] = set()
_MAX_SEEN = 5_000


def _article_id(url: str, title: str) -> str:
    raw = f"{url or title}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _is_new(article_id: str) -> bool:
    if article_id in _seen_articles:
        return False
    _seen_articles.add(article_id)
    if len(_seen_articles) > _MAX_SEEN:
        to_remove = list(_seen_articles)[:_MAX_SEEN // 2]
        for k in to_remove:
            _seen_articles.discard(k)
    return True


async def scrape_port_vessels(port: dict) -> list[dict]:
    """
    Scrape vessel list from a MarineVesselTraffic port page.
    Returns list of vessel dicts with name, type, flag, etc.
    """
    vessels = []
    try:
        async with httpx.AsyncClient(
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (compatible; research bot)"},
            follow_redirects=True,
        ) as client:
            resp = await client.get(port["url"])
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for vessel table rows — the site structures vessels in tables/lists
        for row in soup.select("tr.ship-row, tr[data-mmsi], .vessel-row"):
            name_el = row.select_one(".ship-name, .vessel-name, td:nth-child(1)")
            type_el = row.select_one(".ship-type, .vessel-type, td:nth-child(2)")
            flag_el = row.select_one(".ship-flag, .vessel-flag, td:nth-child(3)")

            name = name_el.get_text(strip=True) if name_el else ""
            vtype = type_el.get_text(strip=True) if type_el else ""
            flag = flag_el.get_text(strip=True) if flag_el else ""

            mmsi_str = row.get("data-mmsi", "")
            mmsi = int(mmsi_str) if mmsi_str.isdigit() else None

            if name:
                vessels.append({
                    "name": name,
                    "type": vtype,
                    "flag": flag,
                    "mmsi": mmsi,
                    "port": port["name"],
                    "port_country": port["country"],
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                })

        # Fallback: look for any vessel-related links
        if not vessels:
            for link in soup.find_all("a", href=re.compile(r"/vessel/")):
                name = link.get_text(strip=True)
                if name and len(name) > 2:
                    vessels.append({
                        "name": name,
                        "type": "",
                        "flag": "",
                        "mmsi": None,
                        "port": port["name"],
                        "port_country": port["country"],
                        "scraped_at": datetime.now(timezone.utc).isoformat(),
                    })

        logger.info("Port %s: scraped %d vessels", port["name"], len(vessels))

    except Exception:
        logger.exception("Failed to scrape port %s", port["name"])

    return vessels


async def scrape_all_ports() -> dict[str, list[dict]]:
    """Scrape vessel lists from all Hormuz-area ports."""
    results = {}
    for port in HORMUZ_PORTS:
        vessels = await scrape_port_vessels(port)
        results[port["id"]] = vessels
    return results


async def fetch_maritime_news() -> list[dict]:
    """Fetch maritime-specific Arabic news from GDELT."""
    items = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(GDELT_MARITIME_URL)
            resp.raise_for_status()
            data = resp.json()

        for article in data.get("articles", []):
            url = article.get("url", "")
            title = article.get("title", "")
            aid = _article_id(url, title)
            if not _is_new(aid):
                continue

            items.append({
                "id": aid,
                "source_id": "gdelt_maritime",
                "source_name": article.get("domain", "GDELT Maritime"),
                "headline_ar": title,
                "body_ar": "",
                "url": url,
                "origin": "gdelt_maritime",
                "seendate": article.get("seendate", ""),
                "language": article.get("language", ""),
                "source_country": article.get("sourcecountry", ""),
            })

        logger.info("GDELT maritime: %d new articles", len(items))

    except Exception:
        logger.exception("GDELT maritime fetch failed")

    return items


def get_port_metadata() -> list[dict]:
    """Return static port metadata for map display."""
    return [
        {
            "id": p["id"],
            "name": p["name"],
            "country": p["country"],
            "lat": p["lat"],
            "lng": p["lng"],
        }
        for p in HORMUZ_PORTS
    ]
