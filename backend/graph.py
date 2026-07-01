"""
Actor knowledge graph.

Accumulates a graph of *actors* (people, tribes, militias, parties, government
bodies, religious and economic actors) and the *relations* between them
(clash / attack / arrest / ally / sanction / negotiate ...) as they are
mentioned across the public news events collected by the platform.

The graph grows over time: every time an actor or a relation is seen again its
weight is incremented and its "last seen" timestamp refreshed. Influence scores
are derived from mention volume, relation activity, severity and recency, so the
"biggest players" surface automatically as more reporting comes in.

Everything here is derived from public reporting only.
"""

import json
import logging
import math
import os
import sqlite3
import threading
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(__file__), "..", "data", "ein.db")
)

# Canonical actor types and the influence dimension(s) they map onto.
ACTOR_TYPES = {
    "person",
    "tribe",
    "militia",
    "party",
    "government",
    "security",       # army / police / intelligence body
    "religious",
    "company",
    "state",          # foreign / national state actor
    "organization",
    "other",
}

# Relation types and whether they are "hostile" (used for colouring / scoring).
RELATION_TYPES = {
    "clash": "hostile",
    "attack": "hostile",
    "arrest": "hostile",
    "sanction": "hostile",
    "protest": "hostile",
    "threat": "hostile",
    "ally": "cooperative",
    "support": "cooperative",
    "negotiate": "cooperative",
    "meet": "cooperative",
    "deal": "cooperative",
    "other": "neutral",
}

_local = threading.local()


def _get_db() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        _local.conn = conn
    return _local.conn


def _init_db():
    db = _get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS actors (
            key TEXT PRIMARY KEY,          -- normalized name
            name TEXT NOT NULL,            -- display name
            atype TEXT DEFAULT 'other',
            dimensions TEXT DEFAULT '[]',  -- JSON list: political/economic/military/religious
            aliases TEXT DEFAULT '[]',     -- JSON list of alternate spellings
            mentions INTEGER DEFAULT 0,
            hostile_edges INTEGER DEFAULT 0,
            first_seen TEXT,
            last_seen TEXT
        );

        CREATE TABLE IF NOT EXISTS relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src TEXT NOT NULL,             -- actor key
            dst TEXT NOT NULL,             -- actor key
            rtype TEXT DEFAULT 'other',
            weight INTEGER DEFAULT 1,
            severity TEXT DEFAULT 'low',
            snippet TEXT,
            event_id TEXT,
            first_seen TEXT,
            last_seen TEXT,
            UNIQUE(src, dst, rtype)
        );
        CREATE INDEX IF NOT EXISTS idx_relations_src ON relations(src);
        CREATE INDEX IF NOT EXISTS idx_relations_dst ON relations(dst);
        CREATE INDEX IF NOT EXISTS idx_relations_last ON relations(last_seen);

        -- Which events have already been folded into the graph (idempotency).
        CREATE TABLE IF NOT EXISTS graph_processed (
            event_id TEXT PRIMARY KEY,
            processed_at TEXT
        );
        """
    )
    db.commit()


_init_db()


def normalize_key(name: str) -> str:
    """Normalize an actor name into a stable key for de-duplication."""
    return " ".join((name or "").strip().lower().split())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------
# Idempotency helpers
# --------------------------------------------------------------------------

def is_processed(event_id: str) -> bool:
    db = _get_db()
    row = db.execute(
        "SELECT 1 FROM graph_processed WHERE event_id = ?", (event_id,)
    ).fetchone()
    return row is not None


def filter_unprocessed(events: list[dict]) -> list[dict]:
    """Return only the events that have not yet been folded into the graph."""
    ids = [e.get("id", "") for e in events if e.get("id")]
    if not ids:
        return []
    db = _get_db()
    processed: set[str] = set()
    # Chunk to stay well under SQLite's variable limit.
    for i in range(0, len(ids), 400):
        chunk = ids[i:i + 400]
        placeholders = ",".join("?" * len(chunk))
        rows = db.execute(
            f"SELECT event_id FROM graph_processed WHERE event_id IN ({placeholders})",
            tuple(chunk),
        ).fetchall()
        processed.update(r["event_id"] for r in rows)
    return [e for e in events if e.get("id") and e["id"] not in processed]


def mark_processed(event_id: str) -> None:
    db = _get_db()
    db.execute(
        "INSERT OR IGNORE INTO graph_processed (event_id, processed_at) VALUES (?, ?)",
        (event_id, _now()),
    )
    db.commit()


# --------------------------------------------------------------------------
# Upserts
# --------------------------------------------------------------------------

def upsert_actor(
    name: str,
    atype: str = "other",
    dimensions: list[str] | None = None,
    alias: str | None = None,
) -> str:
    """Insert or update an actor, returning its normalized key."""
    key = normalize_key(name)
    if not key:
        return ""
    if atype not in ACTOR_TYPES:
        atype = "other"
    dims = [d for d in (dimensions or []) if d in
            ("political", "economic", "military", "religious")]

    db = _get_db()
    now = _now()
    row = db.execute("SELECT * FROM actors WHERE key = ?", (key,)).fetchone()

    if row is None:
        db.execute(
            """INSERT INTO actors
               (key, name, atype, dimensions, aliases, mentions, first_seen, last_seen)
               VALUES (?,?,?,?,?,?,?,?)""",
            (key, name.strip(), atype, json.dumps(dims, ensure_ascii=False),
             json.dumps([alias] if alias else [], ensure_ascii=False),
             1, now, now),
        )
    else:
        # Merge dimensions and aliases; keep a concrete type if we learn one.
        cur_dims = set(json.loads(row["dimensions"] or "[]"))
        cur_dims.update(dims)
        cur_aliases = set(json.loads(row["aliases"] or "[]"))
        if alias:
            cur_aliases.add(alias)
        new_type = row["atype"]
        if (new_type in ("other", "organization")) and atype not in ("other", "organization"):
            new_type = atype
        db.execute(
            """UPDATE actors
               SET mentions = mentions + 1,
                   dimensions = ?,
                   aliases = ?,
                   atype = ?,
                   last_seen = ?
               WHERE key = ?""",
            (json.dumps(sorted(cur_dims), ensure_ascii=False),
             json.dumps(sorted(cur_aliases), ensure_ascii=False),
             new_type, now, key),
        )
    db.commit()
    return key


def add_relation(
    src_name: str,
    dst_name: str,
    rtype: str = "other",
    severity: str = "low",
    snippet: str = "",
    event_id: str = "",
) -> None:
    """Insert or strengthen a directed relation between two actors."""
    src = normalize_key(src_name)
    dst = normalize_key(dst_name)
    if not src or not dst or src == dst:
        return
    if rtype not in RELATION_TYPES:
        rtype = "other"

    db = _get_db()
    now = _now()
    row = db.execute(
        "SELECT id, weight FROM relations WHERE src=? AND dst=? AND rtype=?",
        (src, dst, rtype),
    ).fetchone()

    if row is None:
        db.execute(
            """INSERT INTO relations
               (src, dst, rtype, weight, severity, snippet, event_id, first_seen, last_seen)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (src, dst, rtype, 1, severity, (snippet or "")[:280], event_id, now, now),
        )
    else:
        db.execute(
            """UPDATE relations
               SET weight = weight + 1, severity = ?, snippet = ?, last_seen = ?
               WHERE id = ?""",
            (severity, (snippet or "")[:280], now, row["id"]),
        )

    if RELATION_TYPES.get(rtype) == "hostile":
        for k in (src, dst):
            db.execute(
                "UPDATE actors SET hostile_edges = hostile_edges + 1 WHERE key = ?",
                (k,),
            )
    db.commit()


def ingest_extraction(event_id: str, extraction: dict, severity: str = "low") -> int:
    """
    Fold a single extraction result (actors + relations for one event) into the
    graph. Returns the number of relations recorded.
    """
    actors = extraction.get("actors", []) or []
    relations = extraction.get("relations", []) or []

    for a in actors:
        name = (a.get("name") or "").strip()
        if not name:
            continue
        upsert_actor(
            name,
            atype=a.get("type", "other"),
            dimensions=a.get("dimensions", []),
        )

    rel_count = 0
    for r in relations:
        src = (r.get("source") or r.get("src") or "").strip()
        dst = (r.get("target") or r.get("dst") or "").strip()
        if not src or not dst:
            continue
        # Make sure both endpoints exist as nodes even if not listed above.
        upsert_actor(src)
        upsert_actor(dst)
        add_relation(
            src, dst,
            rtype=r.get("type", "other"),
            severity=r.get("severity", severity),
            snippet=r.get("snippet", ""),
            event_id=event_id,
        )
        rel_count += 1

    mark_processed(event_id)
    return rel_count


# --------------------------------------------------------------------------
# Scoring + queries
# --------------------------------------------------------------------------

def _recency_factor(last_seen: str, half_life_hours: float = 72.0) -> float:
    """Exponential decay so recently active actors rank higher."""
    try:
        ts = datetime.fromisoformat(last_seen)
    except (ValueError, TypeError):
        return 0.5
    age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600.0
    return math.exp(-age_h / half_life_hours)


def _influence(mentions: int, degree: int, hostile: int, recency: float) -> float:
    """
    Composite influence score. Mentions + connectivity (degree) drive the base;
    hostile involvement adds weight (active players in conflict), scaled by
    recency. Log-compressed so a few loud sources don't dominate.
    """
    base = math.log1p(mentions) * 2.0 + math.log1p(degree) * 3.0 + math.log1p(hostile)
    return round(base * (0.4 + 0.6 * recency), 3)


def get_network(hours: float = 168.0, min_weight: int = 1, limit: int = 120) -> dict:
    """
    Return the actor network as nodes + edges for visualization.
    Only actors involved in relations within the window are returned.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    db = _get_db()

    edges_rows = db.execute(
        """SELECT src, dst, rtype, weight, severity, snippet, last_seen
           FROM relations
           WHERE last_seen >= ? AND weight >= ?
           ORDER BY weight DESC, last_seen DESC
           LIMIT 400""",
        (cutoff, min_weight),
    ).fetchall()

    degree: Counter[str] = Counter()
    for e in edges_rows:
        degree[e["src"]] += e["weight"]
        degree[e["dst"]] += e["weight"]

    keys = set(degree.keys())
    if not keys:
        return {"nodes": [], "edges": [], "generated_at": _now()}

    placeholders = ",".join("?" * len(keys))
    actor_rows = db.execute(
        f"SELECT * FROM actors WHERE key IN ({placeholders})", tuple(keys)
    ).fetchall()

    nodes = []
    for a in actor_rows:
        rec = _recency_factor(a["last_seen"])
        score = _influence(a["mentions"], degree[a["key"]], a["hostile_edges"], rec)
        nodes.append({
            "id": a["key"],
            "label": a["name"],
            "type": a["atype"],
            "dimensions": json.loads(a["dimensions"] or "[]"),
            "mentions": a["mentions"],
            "degree": degree[a["key"]],
            "hostile_edges": a["hostile_edges"],
            "influence": score,
            "last_seen": a["last_seen"],
        })

    nodes.sort(key=lambda n: n["influence"], reverse=True)
    nodes = nodes[:limit]
    kept = {n["id"] for n in nodes}

    edges = []
    for e in edges_rows:
        if e["src"] in kept and e["dst"] in kept:
            edges.append({
                "source": e["src"],
                "target": e["dst"],
                "type": e["rtype"],
                "class": RELATION_TYPES.get(e["rtype"], "neutral"),
                "weight": e["weight"],
                "severity": e["severity"],
                "snippet": e["snippet"] or "",
            })

    return {"nodes": nodes, "edges": edges, "generated_at": _now()}


def get_top_actors(hours: float = 168.0, limit: int = 25, dimension: str | None = None) -> list[dict]:
    """Ranked list of the most influential actors, optionally by dimension."""
    net = get_network(hours=hours, limit=500)
    nodes = net["nodes"]
    if dimension:
        nodes = [n for n in nodes if dimension in n["dimensions"]]
    return nodes[:limit]


def get_graph_stats() -> dict:
    db = _get_db()
    actor_count = db.execute("SELECT COUNT(*) FROM actors").fetchone()[0]
    rel_count = db.execute("SELECT COUNT(*) FROM relations").fetchone()[0]
    by_type = db.execute(
        "SELECT atype, COUNT(*) c FROM actors GROUP BY atype ORDER BY c DESC"
    ).fetchall()
    return {
        "actors": actor_count,
        "relations": rel_count,
        "by_type": {r["atype"]: r["c"] for r in by_type},
    }


def describe_graph_context(hours: float = 168.0, top_n: int = 20) -> str:
    """
    Produce a compact text description of the current graph for feeding to the
    analyst LLM: top actors and the most significant recent relations.
    """
    net = get_network(hours=hours, limit=top_n)
    lines: list[str] = []

    if net["nodes"]:
        lines.append("TOP ACTORS (name [type; dimensions] influence, mentions):")
        for n in net["nodes"]:
            dims = ", ".join(n["dimensions"]) or "n/a"
            lines.append(
                f"- {n['label']} [{n['type']}; {dims}] "
                f"influence={n['influence']}, mentions={n['mentions']}, "
                f"connections={n['degree']}"
            )

    if net["edges"]:
        top_edges = sorted(net["edges"], key=lambda e: e["weight"], reverse=True)[:40]
        id_to_label = {n["id"]: n["label"] for n in net["nodes"]}
        lines.append("\nKEY RELATIONS (who did what to whom, x = times reported):")
        for e in top_edges:
            s = id_to_label.get(e["source"], e["source"])
            d = id_to_label.get(e["target"], e["target"])
            lines.append(f"- {s} --[{e['type']}]--> {d} (x{e['weight']}, {e['severity']})")

    return "\n".join(lines)
