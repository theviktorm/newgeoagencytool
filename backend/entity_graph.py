"""Momentus AI — Entity Relationship Graph.

Explainable Authority Graph over the GEO Conquer data sources. No LLM
calls — pure structure derivation. Nodes: brand | topic | prompt | model
| url | subreddit | doctor. Edges: covers_topic | observed_in |
mentioned_in | cited_at | cited_in | published_on | hosted_in.
Multi-tenant: every query filters on workspace_id.
"""
from __future__ import annotations
import logging
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse
from .database import execute, fetch_all, fetch_one, from_json, gen_id, get_db, to_json

logger = logging.getLogger("geo.entity_graph")
NODE_TYPES = {"brand", "topic", "prompt", "model", "url", "subreddit", "doctor"}
EDGE_TYPES = {"covers_topic", "observed_in", "mentioned_in", "cited_at",
              "cited_in", "published_on", "hosted_in"}
KNOWN_MODELS = ("claude", "chatgpt", "gemini", "perplexity", "google_aio", "peec_aggregate")

SCHEMA = """
CREATE TABLE IF NOT EXISTS graph_nodes (
    id            TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL,
    node_type     TEXT NOT NULL,
    label         TEXT NOT NULL,
    meta          TEXT DEFAULT '{}',
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_gn_ws ON graph_nodes(workspace_id);
CREATE INDEX IF NOT EXISTS idx_gn_type ON graph_nodes(node_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gn_uniq ON graph_nodes(workspace_id, node_type, label);

CREATE TABLE IF NOT EXISTS graph_edges (
    id            TEXT PRIMARY KEY,
    workspace_id  TEXT NOT NULL,
    from_id       TEXT NOT NULL,
    to_id         TEXT NOT NULL,
    edge_type     TEXT NOT NULL,
    weight        REAL DEFAULT 1.0,
    last_seen_at  TEXT DEFAULT (datetime('now')),
    evidence      TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_ge_ws ON graph_edges(workspace_id);
CREATE INDEX IF NOT EXISTS idx_ge_from ON graph_edges(workspace_id, from_id);
CREATE INDEX IF NOT EXISTS idx_ge_to ON graph_edges(workspace_id, to_id);
CREATE INDEX IF NOT EXISTS idx_ge_type ON graph_edges(edge_type);
"""


async def init() -> None:
    """Create graph tables. Safe to call repeatedly."""
    db = await get_db()
    await db.executescript(SCHEMA)
    await db.commit()


def _norm(s: Any) -> str:
    return (str(s) if s is not None else "").strip()

def _domain(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""

def _brand_name(b: Any) -> str:
    if isinstance(b, dict):
        return _norm(b.get("name") or b.get("brand") or b.get("domain"))
    return _norm(b)

def _url_of(s: Any) -> str:
    if isinstance(s, dict):
        return _norm(s.get("url"))
    return _norm(s)


async def upsert_node(
    workspace_id: str, node_type: str, label: str, meta: Optional[Dict[str, Any]] = None,
) -> str:
    label = _norm(label)
    if not label or node_type not in NODE_TYPES:
        raise ValueError(f"invalid node: type={node_type} label={label!r}")
    row = await fetch_one(
        "SELECT id, meta FROM graph_nodes WHERE workspace_id=? AND node_type=? AND label=?",
        (workspace_id, node_type, label),
    )
    if row:
        if meta:
            merged = from_json(row.get("meta") or "{}", {}) or {}
            merged.update(meta)
            await execute(
                "UPDATE graph_nodes SET meta=?, updated_at=datetime('now') WHERE id=?",
                (to_json(merged), row["id"]),
            )
        return row["id"]
    nid = gen_id("gn-")
    await execute(
        "INSERT INTO graph_nodes (id, workspace_id, node_type, label, meta) VALUES (?,?,?,?,?)",
        (nid, workspace_id, node_type, label, to_json(meta or {})),
    )
    return nid


async def upsert_edge(
    workspace_id: str, from_id: str, to_id: str, edge_type: str,
    weight: float = 1.0, evidence: Optional[Dict[str, Any]] = None,
) -> str:
    if not from_id or not to_id or from_id == to_id:
        raise ValueError("invalid edge endpoints")
    row = await fetch_one(
        "SELECT id, weight FROM graph_edges "
        "WHERE workspace_id=? AND from_id=? AND to_id=? AND edge_type=?",
        (workspace_id, from_id, to_id, edge_type),
    )
    if row:
        new_w = (row["weight"] or 0.0) + float(weight)
        await execute(
            "UPDATE graph_edges SET weight=?, last_seen_at=datetime('now'), "
            "evidence=COALESCE(?, evidence) WHERE id=?",
            (new_w, to_json(evidence) if evidence else None, row["id"]),
        )
        return row["id"]
    eid = gen_id("ge-")
    await execute(
        "INSERT INTO graph_edges (id, workspace_id, from_id, to_id, edge_type, weight, evidence) "
        "VALUES (?,?,?,?,?,?,?)",
        (eid, workspace_id, from_id, to_id, edge_type, float(weight), to_json(evidence or {})),
    )
    return eid


async def _wipe(workspace_id: str) -> None:
    await execute("DELETE FROM graph_edges WHERE workspace_id=?", (workspace_id,))
    await execute("DELETE FROM graph_nodes WHERE workspace_id=?", (workspace_id,))


async def rebuild_from_workspace(workspace_id: str) -> Dict[str, Any]:
    """Wipe + rebuild the graph from existing GEO data sources."""
    await init()
    await _wipe(workspace_id)

    # caches keyed by lookup string -> node id
    prompt_node: Dict[str, str] = {}
    topic_node: Dict[str, str] = {}
    model_node: Dict[str, str] = {}
    brand_node: Dict[str, str] = {}
    url_node: Dict[str, str] = {}
    sub_node: Dict[str, str] = {}

    async def _model(m: str) -> str:
        m = (_norm(m) or "unknown").lower()
        if m not in model_node:
            model_node[m] = await upsert_node(workspace_id, "model", m)
        return model_node[m]

    async def _brand(name: str) -> Optional[str]:
        name = _norm(name)
        if not name:
            return None
        k = name.lower()
        if k not in brand_node:
            brand_node[k] = await upsert_node(workspace_id, "brand", name)
        return brand_node[k]

    async def _url(u: str, title: str = "") -> Optional[str]:
        u = _norm(u)
        if not u:
            return None
        if u not in url_node:
            meta = {"domain": _domain(u)}
            if title:
                meta["title"] = title
            url_node[u] = await upsert_node(workspace_id, "url", u, meta=meta)
        return url_node[u]

    async def _sub(name: str) -> Optional[str]:
        name = _norm(name).lstrip("/")
        if name.lower().startswith("r/"):
            name = name[2:]
        if not name:
            return None
        if name not in sub_node:
            sub_node[name] = await upsert_node(workspace_id, "subreddit", name)
        return sub_node[name]

    # Prompts + Topics
    prompts = await fetch_all(
        "SELECT id, text, cluster_id, buyer_stage FROM prompts WHERE workspace_id=?",
        (workspace_id,),
    )
    for p in prompts:
        pnid = await upsert_node(
            workspace_id, "prompt", p["text"],
            meta={"prompt_id": p["id"], "buyer_stage": p.get("buyer_stage", "")},
        )
        prompt_node[p["id"]] = pnid
        tl = _norm(p.get("cluster_id")) or "uncategorized"
        if tl not in topic_node:
            topic_node[tl] = await upsert_node(workspace_id, "topic", tl)
        await upsert_edge(workspace_id, pnid, topic_node[tl], "covers_topic", 1.0)

    # prompt_observations
    obs = await fetch_all(
        "SELECT id, prompt_id, model, brands_appeared, sources_cited "
        "FROM prompt_observations WHERE workspace_id=?",
        (workspace_id,),
    )
    for o in obs:
        pnid = prompt_node.get(o["prompt_id"])
        if not pnid:
            continue
        mnid = await _model(o.get("model") or "unknown")
        await upsert_edge(workspace_id, pnid, mnid, "observed_in", 1.0)

        for b in from_json(o.get("brands_appeared") or "[]", []) or []:
            bnid = await _brand(_brand_name(b))
            if not bnid:
                continue
            ev: Dict[str, Any] = {"observation_id": o["id"], "model": o.get("model", "")}
            if isinstance(b, dict):
                if isinstance(b.get("position"), (int, float)):
                    ev["position"] = int(b["position"])
                if b.get("snippet"):
                    ev["snippet"] = str(b["snippet"])[:240]
            await upsert_edge(workspace_id, bnid, pnid, "mentioned_in", 1.0, ev)

        for s in from_json(o.get("sources_cited") or "[]", []) or []:
            title = s.get("title", "") if isinstance(s, dict) else ""
            unid = await _url(_url_of(s), title)
            if unid:
                await upsert_edge(
                    workspace_id, unid, pnid, "cited_in", 1.0,
                    {"observation_id": o["id"], "model": o.get("model", "")},
                )

    # reddit_intel
    reddit = await fetch_all(
        "SELECT subreddit, thread_url, brands_mentioned FROM reddit_intel WHERE workspace_id=?",
        (workspace_id,),
    )
    for r in reddit:
        snid = await _sub(r.get("subreddit") or "")
        if not snid:
            continue
        tnid = await _url(r.get("thread_url") or "")
        if tnid:
            await upsert_edge(workspace_id, tnid, snid, "published_on", 1.0)
        for b in from_json(r.get("brands_mentioned") or "[]", []) or []:
            bnid = await _brand(_brand_name(b))
            if bnid:
                await upsert_edge(workspace_id, bnid, snid, "mentioned_in", 1.0)

    # aio_tracking
    aio = await fetch_all(
        "SELECT prompt_id, visible_brands, source_urls FROM aio_tracking WHERE workspace_id=?",
        (workspace_id,),
    )
    aio_mid = await _model("google_aio")
    for a in aio:
        pnid = prompt_node.get(a["prompt_id"])
        if not pnid:
            continue
        await upsert_edge(workspace_id, pnid, aio_mid, "observed_in", 1.0)
        for b in from_json(a.get("visible_brands") or "[]", []) or []:
            bnid = await _brand(_brand_name(b))
            if bnid:
                await upsert_edge(
                    workspace_id, bnid, pnid, "mentioned_in", 1.0, {"model": "google_aio"},
                )
        for u in from_json(a.get("source_urls") or "[]", []) or []:
            unid = await _url(_url_of(u))
            if unid:
                await upsert_edge(
                    workspace_id, unid, pnid, "cited_in", 1.0, {"model": "google_aio"},
                )

    # sources (URL universe)
    for s in await fetch_all(
        "SELECT url, title FROM sources WHERE project_id=?", (workspace_id,),
    ):
        await _url(s.get("url") or "", s.get("title", ""))

    # brand -> url co-occurrence (observation-level)
    co: Counter = Counter()
    for o in obs:
        brands = {_brand_name(b).lower() for b in (from_json(o.get("brands_appeared") or "[]", []) or [])}
        urls = {_url_of(s) for s in (from_json(o.get("sources_cited") or "[]", []) or [])}
        for bn in brands - {""}:
            for u in urls - {""}:
                co[(bn, u)] += 1
    for (bn, u), w in co.items():
        bnid, unid = brand_node.get(bn), url_node.get(u)
        if bnid and unid:
            await upsert_edge(workspace_id, bnid, unid, "cited_at", float(w))

    by_nodes = await fetch_all(
        "SELECT node_type, COUNT(*) c FROM graph_nodes WHERE workspace_id=? GROUP BY node_type",
        (workspace_id,),
    )
    by_edges = await fetch_all(
        "SELECT edge_type, COUNT(*) c FROM graph_edges WHERE workspace_id=? GROUP BY edge_type",
        (workspace_id,),
    )
    n_nodes = sum(r["c"] for r in by_nodes)
    n_edges = sum(r["c"] for r in by_edges)
    logger.info("graph rebuilt ws=%s nodes=%d edges=%d", workspace_id, n_nodes, n_edges)
    return {
        "nodes": n_nodes, "edges": n_edges,
        "by_type": {r["node_type"]: r["c"] for r in by_nodes},
        "by_edge": {r["edge_type"]: r["c"] for r in by_edges},
    }


async def explain_why_wins(
    workspace_id: str, prompt_id: str, competitor_brand: str,
) -> Dict[str, Any]:
    """Walk graph from competitor brand back to prompt and structure the why."""
    pnode = await fetch_one(
        "SELECT * FROM graph_nodes WHERE workspace_id=? AND node_type='prompt' "
        "AND json_extract(meta, '$.prompt_id')=?",
        (workspace_id, prompt_id),
    )
    bnode = await fetch_one(
        "SELECT * FROM graph_nodes WHERE workspace_id=? AND node_type='brand' AND label=?",
        (workspace_id, _norm(competitor_brand)),
    )
    if not pnode or not bnode:
        return {"ok": False, "reason": "missing_node",
                "prompt_found": bool(pnode), "brand_found": bool(bnode)}

    mention_edges = await fetch_all(
        "SELECT weight, evidence FROM graph_edges WHERE workspace_id=? "
        "AND from_id=? AND to_id=? AND edge_type='mentioned_in'",
        (workspace_id, bnode["id"], pnode["id"]),
    )
    models_cnt: Counter = Counter()
    positions: List[int] = []
    for e in mention_edges:
        ev = from_json(e.get("evidence") or "{}", {}) or {}
        if ev.get("model"):
            models_cnt[ev["model"]] += int(e.get("weight") or 1)
        if isinstance(ev.get("position"), (int, float)):
            positions.append(int(ev["position"]))

    brand_urls = await fetch_all(
        "SELECT to_id, weight FROM graph_edges WHERE workspace_id=? AND from_id=? "
        "AND edge_type='cited_at'",
        (workspace_id, bnode["id"]),
    )
    brand_url_w = {r["to_id"]: r["weight"] for r in brand_urls}
    prompt_urls = await fetch_all(
        "SELECT from_id FROM graph_edges WHERE workspace_id=? AND to_id=? AND edge_type='cited_in'",
        (workspace_id, pnode["id"]),
    )
    supporting = []
    for r in prompt_urls:
        uid = r["from_id"]
        if uid not in brand_url_w:
            continue
        n = await fetch_one("SELECT label, meta FROM graph_nodes WHERE id=?", (uid,))
        if n:
            supporting.append({
                "url": n["label"],
                "domain": (from_json(n.get("meta") or "{}", {}) or {}).get("domain", ""),
                "weight": brand_url_w[uid],
            })
    supporting.sort(key=lambda x: x["weight"], reverse=True)

    sub_rows = await fetch_all(
        "SELECT e.weight, n.label FROM graph_edges e JOIN graph_nodes n ON n.id=e.to_id "
        "WHERE e.workspace_id=? AND e.from_id=? AND e.edge_type='mentioned_in' "
        "AND n.node_type='subreddit'",
        (workspace_id, bnode["id"]),
    )
    subreddits = sorted(
        [{"subreddit": r["label"], "weight": r["weight"]} for r in sub_rows],
        key=lambda x: x["weight"], reverse=True,
    )[:20]

    return {
        "ok": True,
        "prompt": {"id": prompt_id, "text": pnode["label"]},
        "brand": competitor_brand,
        "models_citing": [{"model": m, "count": c} for m, c in models_cnt.most_common()],
        "avg_position": (sum(positions) / len(positions)) if positions else None,
        "mention_total_weight": sum(e["weight"] or 0 for e in mention_edges),
        "supporting_urls": supporting[:20],
        "supporting_subreddits": subreddits,
        "summary": (
            f"{competitor_brand} appears in {len(mention_edges)} observations of this prompt "
            f"across {len(models_cnt)} model(s); backed by {len(supporting)} URL(s) "
            f"and {len(subreddits)} subreddit(s)."
        ),
    }


async def graph_summary(workspace_id: str) -> Dict[str, Any]:
    """Top brands by total degree, top topics by edge density, isolated nodes."""
    top_brands = await fetch_all(
        "SELECT n.id, n.label, "
        " (SELECT COALESCE(SUM(weight),0) FROM graph_edges e "
        "  WHERE e.workspace_id=n.workspace_id AND (e.from_id=n.id OR e.to_id=n.id)) AS degree "
        "FROM graph_nodes n WHERE n.workspace_id=? AND n.node_type='brand' "
        "ORDER BY degree DESC LIMIT 20",
        (workspace_id,),
    )
    top_topics = await fetch_all(
        "SELECT n.id, n.label, "
        " (SELECT COUNT(*) FROM graph_edges e "
        "  WHERE e.workspace_id=n.workspace_id AND e.to_id=n.id AND e.edge_type='covers_topic') "
        "  AS prompt_count "
        "FROM graph_nodes n WHERE n.workspace_id=? AND n.node_type='topic' "
        "ORDER BY prompt_count DESC LIMIT 20",
        (workspace_id,),
    )
    isolated = await fetch_all(
        "SELECT n.id, n.node_type, n.label FROM graph_nodes n WHERE n.workspace_id=? "
        "AND NOT EXISTS (SELECT 1 FROM graph_edges e WHERE e.workspace_id=n.workspace_id "
        "  AND (e.from_id=n.id OR e.to_id=n.id)) LIMIT 50",
        (workspace_id,),
    )
    totals = await fetch_all(
        "SELECT node_type, COUNT(*) c FROM graph_nodes WHERE workspace_id=? GROUP BY node_type",
        (workspace_id,),
    )
    return {
        "totals": {r["node_type"]: r["c"] for r in totals},
        "top_brands": top_brands,
        "top_topics": top_topics,
        "isolated_nodes": isolated,
    }


async def neighbors(
    workspace_id: str, node_id: str,
    edge_types: Optional[Iterable[str]] = None, limit: int = 50,
) -> List[Dict[str, Any]]:
    """Adjacent nodes (both directions) optionally filtered by edge_type."""
    types = list(edge_types) if edge_types else []
    sql = (
        "SELECT e.id AS edge_id, e.edge_type, e.weight, e.evidence, e.last_seen_at, "
        " e.from_id, e.to_id, nf.node_type AS from_type, nf.label AS from_label, "
        " nt.node_type AS to_type, nt.label AS to_label "
        "FROM graph_edges e "
        "JOIN graph_nodes nf ON nf.id=e.from_id "
        "JOIN graph_nodes nt ON nt.id=e.to_id "
        "WHERE e.workspace_id=? AND (e.from_id=? OR e.to_id=?) "
    )
    params: List[Any] = [workspace_id, node_id, node_id]
    if types:
        sql += "AND e.edge_type IN (" + ",".join("?" * len(types)) + ") "
        params.extend(types)
    sql += "ORDER BY e.weight DESC LIMIT ?"
    params.append(int(limit))
    rows = await fetch_all(sql, tuple(params))
    out = []
    for r in rows:
        outgoing = r["from_id"] == node_id
        out.append({
            "edge_id": r["edge_id"],
            "edge_type": r["edge_type"],
            "weight": r["weight"],
            "direction": "out" if outgoing else "in",
            "neighbor_id": r["to_id"] if outgoing else r["from_id"],
            "neighbor_type": r["to_type"] if outgoing else r["from_type"],
            "neighbor_label": r["to_label"] if outgoing else r["from_label"],
            "evidence": from_json(r.get("evidence") or "{}", {}),
            "last_seen_at": r["last_seen_at"],
        })
    return out


# ═══════════════════════════════════════════════════════════════
# PHASE 3 — TOPIC RELABEL + GRAPH INSIGHT
# Replaces UUID topic labels with human-readable ones derived from the
# prompt texts grouped under that topic.
# ═══════════════════════════════════════════════════════════════

# Conservative bilingual stop-word set. EN + HU. Kept inline (no new deps).
_EN_STOP = {
    "the", "a", "an", "of", "to", "in", "for", "on", "and", "or", "is",
    "are", "be", "with", "by", "as", "at", "this", "that", "it", "from",
    "what", "how", "who", "where", "when", "why", "vs", "versus", "best",
    "top", "near", "me", "i", "we", "our", "you", "your", "do", "does",
    "did", "can", "should", "would", "could", "will", "have", "has",
    "had", "but", "if", "than", "then", "so", "too", "very", "just",
    "out", "into", "more", "less", "any", "some", "all", "no", "not",
    "about", "after", "before", "between", "during", "over", "under",
    "such", "much", "many", "vs.", "etc",
}
_HU_STOP = {
    "a", "az", "egy", "és", "vagy", "mi", "mit", "ki", "kik", "hol",
    "hogyan", "mikor", "miért", "milyen", "mely", "van", "vannak",
    "nincs", "is", "csak", "így", "úgy", "de", "ha", "mert", "ezek",
    "azok", "ez", "az", "ami", "amely", "akik", "amelyek", "egyik",
    "másik", "valamint", "vagyis", "tehát", "leg", "legjobb", "legjobban",
    "legjobbak", "ahol", "ahogy", "ahogyan", "miközben", "amíg", "amikor",
    "vmint", "kell", "lehet", "lesz", "volt", "voltak", "lett", "lettek",
    "magyar", "magyarországon", "budapesten",
}
_STOPWORDS = _EN_STOP | _HU_STOP


def _strip_accents(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _topic_tokens(text: str) -> List[str]:
    """Lowercase, accent-strip, drop stop-words & short tokens."""
    if not text:
        return []
    import re as _re
    s = _strip_accents(text).lower()
    # Allow apostrophes inside words; otherwise split on anything non-alphanumeric.
    s = _re.sub(r"[^a-z0-9]+", " ", s)
    out: List[str] = []
    for t in s.split():
        if not t or t in _STOPWORDS or len(t) < 3 or t.isdigit():
            continue
        out.append(t)
    return out


def _heuristic_topic_label(prompt_texts: List[str]) -> str:
    """Pick the 1-2 most frequent content tokens across prompt texts and
    Title-Case them. Tiny, deterministic, language-agnostic."""
    if not prompt_texts:
        return ""
    counter: Counter = Counter()
    for t in prompt_texts:
        counter.update(_topic_tokens(t))
    if not counter:
        return ""
    top = counter.most_common(3)
    # Drop any candidate that only appears once if we have alternatives.
    keep = [w for w, c in top if c >= 2] or [top[0][0]]
    keep = keep[:2]
    return " ".join(w.capitalize() for w in keep)


async def _claude_topic_label(prompt_texts: List[str]) -> Optional[str]:
    """Optional upgrade: ask Claude for a sharper 2-4 word label."""
    try:
        from .config import settings as _settings
        if not _settings.has_claude_api:
            return None
        from .claude_engine import ClaudeEngine
        engine = ClaudeEngine()
        sample = "\n- " + "\n- ".join(t[:160] for t in prompt_texts[:8])
        resp = await engine.call(
            messages=[{"role": "user", "content": (
                "Give a 2-4 word Title Case topic label for these search prompts. "
                "Return ONLY the label, no quotes, no punctuation.\n" + sample
            )}],
            system="You produce concise topic labels for clusters of search prompts.",
            max_tokens=24,
            temperature=0.1,
            operation="entity_graph_topic_label",
            use_cache=True,
        )
        label = (resp.get("content", "") or "").strip().splitlines()[0].strip()
        # Defensive cleanup
        label = label.strip(' "\'.:;,').strip()
        if 1 < len(label) <= 60:
            return label
        return None
    except Exception:
        return None


async def relabel_topics(
    workspace_id: str, use_claude_for_top: int = 0,
) -> Dict[str, Any]:
    """Replace UUID-ish topic node labels with human-readable ones.

    Heuristic: for each topic node we gather every prompt whose
    `cluster_id` matches the node's current label, tokenize the texts
    (lowercase + accent-strip + EN/HU stop-words), and use the top 1-2
    most frequent content tokens, Title Case.

    `use_claude_for_top` (default 0): if Claude is configured, upgrade the
    N topics with the most prompts.

    Never overwrites a manual label (`label_source = 'manual'`)."""
    await init()
    # Pull every topic node.
    topics = await fetch_all(
        "SELECT id, label, meta, COALESCE(label_source,'auto') AS label_source "
        "FROM graph_nodes WHERE workspace_id = ? AND node_type = 'topic'",
        (workspace_id,),
    )
    if not topics:
        return {"workspace_id": workspace_id, "relabeled": 0, "skipped_manual": 0,
                "claude_upgraded": 0, "details": []}

    # Pull all prompts once; group by cluster_id.
    prompts = await fetch_all(
        "SELECT id, text, cluster_id FROM prompts WHERE workspace_id = ?",
        (workspace_id,),
    )
    by_cluster: Dict[str, List[str]] = {}
    for p in prompts:
        cid = _norm(p.get("cluster_id")) or "uncategorized"
        by_cluster.setdefault(cid, []).append(p.get("text") or "")

    # Compute heuristic label for each topic; sort by prompt count desc so
    # the optional Claude upgrade hits the biggest topics first.
    plans: List[Dict[str, Any]] = []
    for t in topics:
        if (t.get("label_source") or "auto") == "manual":
            plans.append({"topic": t, "skip_reason": "manual"})
            continue
        cid = t.get("label") or ""
        texts = by_cluster.get(cid) or []
        heuristic = _heuristic_topic_label(texts)
        plans.append({"topic": t, "texts": texts, "heuristic": heuristic,
                      "count": len(texts)})

    plans.sort(key=lambda x: -(x.get("count") or 0))

    relabeled = 0
    skipped_manual = 0
    claude_upgraded = 0
    details: List[Dict[str, Any]] = []
    claude_budget = max(0, int(use_claude_for_top))

    for plan in plans:
        topic = plan["topic"]
        if plan.get("skip_reason") == "manual":
            skipped_manual += 1
            details.append({
                "id": topic["id"], "old_label": topic["label"],
                "new_label": topic["label"], "action": "skipped_manual",
                "confidence": "manual",
            })
            continue

        heuristic = plan.get("heuristic") or ""
        texts = plan.get("texts") or []
        new_label = heuristic
        confidence = "heuristic"

        if claude_budget > 0 and texts:
            claude_label = await _claude_topic_label(texts)
            if claude_label:
                new_label = claude_label
                confidence = "claude_analyzed"
                claude_upgraded += 1
            claude_budget -= 1

        if not new_label:
            # Skip — nothing to write. Better to leave the UUID than corrupt.
            details.append({
                "id": topic["id"], "old_label": topic["label"],
                "new_label": topic["label"], "action": "no_change",
                "confidence": confidence,
            })
            continue
        if new_label == topic["label"]:
            details.append({
                "id": topic["id"], "old_label": topic["label"],
                "new_label": new_label, "action": "unchanged",
                "confidence": confidence,
            })
            continue

        # Honor unique (workspace_id, node_type, label) — append disambiguator if
        # collision. Cheap pre-check.
        clash = await fetch_one(
            "SELECT id FROM graph_nodes WHERE workspace_id=? AND node_type='topic' "
            "AND label=? AND id != ?",
            (workspace_id, new_label, topic["id"]),
        )
        final_label = new_label
        if clash:
            suffix = (topic["id"] or "")[-4:]
            final_label = f"{new_label} ({suffix})"

        meta = from_json(topic.get("meta") or "{}", {}) or {}
        meta["original_label"] = topic.get("label") or meta.get("original_label", "")
        meta["label_confidence"] = confidence
        try:
            await execute(
                "UPDATE graph_nodes SET label=?, meta=?, updated_at=datetime('now'), "
                "label_source='auto' WHERE id=?",
                (final_label, to_json(meta), topic["id"]),
            )
            relabeled += 1
            details.append({
                "id": topic["id"], "old_label": topic["label"],
                "new_label": final_label, "action": "updated",
                "confidence": confidence,
            })
        except Exception as e:
            logger.warning("relabel topic %s failed: %s", topic["id"], e)
            details.append({
                "id": topic["id"], "old_label": topic["label"],
                "new_label": topic["label"], "action": "error",
                "error": str(e),
            })

    return {
        "workspace_id": workspace_id,
        "topics_total": len(topics),
        "relabeled": relabeled,
        "skipped_manual": skipped_manual,
        "claude_upgraded": claude_upgraded,
        "details": details,
    }


async def graph_insight(workspace_id: str) -> Dict[str, Any]:
    """Aggregate insight over the entity graph for this workspace.

    Returns:
      top_brands_by_degree, top_topics, isolated_opportunities,
      strongest_edges, citation_heavy_nodes, weakly_defended_topics,
      summary_text
    """
    await init()
    # Top brands by total degree (sum of weights on incident edges).
    top_brands = await fetch_all(
        "SELECT n.id, n.label AS name, "
        " (SELECT COALESCE(SUM(weight), 0) FROM graph_edges e "
        "  WHERE e.workspace_id=n.workspace_id AND (e.from_id=n.id OR e.to_id=n.id)) "
        "  AS degree "
        "FROM graph_nodes n WHERE n.workspace_id=? AND n.node_type='brand' "
        "ORDER BY degree DESC LIMIT 10",
        (workspace_id,),
    )
    # Top topics by (prompt_count, competitor_count).
    top_topics_raw = await fetch_all(
        "SELECT n.id, n.label, "
        " (SELECT COUNT(*) FROM graph_edges e "
        "  WHERE e.workspace_id=n.workspace_id AND e.to_id=n.id "
        "    AND e.edge_type='covers_topic') AS prompt_count "
        "FROM graph_nodes n WHERE n.workspace_id=? AND n.node_type='topic' "
        "ORDER BY prompt_count DESC LIMIT 25",
        (workspace_id,),
    )
    top_topics: List[Dict[str, Any]] = []
    weakly_defended_topics: List[Dict[str, Any]] = []
    isolated_opportunities: List[Dict[str, Any]] = []
    for t in top_topics_raw:
        topic_id = t["id"]
        prompt_count = int(t.get("prompt_count") or 0)
        # How many distinct brands mention prompts that cover this topic?
        row = await fetch_one(
            """SELECT COUNT(DISTINCT b.id) AS n
               FROM graph_edges e_pt
               JOIN graph_edges e_bp
                 ON e_bp.workspace_id = e_pt.workspace_id
                AND e_bp.to_id = e_pt.from_id
                AND e_bp.edge_type = 'mentioned_in'
               JOIN graph_nodes b ON b.id = e_bp.from_id AND b.node_type='brand'
               WHERE e_pt.workspace_id = ? AND e_pt.to_id = ?
                 AND e_pt.edge_type = 'covers_topic'""",
            (workspace_id, topic_id),
        )
        competitor_count = int((row or {}).get("n", 0) or 0)
        item = {
            "id": topic_id,
            "label": t["label"],
            "prompt_count": prompt_count,
            "competitor_count": competitor_count,
        }
        top_topics.append(item)
        if competitor_count == 0 and prompt_count > 0:
            isolated_opportunities.append({
                "topic_id": topic_id,
                "topic_label": t["label"],
                "prompt_count": prompt_count,
                "reason": "no_brand_mentioned",
            })
        elif competitor_count <= 1 and prompt_count >= 2:
            weakly_defended_topics.append({
                "topic_id": topic_id,
                "topic_label": t["label"],
                "prompt_count": prompt_count,
                "competitor_count": competitor_count,
                "reason": "fragmented_or_open",
            })

    # Strongest individual edges (weight).
    strongest_edge_rows = await fetch_all(
        "SELECT e.from_id, e.to_id, e.edge_type, e.weight, "
        " nf.node_type AS from_type, nf.label AS from_label, "
        " nt.node_type AS to_type, nt.label AS to_label "
        "FROM graph_edges e "
        "JOIN graph_nodes nf ON nf.id=e.from_id "
        "JOIN graph_nodes nt ON nt.id=e.to_id "
        "WHERE e.workspace_id=? ORDER BY e.weight DESC LIMIT 15",
        (workspace_id,),
    )
    strongest_edges = [
        {
            "edge_type": r["edge_type"],
            "weight": r["weight"],
            "from": {"type": r["from_type"], "label": r["from_label"]},
            "to": {"type": r["to_type"], "label": r["to_label"]},
        }
        for r in strongest_edge_rows
    ]

    # Citation-heavy URLs: top URLs by sum of cited_in / cited_at weight.
    citation_heavy = await fetch_all(
        "SELECT n.id, n.label AS url, n.meta, "
        " (SELECT COALESCE(SUM(weight), 0) FROM graph_edges e "
        "  WHERE e.workspace_id=n.workspace_id AND e.from_id=n.id "
        "    AND e.edge_type IN ('cited_in','cited_at')) AS citation_weight "
        "FROM graph_nodes n WHERE n.workspace_id=? AND n.node_type='url' "
        "ORDER BY citation_weight DESC LIMIT 15",
        (workspace_id,),
    )
    citation_heavy_nodes = [
        {
            "url": r["url"],
            "domain": (from_json(r.get("meta") or "{}", {}) or {}).get("domain", ""),
            "citation_weight": r["citation_weight"],
        }
        for r in citation_heavy
        if (r.get("citation_weight") or 0) > 0
    ]

    n_brands = len(top_brands)
    n_topics = len(top_topics)
    summary_text = (
        f"{n_topics} topic(s) tracked; "
        f"{len(isolated_opportunities)} with no clear brand owner, "
        f"{len(weakly_defended_topics)} weakly defended. "
        f"Top brand by degree: "
        f"{(top_brands[0]['name'] if top_brands else 'n/a')}. "
        f"{len(citation_heavy_nodes)} citation-heavy URL(s)."
    )

    return {
        "workspace_id": workspace_id,
        "top_brands_by_degree": [
            {"name": r["name"], "degree": r["degree"]} for r in top_brands
        ],
        "top_topics": top_topics[:15],
        "isolated_opportunities": isolated_opportunities[:15],
        "strongest_edges": strongest_edges,
        "citation_heavy_nodes": citation_heavy_nodes,
        "weakly_defended_topics": weakly_defended_topics[:15],
        "summary_text": summary_text,
    }


async def get_graph_json(workspace_id: str, top_n_nodes: int = 80) -> Dict[str, Any]:
    """D3-ready {nodes:[...], links:[...]}. Caps nodes for browser sanity."""
    top_n_nodes = max(10, min(int(top_n_nodes), 500))
    rows = await fetch_all(
        "SELECT n.id, n.node_type, n.label, n.meta, "
        " (SELECT COALESCE(SUM(weight),0) FROM graph_edges e "
        "  WHERE e.workspace_id=n.workspace_id AND (e.from_id=n.id OR e.to_id=n.id)) AS degree "
        "FROM graph_nodes n WHERE n.workspace_id=? ORDER BY degree DESC LIMIT ?",
        (workspace_id, top_n_nodes),
    )
    if not rows:
        return {"nodes": [], "links": []}
    keep = [r["id"] for r in rows]
    placeholders = ",".join("?" * len(keep))
    nodes = [
        {
            "id": r["id"], "type": r["node_type"], "label": r["label"],
            "degree": r["degree"], "meta": from_json(r.get("meta") or "{}", {}),
        }
        for r in rows
    ]
    edge_rows = await fetch_all(
        f"SELECT from_id, to_id, edge_type, weight FROM graph_edges "
        f"WHERE workspace_id=? AND from_id IN ({placeholders}) AND to_id IN ({placeholders})",
        tuple([workspace_id] + keep + keep),
    )
    links = [
        {"source": e["from_id"], "target": e["to_id"],
         "type": e["edge_type"], "weight": e["weight"]}
        for e in edge_rows
    ]
    return {"nodes": nodes, "links": links}
