"""
Momentus AI — Prompt Ownership Engine

Treats prompts (questions users ask AI) as the primary GEO entity, not URLs.
Each prompt carries: type, buyer-stage, revenue score, ownership rollup, and
a stream of observations from prompt_tracker.

This module owns:
  - prompt CRUD
  - LLM-based classification (type / buyer_stage / revenue_score)
  - rolling ownership computation from prompt_observations
  - "battlefield" summary used by the dashboard

Heavy I/O (running prompts against ChatGPT / Gemini / Perplexity / Google AIO
to record observations) lives in prompt_tracker.py.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .claude_engine import ClaudeEngine
from .database import (
    execute, execute_many, fetch_all, fetch_one, gen_id, to_json, from_json,
)

logger = logging.getLogger("geo.prompt_engine")

PROMPT_TYPES = ("informational", "comparative", "decision", "purchase")
BUYER_STAGES = (
    "awareness", "problem", "solution", "comparison",
    "trust", "objection", "decision",
)
HIGH_VALUE_STAGES = {"comparison", "trust", "objection", "decision"}

# Heuristic baseline used when Claude isn't reachable.
_KEYWORD_RULES = [
    # (matchers, type, stage, base_revenue)
    (("best", "top", "near me", "nearest"), "decision", "trust", 75),
    (("vs", "versus", "compare", "comparison", "or"), "comparative", "comparison", 65),
    (("review", "rating", "experience", "reddit"), "comparative", "trust", 55),
    (("price", "cost", "how much", "buy", "book"), "purchase", "decision", 85),
    (("how to choose", "which", "should i", "is it worth"), "decision", "decision", 70),
    (("what is", "symptoms", "causes", "definition"), "informational", "awareness", 15),
    (("recovery", "side effect", "risk", "complication"), "informational", "objection", 40),
    (("alternative", "without", "instead of"), "comparative", "solution", 50),
]


# ═══════════════════════════════════════════════════════════════
# CRUD
# ═══════════════════════════════════════════════════════════════

async def upsert_prompt(
    workspace_id: str,
    text: str,
    target_brand: str = "",
    target_url: str = "",
    cluster_id: str = "",
    classify: bool = True,
    source: str = "manual",
    confidence: str = "estimated",
) -> Dict[str, Any]:
    """Create or update a prompt; returns the persisted row.

    `classify=True` runs the LLM/heuristic classifier; pass False if you're
    bulk-importing and will classify later in batches.

    `source` / `confidence` record provenance (e.g. source='peec_import',
    confidence='imported'). They default to manual/estimated so existing callers
    keep working unchanged. On update of an existing prompt they only overwrite
    when a non-empty value is passed.
    """
    text = (text or "").strip()
    if not text:
        raise ValueError("prompt text required")

    existing = await fetch_one(
        "SELECT * FROM prompts WHERE workspace_id = ? AND text = ?",
        (workspace_id, text),
    )
    if existing:
        if target_brand or target_url or cluster_id or source or confidence:
            await execute(
                "UPDATE prompts SET target_brand = COALESCE(NULLIF(?,''), target_brand), "
                "target_url = COALESCE(NULLIF(?,''), target_url), "
                "cluster_id = COALESCE(NULLIF(?,''), cluster_id), "
                "source = COALESCE(NULLIF(?,''), source), "
                "confidence = COALESCE(NULLIF(?,''), confidence), "
                "updated_at = datetime('now') WHERE id = ?",
                (target_brand, target_url, cluster_id, source, confidence, existing["id"]),
            )
        return await fetch_one("SELECT * FROM prompts WHERE id = ?", (existing["id"],))

    pid = gen_id("p-")
    classification = classify_prompt_heuristic(text)
    await execute(
        "INSERT INTO prompts (id, workspace_id, text, prompt_type, buyer_stage, "
        "revenue_score, target_brand, target_url, cluster_id, classifier_meta, "
        "source, confidence) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            pid, workspace_id, text,
            classification["prompt_type"],
            classification["buyer_stage"],
            classification["revenue_score"],
            target_brand, target_url, cluster_id,
            to_json({"source": "heuristic", **classification}),
            source or "manual", confidence or "estimated",
        ),
    )
    if classify:
        # Best-effort LLM upgrade — never block the caller
        try:
            await reclassify_prompt(pid)
        except Exception as e:
            logger.debug("reclassify_prompt failed for %s: %s", pid, e)
    return await fetch_one("SELECT * FROM prompts WHERE id = ?", (pid,))


async def bulk_upsert_prompts(
    workspace_id: str,
    texts: List[str],
    target_brand: str = "",
    classify: bool = False,
) -> Dict[str, Any]:
    """Fast path for adding 100s at once. Skips per-prompt LLM calls; the
    caller is expected to invoke `reclassify_workspace` once afterwards."""
    created = 0
    for t in texts:
        await upsert_prompt(workspace_id, t, target_brand=target_brand, classify=False)
        created += 1
    return {"upserted": created}


async def list_prompts(
    workspace_id: str,
    stage: str = "",
    min_revenue: float = 0,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM prompts WHERE workspace_id = ?"
    args: List[Any] = [workspace_id]
    if stage:
        sql += " AND buyer_stage = ?"
        args.append(stage)
    if min_revenue:
        sql += " AND revenue_score >= ?"
        args.append(min_revenue)
    sql += " ORDER BY revenue_score DESC, updated_at DESC LIMIT ?"
    args.append(limit)
    return await fetch_all(sql, tuple(args))


# ═══════════════════════════════════════════════════════════════
# CLASSIFICATION
# ═══════════════════════════════════════════════════════════════

def classify_prompt_heuristic(text: str) -> Dict[str, Any]:
    """Cheap, offline classifier. Used as fallback + warm-start before LLM."""
    t = (text or "").lower()
    best = {"prompt_type": "informational", "buyer_stage": "awareness", "revenue_score": 25.0}
    for matchers, ptype, stage, rev in _KEYWORD_RULES:
        if any(m in t for m in matchers):
            if rev > best["revenue_score"]:
                best = {"prompt_type": ptype, "buyer_stage": stage, "revenue_score": float(rev)}
    # local + branded + commercial intent boosters
    if any(loc in t for loc in (" budapest", " hungary", "near me", " london", " nyc")):
        best["revenue_score"] = min(100.0, best["revenue_score"] + 10)
    if "private" in t or "luxury" in t or "premium" in t:
        best["revenue_score"] = min(100.0, best["revenue_score"] + 5)
    return best


async def reclassify_prompt(prompt_id: str) -> Dict[str, Any]:
    """Use Claude to classify prompt_type + buyer_stage + revenue_score + rationale."""
    row = await fetch_one("SELECT * FROM prompts WHERE id = ?", (prompt_id,))
    if not row:
        return {}
    engine = ClaudeEngine()
    sys_prompt = (
        "You score search prompts for a Generative Engine Optimization (GEO) tool. "
        "For every prompt, classify:\n"
        f"  prompt_type: one of {list(PROMPT_TYPES)}\n"
        f"  buyer_stage: one of {list(BUYER_STAGES)}\n"
        "  revenue_score: 0-100, where 100 = direct purchase intent worth thousands "
        "of dollars per conversion (e.g. 'best private surgeon Budapest'), 50 = "
        "active comparison, 15 = pure educational.\n\n"
        "Return STRICT JSON only: "
        '{"prompt_type":"...","buyer_stage":"...","revenue_score":0,"rationale":"one sentence"}'
    )
    try:
        resp = await engine.call(
            messages=[{"role": "user", "content": row["text"]}],
            system=sys_prompt,
            max_tokens=200,
            temperature=0.0,
            project_id=row["workspace_id"],
            operation="prompt_classify",
            reference_id=prompt_id,
        )
    except Exception as e:
        logger.warning("Claude classify failed: %s", e)
        return {}
    parsed = _safe_json(resp.get("content", ""))
    if not parsed:
        return {}
    ptype = parsed.get("prompt_type") if parsed.get("prompt_type") in PROMPT_TYPES else row["prompt_type"]
    stage = parsed.get("buyer_stage") if parsed.get("buyer_stage") in BUYER_STAGES else row["buyer_stage"]
    rev = float(parsed.get("revenue_score") or row["revenue_score"] or 0)
    rev = max(0.0, min(100.0, rev))
    await execute(
        "UPDATE prompts SET prompt_type = ?, buyer_stage = ?, revenue_score = ?, "
        "classifier_meta = ?, updated_at = datetime('now') WHERE id = ?",
        (ptype, stage, rev, to_json({"source": "claude", **parsed}), prompt_id),
    )
    return parsed


async def reclassify_workspace(workspace_id: str, max_n: int = 200) -> Dict[str, Any]:
    rows = await fetch_all(
        "SELECT id FROM prompts WHERE workspace_id = ? "
        "ORDER BY updated_at ASC LIMIT ?",
        (workspace_id, max_n),
    )
    n = 0
    for r in rows:
        try:
            await reclassify_prompt(r["id"])
            n += 1
        except Exception:
            continue
    return {"reclassified": n}


# ═══════════════════════════════════════════════════════════════
# OWNERSHIP ROLLUP
# ═══════════════════════════════════════════════════════════════

async def recompute_ownership(workspace_id: str, prompt_id: str) -> Dict[str, Any]:
    """Aggregate the recent prompt_observations into an ownership snapshot.

    Scoring model (0..100):
      base 100 if our brand always cited first across all models, decays with
      missed models and lower position.
    """
    obs = await fetch_all(
        "SELECT * FROM prompt_observations WHERE prompt_id = ? "
        "AND observed_at >= datetime('now', '-30 days') ORDER BY observed_at DESC",
        (prompt_id,),
    )
    if not obs:
        return {}
    # Latest observation per model
    latest_by_model: Dict[str, Dict[str, Any]] = {}
    for o in obs:
        m = o["model"]
        if m not in latest_by_model:
            latest_by_model[m] = o

    our_score = 0.0
    competitor_scores: Dict[str, float] = {}
    models_covered = len(latest_by_model)

    for m, o in latest_by_model.items():
        brands = from_json(o.get("brands_appeared") or "[]") or []
        if not isinstance(brands, list):
            continue
        # Our brand contribution
        if o.get("our_brand_present"):
            pos = max(1, int(o.get("our_brand_position") or 1))
            our_score += max(0.0, 100.0 / pos)  # 100 for #1, 50 for #2, ...
        # Competitor contribution — top 5 only
        for idx, b in enumerate(brands[:5]):
            name = (b.get("name") if isinstance(b, dict) else str(b)) or ""
            name = name.strip().lower()
            if not name:
                continue
            competitor_scores[name] = competitor_scores.get(name, 0.0) + max(
                0.0, 100.0 / (idx + 1)
            )

    # Normalize: divide by models_covered so per-model averages.
    if models_covered:
        our_score /= models_covered
        for k in list(competitor_scores.keys()):
            competitor_scores[k] /= models_covered

    leader_domain, leader_score = "", 0.0
    if competitor_scores:
        leader_domain, leader_score = max(competitor_scores.items(), key=lambda kv: kv[1])

    last_observed = max(o["observed_at"] for o in obs)
    await execute(
        "INSERT INTO prompt_ownership "
        "(workspace_id, prompt_id, our_score, leader_domain, leader_score, "
        " competitor_scores, models_covered, last_observed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(workspace_id, prompt_id) DO UPDATE SET "
        "our_score = excluded.our_score, leader_domain = excluded.leader_domain, "
        "leader_score = excluded.leader_score, competitor_scores = excluded.competitor_scores, "
        "models_covered = excluded.models_covered, last_observed_at = excluded.last_observed_at",
        (
            workspace_id, prompt_id, our_score,
            leader_domain, leader_score,
            to_json(competitor_scores), models_covered, last_observed,
        ),
    )
    # Auto-update prompt status if we now own it
    if our_score >= 70 and (not leader_score or our_score > leader_score):
        await execute("UPDATE prompts SET status = 'won' WHERE id = ?", (prompt_id,))
    return {
        "our_score": our_score,
        "leader_domain": leader_domain,
        "leader_score": leader_score,
        "competitor_scores": competitor_scores,
        "models_covered": models_covered,
    }


# ═══════════════════════════════════════════════════════════════
# BATTLEFIELD SUMMARY
# ═══════════════════════════════════════════════════════════════

async def battlefield_summary(workspace_id: str) -> Dict[str, Any]:
    """The dashboard hero: high-value prompt count, owned, lost, emerging."""
    prompts = await fetch_all(
        "SELECT * FROM prompts WHERE workspace_id = ?", (workspace_id,)
    )
    ownership = {
        r["prompt_id"]: r
        for r in await fetch_all(
            "SELECT * FROM prompt_ownership WHERE workspace_id = ?", (workspace_id,)
        )
    }
    total = len(prompts)
    high_value = [p for p in prompts if (p.get("revenue_score") or 0) >= 60]
    owned, lost, emerging = 0, 0, 0
    competitor_grip: Dict[str, float] = {}
    revenue_at_stake = 0.0

    for p in high_value:
        own = ownership.get(p["id"])
        if own and (own.get("our_score") or 0) >= 70:
            owned += 1
        elif own and (own.get("leader_score") or 0) > (own.get("our_score") or 0):
            lost += 1
            d = (own.get("leader_domain") or "").lower()
            if d:
                competitor_grip[d] = competitor_grip.get(d, 0.0) + 1
        else:
            emerging += 1
        revenue_at_stake += float(p.get("estimated_value_eur") or 0) or float(p.get("revenue_score") or 0) * 1000

    top_dominators = sorted(competitor_grip.items(), key=lambda kv: -kv[1])[:5]

    return {
        "workspace_id": workspace_id,
        "total_prompts": total,
        "high_value_prompts": len(high_value),
        "owned": owned,
        "lost": lost,
        "emerging": emerging,
        "estimated_revenue_at_stake_eur": revenue_at_stake,
        "top_dominators": [{"domain": d, "lost_prompts": int(n)} for d, n in top_dominators],
    }


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

def _safe_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    try:
        # Some models wrap JSON in ```json fences; strip them.
        t = text.strip()
        if t.startswith("```"):
            t = t.strip("`")
            if t.lower().startswith("json"):
                t = t[4:]
        # Find first { ... last }
        s, e = t.find("{"), t.rfind("}")
        if s == -1 or e == -1:
            return None
        return json.loads(t[s:e + 1])
    except Exception:
        return None
