"""
Momentus AI — Per-Workspace Alert Rule Engine
─────────────────────────────────────────────
Workspace-scoped alert rules over the 11 GEO dimensions. Evaluates on
demand, fires events through multiple channels (in-app, Slack, email-stub,
generic webhook), respects per-rule cooldowns. Reads from existing GEO
tables; only writes to its own alert_rules + alert_events tables.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

from .database import execute, fetch_all, fetch_one, from_json, gen_id, get_db, to_json
from .automation import create_notification

logger = logging.getLogger("geo.alerts")


ALERT_SCHEMA = """
CREATE TABLE IF NOT EXISTS alert_rules (
    id             TEXT PRIMARY KEY,
    workspace_id   TEXT NOT NULL,
    name           TEXT NOT NULL,
    trigger_type   TEXT NOT NULL,
    params         TEXT DEFAULT '{}',
    channels       TEXT DEFAULT '[]',
    cooldown_minutes INTEGER DEFAULT 60,
    enabled        INTEGER DEFAULT 1,
    created_by     TEXT DEFAULT '',
    created_at     TEXT DEFAULT (datetime('now')),
    last_fired_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_ar_ws ON alert_rules(workspace_id);

CREATE TABLE IF NOT EXISTS alert_events (
    id             TEXT PRIMARY KEY,
    rule_id        TEXT NOT NULL,
    workspace_id   TEXT NOT NULL,
    fired_at       TEXT DEFAULT (datetime('now')),
    severity       TEXT DEFAULT 'info',
    title          TEXT NOT NULL,
    message        TEXT DEFAULT '',
    payload        TEXT DEFAULT '{}',
    delivered_to   TEXT DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_ae_ws ON alert_events(workspace_id);
CREATE INDEX IF NOT EXISTS idx_ae_rule ON alert_events(rule_id);
"""


TRIGGER_TYPES: Dict[str, Dict[str, Any]] = {
    "ownership_drop":       {"defaults": {"min_revenue_score": 50.0, "drop_delta": 0.10, "window_hours": 168}},
    "authority_drop":       {"defaults": {"drop_delta": 5.0, "window_days": 7}},
    "competitor_aio_entry": {"defaults": {"min_revenue_score": 50.0}},
    "we_dropped_from_aio":  {"defaults": {"min_revenue_score": 50.0}},
    "sentiment_drop":       {"defaults": {"window_hours": 168, "min_negative_ratio": 0.3}},
    "reddit_opportunity":   {"defaults": {"subreddits": [], "min_brands_mentioned": 2}},
    "prompt_lost":          {"defaults": {"min_revenue_score": 50.0}},
    "capability_movement":  {"defaults": {"axis": "reddit", "delta": 5.0}},
}


async def init() -> None:
    """Create alert_rules + alert_events tables. Idempotent."""
    db = await get_db()
    await db.executescript(ALERT_SCHEMA)
    await db.commit()


# ═══════════════════════════════════════════════════════════════
# RULE CRUD
# ═══════════════════════════════════════════════════════════════

def _row_to_rule(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not row:
        return {}
    r = dict(row)
    r["params"] = from_json(r.get("params") or "{}", {})
    r["channels"] = from_json(r.get("channels") or "[]", [])
    r["enabled"] = bool(r.get("enabled", 1))
    return r


async def create_rule(
    workspace_id: str, name: str, trigger_type: str,
    params: Optional[Dict[str, Any]] = None,
    channels: Optional[List[str]] = None,
    cooldown_minutes: int = 60, created_by: str = "",
) -> Dict[str, Any]:
    if trigger_type not in TRIGGER_TYPES:
        raise ValueError(f"unknown trigger_type: {trigger_type}")
    merged = dict(TRIGGER_TYPES[trigger_type]["defaults"])
    merged.update(params or {})
    rid = gen_id("ar-")
    await execute(
        "INSERT INTO alert_rules (id, workspace_id, name, trigger_type, params, channels, "
        "cooldown_minutes, enabled, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
        (rid, workspace_id, name, trigger_type, to_json(merged),
         to_json(channels or ["inapp"]), int(cooldown_minutes), created_by),
    )
    return _row_to_rule(await fetch_one("SELECT * FROM alert_rules WHERE id = ?", (rid,)))


async def list_rules(workspace_id: str) -> List[Dict[str, Any]]:
    rows = await fetch_all(
        "SELECT * FROM alert_rules WHERE workspace_id = ? ORDER BY created_at DESC",
        (workspace_id,),
    )
    return [_row_to_rule(r) for r in rows]


async def update_rule(rule_id: str, **fields: Any) -> Dict[str, Any]:
    allowed = {"name", "trigger_type", "params", "channels", "cooldown_minutes", "enabled"}
    sets, vals = [], []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k in ("params", "channels"):
            v = to_json(v)
        elif k == "enabled":
            v = 1 if v else 0
        sets.append(f"{k} = ?")
        vals.append(v)
    if sets:
        vals.append(rule_id)
        await execute(f"UPDATE alert_rules SET {', '.join(sets)} WHERE id = ?", tuple(vals))
    return _row_to_rule(await fetch_one("SELECT * FROM alert_rules WHERE id = ?", (rule_id,)))


async def delete_rule(rule_id: str) -> bool:
    return (await execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))) > 0


async def list_events(workspace_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    rows = await fetch_all(
        "SELECT * FROM alert_events WHERE workspace_id = ? ORDER BY fired_at DESC LIMIT ?",
        (workspace_id, int(limit)),
    )
    out = []
    for r in rows:
        d = dict(r)
        d["payload"] = from_json(d.get("payload") or "{}", {})
        d["delivered_to"] = from_json(d.get("delivered_to") or "[]", [])
        out.append(d)
    return out


# ═══════════════════════════════════════════════════════════════
# COOLDOWN + EVENT HELPERS
# ═══════════════════════════════════════════════════════════════

def _within_cooldown(last_fired_at: Optional[str], cooldown_minutes: int) -> bool:
    if not last_fired_at or cooldown_minutes <= 0:
        return False
    try:
        last = datetime.fromisoformat(last_fired_at.replace("Z", "+00:00"))
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return datetime.now(timezone.utc) - last < timedelta(minutes=cooldown_minutes)


def _mk_event(rule: Dict[str, Any], title: str, message: str = "",
              severity: str = "info", payload: Optional[Dict] = None) -> Dict[str, Any]:
    return {
        "rule_id": rule["id"], "workspace_id": rule["workspace_id"],
        "rule_name": rule.get("name", ""), "channels": rule.get("channels", []),
        "severity": severity, "title": title, "message": message,
        "payload": payload or {},
    }


# ═══════════════════════════════════════════════════════════════
# TRIGGER EVALUATORS — one per trigger_type. Defensive: never raise.
# ═══════════════════════════════════════════════════════════════

async def _eval_ownership_drop(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Joins prompt_ownership × prompts; flags when leader_score-our_score >= drop_delta×100 inside window_hours."""
    p, ws = rule["params"], rule["workspace_id"]
    try:
        rows = await fetch_all(
            "SELECT po.prompt_id, po.our_score, po.leader_domain, po.leader_score, "
            "       p.text, p.revenue_score FROM prompt_ownership po "
            "JOIN prompts p ON p.id = po.prompt_id AND p.workspace_id = po.workspace_id "
            "WHERE po.workspace_id = ? AND p.revenue_score >= ? "
            "  AND po.last_observed_at >= datetime('now', ?)",
            (ws, float(p.get("min_revenue_score", 50)),
             f"-{int(p.get('window_hours', 168))} hours"),
        )
    except Exception as e:
        logger.warning("ownership_drop query failed: %s", e); return []
    drop_pts = float(p.get("drop_delta", 0.10)) * 100.0
    events = []
    for r in rows:
        gap = float(r["leader_score"] or 0) - float(r["our_score"] or 0)
        if gap >= drop_pts:
            events.append(_mk_event(rule,
                title=f"Ownership drop: '{(r['text'] or '')[:80]}'",
                message=f"our={r['our_score']:.1f} vs {r['leader_domain']}={r['leader_score']:.1f} (gap {gap:.1f})",
                severity="warning",
                payload={"prompt_id": r["prompt_id"], "our_score": r["our_score"],
                         "leader_score": r["leader_score"], "leader_domain": r["leader_domain"]}))
    return events


async def _eval_authority_drop(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Reads authority_scores for is_us=1 inside window_days; compares first vs last per domain."""
    p, ws = rule["params"], rule["workspace_id"]
    window, drop = int(p.get("window_days", 7)), float(p.get("drop_delta", 5.0))
    try:
        rows = await fetch_all(
            "SELECT subject_domain, total_score, observed_at FROM authority_scores "
            "WHERE workspace_id = ? AND is_us = 1 AND observed_at >= datetime('now', ?) "
            "ORDER BY observed_at ASC",
            (ws, f"-{window} days"),
        )
    except Exception as e:
        logger.warning("authority_drop query failed: %s", e); return []
    by_domain: Dict[str, List[Dict]] = {}
    for r in rows:
        by_domain.setdefault(r["subject_domain"], []).append(r)
    events = []
    for dom, series in by_domain.items():
        if len(series) < 2:
            continue
        first, last = series[0], series[-1]
        delta = float(first["total_score"] or 0) - float(last["total_score"] or 0)
        if delta >= drop:
            events.append(_mk_event(rule,
                title=f"Authority drop on {dom}",
                message=f"total_score {first['total_score']:.1f} → {last['total_score']:.1f} (−{delta:.1f}) over {window}d",
                severity="critical" if delta >= 2 * drop else "warning",
                payload={"domain": dom, "from": first["total_score"], "to": last["total_score"],
                         "first_at": first["observed_at"], "last_at": last["observed_at"]}))
    return events


async def _eval_competitor_aio_entry(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """aio_tracking joined to prompts; recent rows where we are absent AND visible_brands not empty / delta indicates entry."""
    p, ws = rule["params"], rule["workspace_id"]
    try:
        rows = await fetch_all(
            "SELECT a.prompt_id, a.visible_brands, a.delta_vs_prev, a.observed_at, "
            "       p.text, p.revenue_score FROM aio_tracking a "
            "JOIN prompts p ON p.id = a.prompt_id AND p.workspace_id = a.workspace_id "
            "WHERE a.workspace_id = ? AND a.aio_present = 1 AND a.our_brand_in_aio = 0 "
            "  AND p.revenue_score >= ? AND a.observed_at >= datetime('now', '-24 hours')",
            (ws, float(p.get("min_revenue_score", 50))),
        )
    except Exception as e:
        logger.warning("competitor_aio_entry query failed: %s", e); return []
    events = []
    for r in rows:
        brands = from_json(r["visible_brands"] or "[]", [])
        delta = (r["delta_vs_prev"] or "").lower()
        if "enter" in delta or "new" in delta or brands:
            events.append(_mk_event(rule,
                title=f"Competitor entered AIO: '{(r['text'] or '')[:80]}'",
                message=f"Visible brands: {', '.join(str(b) for b in brands[:5])}",
                severity="warning",
                payload={"prompt_id": r["prompt_id"], "visible_brands": brands,
                         "delta": r["delta_vs_prev"], "observed_at": r["observed_at"]}))
    return events


async def _eval_we_dropped_from_aio(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """For each high-revenue prompt, compare 2 latest aio_tracking rows; fire on 1→0 transition of our_brand_in_aio."""
    p, ws = rule["params"], rule["workspace_id"]
    try:
        prompts = await fetch_all(
            "SELECT DISTINCT a.prompt_id, p.text FROM aio_tracking a "
            "JOIN prompts p ON p.id = a.prompt_id AND p.workspace_id = a.workspace_id "
            "WHERE a.workspace_id = ? AND p.revenue_score >= ? "
            "  AND a.observed_at >= datetime('now', '-48 hours')",
            (ws, float(p.get("min_revenue_score", 50))),
        )
    except Exception as e:
        logger.warning("we_dropped_from_aio query failed: %s", e); return []
    events = []
    for pr in prompts:
        try:
            pair = await fetch_all(
                "SELECT our_brand_in_aio, observed_at FROM aio_tracking "
                "WHERE workspace_id = ? AND prompt_id = ? ORDER BY observed_at DESC LIMIT 2",
                (ws, pr["prompt_id"]),
            )
        except Exception:
            continue
        if len(pair) == 2 and pair[0]["our_brand_in_aio"] == 0 and pair[1]["our_brand_in_aio"] == 1:
            events.append(_mk_event(rule,
                title=f"We dropped from AIO: '{(pr['text'] or '')[:80]}'",
                message=f"Lost Google AI Overview presence at {pair[0]['observed_at']}",
                severity="critical",
                payload={"prompt_id": pr["prompt_id"], "lost_at": pair[0]["observed_at"]}))
    return events


async def _eval_sentiment_drop(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Counts negative vs total mention_events (project_id == workspace_id) over window_hours; fires if ratio >= threshold."""
    p, ws = rule["params"], rule["workspace_id"]
    window, min_ratio = int(p.get("window_hours", 168)), float(p.get("min_negative_ratio", 0.3))
    try:
        row = await fetch_one(
            "SELECT SUM(CASE WHEN sentiment='negative' THEN 1 ELSE 0 END) AS neg, "
            "       COUNT(*) AS total FROM mention_events "
            "WHERE project_id = ? AND observed_at >= datetime('now', ?)",
            (ws, f"-{window} hours"),
        )
    except Exception as e:
        logger.warning("sentiment_drop query failed: %s", e); return []
    total = (row or {}).get("total") or 0
    neg = (row or {}).get("neg") or 0
    if total < 5:
        return []
    ratio = neg / total
    if ratio < min_ratio:
        return []
    return [_mk_event(rule,
        title=f"Sentiment alert: {ratio:.0%} negative",
        message=f"{neg}/{total} mentions negative over last {window}h (threshold {min_ratio:.0%})",
        severity="warning" if ratio < 0.5 else "critical",
        payload={"negative": neg, "total": total, "ratio": ratio, "window_hours": window})]


async def _eval_reddit_opportunity(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """reddit_intel rows from last 24h with is_opportunity_gap=1, optionally filtered to subreddits list, with >= min brands."""
    p, ws = rule["params"], rule["workspace_id"]
    subs = p.get("subreddits") or []
    min_brands = int(p.get("min_brands_mentioned", 2))
    try:
        if subs:
            placeholders = ",".join("?" for _ in subs)
            rows = await fetch_all(
                f"SELECT subreddit, thread_url, thread_title, brands_mentioned, observed_at "
                f"FROM reddit_intel WHERE workspace_id = ? AND is_opportunity_gap = 1 "
                f"  AND subreddit IN ({placeholders}) AND observed_at >= datetime('now', '-24 hours')",
                (ws, *subs),
            )
        else:
            rows = await fetch_all(
                "SELECT subreddit, thread_url, thread_title, brands_mentioned, observed_at "
                "FROM reddit_intel WHERE workspace_id = ? AND is_opportunity_gap = 1 "
                "  AND observed_at >= datetime('now', '-24 hours')",
                (ws,),
            )
    except Exception as e:
        logger.warning("reddit_opportunity query failed: %s", e); return []
    events = []
    for r in rows:
        brands = from_json(r["brands_mentioned"] or "[]", [])
        if len(brands) < min_brands:
            continue
        events.append(_mk_event(rule,
            title=f"Reddit opportunity in r/{r['subreddit']}",
            message=f"{r['thread_title'] or r['thread_url']} — {len(brands)} brands, we are absent",
            severity="info",
            payload={"thread_url": r["thread_url"], "subreddit": r["subreddit"],
                     "brands": brands, "observed_at": r["observed_at"]}))
    return events


async def _eval_prompt_lost(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """prompt_ownership rows where our_score < leader_score AND prompt.revenue_score >= threshold."""
    p, ws = rule["params"], rule["workspace_id"]
    try:
        rows = await fetch_all(
            "SELECT po.prompt_id, po.our_score, po.leader_score, po.leader_domain, "
            "       p.text, p.revenue_score FROM prompt_ownership po "
            "JOIN prompts p ON p.id = po.prompt_id AND p.workspace_id = po.workspace_id "
            "WHERE po.workspace_id = ? AND p.revenue_score >= ? "
            "  AND po.our_score < po.leader_score AND po.leader_score > 0",
            (ws, float(p.get("min_revenue_score", 50))),
        )
    except Exception as e:
        logger.warning("prompt_lost query failed: %s", e); return []
    events = []
    for r in rows:
        gap = float(r["leader_score"] or 0) - float(r["our_score"] or 0)
        if gap <= 0:
            continue
        events.append(_mk_event(rule,
            title=f"Prompt lost: '{(r['text'] or '')[:80]}'",
            message=f"our={r['our_score']:.1f} < {r['leader_domain']}={r['leader_score']:.1f}",
            severity="warning",
            payload={"prompt_id": r["prompt_id"], "our_score": r["our_score"],
                     "leader_score": r["leader_score"], "leader_domain": r["leader_domain"]}))
    return events


async def _eval_capability_movement(rule: Dict[str, Any]) -> List[Dict[str, Any]]:
    """capability_history rows in last 24h with delta_vs_prev >= delta, optionally filtered to a single axis."""
    p, ws = rule["params"], rule["workspace_id"]
    axis, delta = p.get("axis") or "", float(p.get("delta", 5.0))
    try:
        if axis:
            rows = await fetch_all(
                "SELECT competitor_domain, axis, score, delta_vs_prev, observed_at "
                "FROM capability_history WHERE workspace_id = ? AND axis = ? "
                "  AND delta_vs_prev >= ? AND observed_at >= datetime('now', '-24 hours')",
                (ws, axis, delta),
            )
        else:
            rows = await fetch_all(
                "SELECT competitor_domain, axis, score, delta_vs_prev, observed_at "
                "FROM capability_history WHERE workspace_id = ? AND delta_vs_prev >= ? "
                "  AND observed_at >= datetime('now', '-24 hours')",
                (ws, delta),
            )
    except Exception as e:
        logger.warning("capability_movement query failed: %s", e); return []
    return [_mk_event(rule,
        title=f"Competitor surge: {r['competitor_domain']} on {r['axis']}",
        message=f"+{r['delta_vs_prev']:.1f} pts (now {r['score']:.1f}) at {r['observed_at']}",
        severity="warning",
        payload={"domain": r["competitor_domain"], "axis": r["axis"],
                 "delta": r["delta_vs_prev"], "score": r["score"]}) for r in rows]


_EVALUATORS = {
    "ownership_drop": _eval_ownership_drop,
    "authority_drop": _eval_authority_drop,
    "competitor_aio_entry": _eval_competitor_aio_entry,
    "we_dropped_from_aio": _eval_we_dropped_from_aio,
    "sentiment_drop": _eval_sentiment_drop,
    "reddit_opportunity": _eval_reddit_opportunity,
    "prompt_lost": _eval_prompt_lost,
    "capability_movement": _eval_capability_movement,
}


# ═══════════════════════════════════════════════════════════════
# DELIVERY
# ═══════════════════════════════════════════════════════════════

async def deliver(event: Dict[str, Any]) -> List[str]:
    """Dispatch event to each channel. Returns list of successfully delivered channels.

    Channel formats:
      - "inapp"             → fan out to workspace admins/editors via create_notification
      - "slack:<webhook>"   → POST text payload to Slack incoming-webhook URL
      - "email:<addr>"      → STUB (no SMTP wired) — logs only, still appended to delivered list
      - "webhook:<url>"     → POST raw JSON event body
    """
    delivered: List[str] = []
    channels = event.get("channels") or []
    title, message = event.get("title", ""), event.get("message", "")
    payload, ws = event.get("payload", {}), event.get("workspace_id", "")
    severity = event.get("severity", "info")

    for ch in channels:
        try:
            if ch == "inapp":
                try:
                    members = await fetch_all(
                        "SELECT user_id FROM workspace_members "
                        "WHERE workspace_id = ? AND role IN ('admin','editor')",
                        (ws,),
                    )
                    for m in members:
                        await create_notification(m["user_id"], ws, severity, title, message, link="")
                    delivered.append("inapp")
                except Exception as e:
                    logger.warning("inapp delivery failed: %s", e)

            elif ch.startswith("slack:"):
                webhook = ch[len("slack:"):]
                body = {"text": f"*{title}*\n{message}\n```{to_json(payload)[:1500]}```"}
                try:
                    async with httpx.AsyncClient(timeout=15.0) as http:
                        resp = await http.post(webhook, json=body)
                    if 200 <= resp.status_code < 300:
                        delivered.append("slack")
                    else:
                        logger.warning("slack returned %s", resp.status_code)
                except Exception as e:
                    logger.warning("slack delivery failed: %s", e)

            elif ch.startswith("email:"):
                # STUB: no SMTP creds wired in this codebase. Logged and marked delivered.
                addr = ch[len("email:"):]
                logger.info("EMAIL STUB → %s | %s | %s", addr, title, message)
                delivered.append(f"email:{addr}")

            elif ch.startswith("webhook:"):
                url = ch[len("webhook:"):]
                body = {"rule_id": event.get("rule_id"), "workspace_id": ws,
                        "severity": severity, "title": title, "message": message,
                        "payload": payload}
                try:
                    async with httpx.AsyncClient(timeout=15.0) as http:
                        resp = await http.post(url, json=body)
                    if 200 <= resp.status_code < 300:
                        delivered.append("webhook")
                    else:
                        logger.warning("webhook returned %s", resp.status_code)
                except Exception as e:
                    logger.warning("webhook delivery failed: %s", e)
            else:
                logger.warning("unknown channel: %s", ch)
        except Exception as e:
            logger.warning("channel %s blew up: %s", ch, e)

    return delivered


# ═══════════════════════════════════════════════════════════════
# EVALUATION ORCHESTRATION
# ═══════════════════════════════════════════════════════════════

async def _persist_event(evt: Dict[str, Any], delivered: List[str]) -> str:
    eid = gen_id("ae-")
    await execute(
        "INSERT INTO alert_events (id, rule_id, workspace_id, severity, title, message, "
        "payload, delivered_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (eid, evt["rule_id"], evt["workspace_id"], evt.get("severity", "info"),
         evt.get("title", ""), evt.get("message", ""),
         to_json(evt.get("payload", {})), to_json(delivered)),
    )
    return eid


async def evaluate_workspace(workspace_id: str) -> Dict[str, Any]:
    """Run all enabled rules for one workspace. Returns {checked, fired, events}."""
    rules = await list_rules(workspace_id)
    checked = fired = 0
    out_events: List[Dict[str, Any]] = []

    for rule in rules:
        if not rule.get("enabled"):
            continue
        if _within_cooldown(rule.get("last_fired_at"), int(rule.get("cooldown_minutes") or 0)):
            continue
        evaluator = _EVALUATORS.get(rule["trigger_type"])
        if not evaluator:
            logger.warning("no evaluator for trigger_type=%s", rule["trigger_type"])
            continue
        checked += 1
        try:
            events = await evaluator(rule)
        except Exception as e:
            logger.warning("evaluator %s crashed: %s", rule["trigger_type"], e)
            events = []
        if not events:
            continue
        for evt in events:
            delivered = await deliver(evt)
            eid = await _persist_event(evt, delivered)
            evt["id"] = eid
            evt["delivered_to"] = delivered
            out_events.append(evt)
            fired += 1
        await execute(
            "UPDATE alert_rules SET last_fired_at = datetime('now') WHERE id = ?",
            (rule["id"],),
        )

    return {"workspace_id": workspace_id, "checked": checked,
            "fired": fired, "events": out_events}


async def evaluate_all() -> Dict[str, Any]:
    """Iterate every workspace and evaluate."""
    try:
        workspaces = await fetch_all("SELECT id FROM workspaces", ())
    except Exception as e:
        logger.warning("evaluate_all: cannot list workspaces: %s", e)
        return {"workspaces": 0, "checked": 0, "fired": 0, "results": []}
    results, total_checked, total_fired = [], 0, 0
    for w in workspaces:
        try:
            res = await evaluate_workspace(w["id"])
        except Exception as e:
            logger.warning("evaluate_workspace failed for %s: %s", w["id"], e)
            continue
        total_checked += res["checked"]
        total_fired += res["fired"]
        results.append(res)
    return {"workspaces": len(workspaces), "checked": total_checked,
            "fired": total_fired, "results": results}
