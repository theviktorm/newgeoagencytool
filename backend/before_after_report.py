"""
Momentus AI — Before / After Report (Phase 4)

Aggregates deltas over a workspace (or a single action) so the agency can
show clients "what we did, what changed":

  workspace_report(ws, days=30)
      authority delta, ownership delta, AIO delta, capability movements,
      revenue at stake delta, actions completed in window, top 5 next
      recommendations.

  action_report(action_id)
      Per-action before/after — derived purely from the snapshots persisted
      on geo_actions by take_before_snapshot / take_after_snapshot.

  html_report(ws, days=30) → str
      Printable HTML mirroring comparative_report.py styling.

Pure stdlib + existing deps. Defensive: every read tolerates empty tables.
Every computed metric ships with a `confidence` label.
"""
from __future__ import annotations

import html
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .database import fetch_all, fetch_one, from_json

logger = logging.getLogger("geo.before_after_report")


# ─────────────────────────────────────────────────────────────────
# WORKSPACE-LEVEL DELTAS
# ─────────────────────────────────────────────────────────────────

async def _authority_delta(workspace_id: str, days: int) -> Dict[str, Any]:
    """First vs latest authority_scores row inside the window (is_us=1)."""
    try:
        rows = await fetch_all(
            "SELECT total_score, citation_score, prompt_ownership_score, "
            "schema_score, offsite_score, reddit_score, entity_score, "
            "local_score, observed_at FROM authority_scores "
            "WHERE workspace_id = ? AND is_us = 1 "
            "AND observed_at >= datetime('now', ? || ' days') "
            "ORDER BY observed_at ASC",
            (workspace_id, f"-{int(days)}"),
        )
    except Exception:
        rows = []
    if not rows:
        return {"first": None, "latest": None, "delta": 0.0, "confidence": "needs_review"}
    first = rows[0]
    latest = rows[-1]
    delta = float(latest.get("total_score") or 0) - float(first.get("total_score") or 0)
    return {
        "first": first,
        "latest": latest,
        "delta": round(delta, 2),
        "confidence": "verified" if len(rows) >= 2 else "estimated",
    }


async def _ownership_counts(workspace_id: str) -> Dict[str, int]:
    """Latest owned / lost split from prompt_ownership."""
    try:
        rows = await fetch_all(
            "SELECT our_score, leader_score FROM prompt_ownership "
            "WHERE workspace_id = ?",
            (workspace_id,),
        )
    except Exception:
        rows = []
    owned, lost = 0, 0
    for r in rows:
        our = float(r.get("our_score") or 0)
        leader = float(r.get("leader_score") or 0)
        if our >= 50 and our >= leader and our > 0:
            owned += 1
        elif leader > our and leader > 0:
            lost += 1
    return {"owned": owned, "lost": lost, "total": len(rows)}


async def _ownership_window_delta(workspace_id: str, days: int) -> Dict[str, Any]:
    """Compare ownership *position* across the window. Best-effort: derives a
    'before' bucket from the oldest prompt_observations in the window vs the
    current rolled-up state. Returns counts + confidence."""
    current = await _ownership_counts(workspace_id)
    try:
        before_rows = await fetch_all(
            "SELECT prompt_id, MIN(observed_at) AS first_obs, "
            "MAX(our_brand_present) AS ever_present "
            "FROM prompt_observations WHERE workspace_id = ? "
            "AND observed_at <= datetime('now', ? || ' days') "
            "GROUP BY prompt_id",
            (workspace_id, f"-{int(days)}"),
        )
    except Exception:
        before_rows = []
    owned_before = sum(1 for r in before_rows if int(r.get("ever_present") or 0) == 1)
    lost_before = max(0, len(before_rows) - owned_before)
    return {
        "before": {"owned": owned_before, "lost": lost_before, "total": len(before_rows)},
        "after": current,
        "owned_delta": current["owned"] - owned_before,
        "lost_delta": current["lost"] - lost_before,
        "confidence": "estimated",
    }


async def _aio_window_delta(workspace_id: str, days: int) -> Dict[str, Any]:
    try:
        before = await fetch_one(
            "SELECT COUNT(*) AS n, SUM(CASE WHEN our_brand_in_aio=1 THEN 1 ELSE 0 END) AS ours "
            "FROM aio_tracking WHERE workspace_id = ? "
            "AND observed_at <= datetime('now', ? || ' days')",
            (workspace_id, f"-{int(days)}"),
        )
        after = await fetch_one(
            "SELECT COUNT(*) AS n, SUM(CASE WHEN our_brand_in_aio=1 THEN 1 ELSE 0 END) AS ours "
            "FROM aio_tracking WHERE workspace_id = ?",
            (workspace_id,),
        )
    except Exception:
        before, after = None, None
    bn = int((before or {}).get("n") or 0)
    bo = int((before or {}).get("ours") or 0)
    an = int((after or {}).get("n") or 0)
    ao = int((after or {}).get("ours") or 0)
    return {
        "before": {"observations": bn, "our_brand_in_aio": bo},
        "after": {"observations": an, "our_brand_in_aio": ao},
        "delta_our_brand_in_aio": ao - bo,
        "delta_observations": an - bn,
        "confidence": "estimated",
    }


async def _capability_movements(workspace_id: str, days: int) -> List[Dict[str, Any]]:
    try:
        rows = await fetch_all(
            "SELECT competitor_domain, axis, score, delta_vs_prev, observed_at "
            "FROM capability_history WHERE workspace_id = ? "
            "AND observed_at >= datetime('now', ? || ' days') "
            "ORDER BY observed_at DESC LIMIT 200",
            (workspace_id, f"-{int(days)}"),
        )
    except Exception:
        rows = []
    movements: List[Dict[str, Any]] = []
    for r in rows:
        delta = float(r.get("delta_vs_prev") or 0)
        if abs(delta) < 0.1:
            continue
        movements.append({
            "competitor_domain": r.get("competitor_domain"),
            "axis": r.get("axis"),
            "score": float(r.get("score") or 0),
            "delta_vs_prev": delta,
            "direction": "up" if delta > 0 else "down",
            "observed_at": r.get("observed_at"),
        })
    movements.sort(key=lambda x: -abs(x["delta_vs_prev"]))
    return movements[:25]


async def _revenue_at_stake(workspace_id: str) -> float:
    """Current sum of estimated_value_eur (or revenue_score*1000 proxy) on
    prompts a competitor leads. Mirrors the canonical formula used elsewhere."""
    try:
        rows = await fetch_all(
            "SELECT p.estimated_value_eur, p.revenue_score, "
            "po.our_score, po.leader_score "
            "FROM prompts p LEFT JOIN prompt_ownership po "
            "ON po.prompt_id = p.id AND po.workspace_id = p.workspace_id "
            "WHERE p.workspace_id = ?",
            (workspace_id,),
        )
    except Exception:
        rows = []
    total = 0.0
    for r in rows:
        our = float(r.get("our_score") or 0)
        leader = float(r.get("leader_score") or 0)
        if leader <= our:
            continue
        val = float(r.get("estimated_value_eur") or 0)
        if val <= 0:
            val = float(r.get("revenue_score") or 0) * 1000.0
        total += val
    return round(total, 2)


async def _completed_actions(workspace_id: str, days: int) -> List[Dict[str, Any]]:
    try:
        return await fetch_all(
            "SELECT id, action_type, status, title, target_url, target_prompt_id, "
            "completed_at FROM geo_actions WHERE workspace_id = ? "
            "AND status IN ('completed', 'done') "
            "AND completed_at >= datetime('now', ? || ' days') "
            "ORDER BY completed_at DESC LIMIT 50",
            (workspace_id, f"-{int(days)}"),
        )
    except Exception:
        return []


async def _next_recommendations(workspace_id: str) -> List[Dict[str, Any]]:
    """Top 5 from revenue_priority — lazy import keeps the cycle clean."""
    try:
        from . import revenue_priority as _rp
        recs = await _rp.revenue_priority_recs(workspace_id, top_n=5)
        return recs or []
    except Exception as e:
        logger.warning("next_recommendations failed: %s", e)
        return []


async def workspace_report(workspace_id: str, days: int = 30) -> Dict[str, Any]:
    """Per-workspace before/after report over the last `days`."""
    days = max(1, int(days))
    authority = await _authority_delta(workspace_id, days)
    ownership = await _ownership_window_delta(workspace_id, days)
    aio = await _aio_window_delta(workspace_id, days)
    capability = await _capability_movements(workspace_id, days)
    revenue = await _revenue_at_stake(workspace_id)
    completed = await _completed_actions(workspace_id, days)
    next_recs = await _next_recommendations(workspace_id)

    return {
        "workspace_id": workspace_id,
        "window_days": days,
        "authority_delta": authority,
        "ownership_delta": ownership,
        "aio_delta": aio,
        "capability_movements": capability,
        "current_revenue_at_stake_eur": revenue,
        "completed_actions": completed,
        "next_recommendations": next_recs,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "confidence": "estimated",
    }


# ─────────────────────────────────────────────────────────────────
# PER-ACTION REPORT (from persisted snapshots)
# ─────────────────────────────────────────────────────────────────

def _snap_authority(snap: Dict[str, Any]) -> Dict[str, Any]:
    return (snap or {}).get("authority") or {}


def _snap_ownership(snap: Dict[str, Any]) -> Dict[str, Any]:
    return (snap or {}).get("prompt_ownership") or {}


def _snap_aio(snap: Dict[str, Any]) -> Dict[str, Any]:
    return (snap or {}).get("aio_tracking") or {}


def _compute_deltas(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    """Compute deltas between two snapshot dicts (defensive on missing keys)."""
    ba = _snap_authority(before)
    aa = _snap_authority(after)
    bo = _snap_ownership(before)
    ao = _snap_ownership(after)
    baio = _snap_aio(before)
    aaio = _snap_aio(after)

    authority_delta = round(
        float(aa.get("total_score") or 0) - float(ba.get("total_score") or 0), 2,
    )
    citation_delta = round(
        float(aa.get("citation_score") or 0) - float(ba.get("citation_score") or 0), 2,
    )
    ownership_delta = {
        "our_score": round(
            float(ao.get("our_score") or 0) - float(bo.get("our_score") or 0), 2,
        ),
        "leader_score": round(
            float(ao.get("leader_score") or 0) - float(bo.get("leader_score") or 0), 2,
        ),
        "leader_domain_changed": (
            (ao.get("leader_domain") or "") != (bo.get("leader_domain") or "")
        ),
    }
    aio_delta = {
        "count": int(aaio.get("count") or 0) - int(baio.get("count") or 0),
        "our_brand_in_aio": (
            int(aaio.get("our_brand_in_aio") or 0)
            - int(baio.get("our_brand_in_aio") or 0)
        ),
    }
    return {
        "authority_delta": authority_delta,
        "citation_delta": citation_delta,
        "ownership_delta": ownership_delta,
        "aio_delta": aio_delta,
    }


def _narrative(action: Dict[str, Any], deltas: Dict[str, Any]) -> str:
    """Plain-English summary of what changed."""
    parts: List[str] = []
    auth = deltas.get("authority_delta", 0.0)
    cit = deltas.get("citation_delta", 0.0)
    own = (deltas.get("ownership_delta") or {}).get("our_score", 0.0)
    aio_ours = (deltas.get("aio_delta") or {}).get("our_brand_in_aio", 0)
    if auth > 0:
        parts.append(f"GEO Authority climbed +{auth:.1f} pts")
    elif auth < 0:
        parts.append(f"GEO Authority dropped {auth:.1f} pts")
    if cit > 0:
        parts.append(f"citation share up +{cit:.1f}")
    elif cit < 0:
        parts.append(f"citation share down {cit:.1f}")
    if own > 0:
        parts.append(f"prompt ownership up +{own:.1f}")
    elif own < 0:
        parts.append(f"prompt ownership down {own:.1f}")
    if aio_ours > 0:
        parts.append(f"+{aio_ours} new AI-Overview appearances")
    elif aio_ours < 0:
        parts.append(f"{aio_ours} AI-Overview presence")
    if not parts:
        return "No measurable movement yet — let the re-track window close."
    return f"Action '{action.get('title','')}' — " + ", ".join(parts) + "."


async def action_report(action_id: str) -> Dict[str, Any]:
    """Per-action before/after using the snapshots stored on geo_actions."""
    action = await fetch_one(
        "SELECT * FROM geo_actions WHERE id = ?", (action_id,),
    )
    if not action:
        raise ValueError(f"unknown action {action_id}")
    before = from_json(action.get("before_snapshot") or "{}", {}) or {}
    after = from_json(action.get("after_snapshot") or "{}", {}) or {}
    deltas = _compute_deltas(before, after)

    # Competitor movement: pull capability_history rows since the before snapshot.
    competitor_movement: List[Dict[str, Any]] = []
    try:
        taken_at = before.get("taken_at") or action.get("created_at")
        if taken_at:
            rows = await fetch_all(
                "SELECT competitor_domain, axis, score, delta_vs_prev, observed_at "
                "FROM capability_history WHERE workspace_id = ? "
                "AND observed_at >= ? ORDER BY observed_at DESC LIMIT 25",
                (action.get("workspace_id") or "", taken_at),
            )
            for r in rows:
                d = float(r.get("delta_vs_prev") or 0)
                if abs(d) >= 0.1:
                    competitor_movement.append({
                        "competitor_domain": r.get("competitor_domain"),
                        "axis": r.get("axis"),
                        "score": float(r.get("score") or 0),
                        "delta_vs_prev": d,
                    })
    except Exception:
        competitor_movement = []

    confidence = "verified" if (before and after) else (
        "estimated" if (before or after) else "needs_review"
    )
    return {
        "action": {
            "id": action.get("id"),
            "workspace_id": action.get("workspace_id"),
            "action_type": action.get("action_type"),
            "status": action.get("status"),
            "workflow_stage": action.get("workflow_stage"),
            "title": action.get("title"),
            "target_url": action.get("target_url"),
            "target_prompt_id": action.get("target_prompt_id"),
            "published_url": action.get("published_url"),
            "re_track_scheduled_at": action.get("re_track_scheduled_at"),
        },
        "before_snapshot": before,
        "after_snapshot": after,
        **deltas,
        "competitor_movement": competitor_movement,
        "narrative": _narrative(action, deltas),
        "confidence": confidence,
    }


# ─────────────────────────────────────────────────────────────────
# HTML RENDER (mirrors comparative_report.py styling)
# ─────────────────────────────────────────────────────────────────

def _band_color(score: float) -> str:
    if score >= 85: return "#10b981"
    if score >= 70: return "#22c55e"
    if score >= 55: return "#eab308"
    if score >= 40: return "#f97316"
    return "#ef4444"


def _delta_color(delta: float) -> str:
    if delta > 0.5: return "#047857"
    if delta < -0.5: return "#b91c1c"
    return "#475569"


def _safe(s: Any) -> str:
    return html.escape(str(s if s is not None else ""))


async def html_report(workspace_id: str, days: int = 30) -> str:
    """Printable HTML version of workspace_report. Inline CSS only."""
    data = await workspace_report(workspace_id, days=days)
    ws = await fetch_one(
        "SELECT id, slug, name, brand_name, color_primary, color_accent "
        "FROM workspaces WHERE id = ?",
        (workspace_id,),
    ) or {}
    brand = ws.get("brand_name") or ws.get("name") or workspace_id
    primary = ws.get("color_primary") or "#2563EB"
    accent = ws.get("color_accent") or "#10B981"

    auth = data.get("authority_delta") or {}
    own = data.get("ownership_delta") or {}
    aio = data.get("aio_delta") or {}
    cap = data.get("capability_movements") or []
    completed = data.get("completed_actions") or []
    next_recs = data.get("next_recommendations") or []

    first_total = float((auth.get("first") or {}).get("total_score") or 0)
    latest_total = float((auth.get("latest") or {}).get("total_score") or 0)
    auth_delta = float(auth.get("delta") or 0)

    def _row(label: str, before: Any, after: Any, delta: Optional[float] = None) -> str:
        if delta is None:
            try:
                delta = float(after or 0) - float(before or 0)
            except Exception:
                delta = 0.0
        return (
            "<tr>"
            f"<td>{_safe(label)}</td>"
            f"<td>{_safe(before)}</td>"
            f"<td>{_safe(after)}</td>"
            f"<td style=\"color:{_delta_color(delta)};font-weight:600\">"
            f"{'+' if delta>=0 else ''}{delta:.1f}</td>"
            "</tr>"
        )

    cap_rows = "".join(
        f"<tr><td>{_safe(m['competitor_domain'])}</td>"
        f"<td>{_safe(m['axis'])}</td>"
        f"<td>{_safe(round(m['score'],1))}</td>"
        f"<td style=\"color:{_delta_color(m['delta_vs_prev'])};font-weight:600\">"
        f"{'+' if m['delta_vs_prev']>=0 else ''}{m['delta_vs_prev']:.1f}</td></tr>"
        for m in cap[:15]
    ) or "<tr><td colspan='4' class='muted'>No notable competitor movement this window.</td></tr>"

    completed_rows = "".join(
        f"<tr><td>{_safe(a.get('action_type'))}</td>"
        f"<td>{_safe(a.get('title'))}</td>"
        f"<td class='muted'>{_safe(a.get('completed_at'))}</td></tr>"
        for a in completed[:25]
    ) or "<tr><td colspan='3' class='muted'>No actions completed in this window.</td></tr>"

    rec_rows = "".join(
        f"<tr><td>{_safe(r.get('text'))}</td>"
        f"<td>{_safe(round(float(r.get('revenue_score') or 0),0))}</td>"
        f"<td>{_safe(r.get('leader_domain') or '—')}</td>"
        f"<td>{_safe(round(float(r.get('priority') or 0),1))}</td></tr>"
        for r in next_recs[:5]
    ) or "<tr><td colspan='4' class='muted'>No recommendations available yet.</td></tr>"

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    band = _band_color(latest_total)

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Before / After Report — {_safe(brand)}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
@page {{ margin: 18mm; size: A4; }}
* {{ box-sizing: border-box; }}
html, body {{ margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
               'Helvetica Neue', Arial, sans-serif;
  color: #0f172a; background: #ffffff; line-height: 1.5;
  -webkit-print-color-adjust: exact; print-color-adjust: exact;
}}
.report {{ max-width: 1100px; margin: 0 auto; padding: 24px 32px 48px; }}
.stripe {{ height: 8px; background: linear-gradient(90deg, {primary} 0%, {accent} 100%); }}
header.head {{ padding: 24px 0 12px; border-bottom: 1px solid #e2e8f0; margin-bottom: 24px; }}
header.head h1 {{ font-size: 28px; margin: 0 0 4px; letter-spacing: -.01em; }}
header.head .meta {{ color: #64748b; font-size: 13px; }}
.panel {{ margin: 28px 0; page-break-inside: avoid; }}
.panel h2 {{ font-size: 18px; margin: 0 0 12px; color: #0f172a;
             border-left: 3px solid {primary}; padding-left: 10px; }}
.muted {{ color: #64748b; font-size: 13px; }}
.score-card {{ display: flex; align-items: center; gap: 18px;
               background: #f8fafc; border: 1px solid #e2e8f0;
               border-radius: 10px; padding: 16px 20px; }}
.score-card .bigscore {{ font-size: 44px; font-weight: 700; color: {band}; }}
.score-card .lbl {{ font-size: 12px; color: #64748b; text-transform: uppercase;
                    letter-spacing: .05em; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px;
         background: #ffffff; }}
th, td {{ padding: 8px 10px; text-align: left;
          border-bottom: 1px solid #e2e8f0; }}
th {{ background: #f1f5f9; font-weight: 600; color: #334155;
      font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }}
footer.foot {{ margin-top: 40px; padding-top: 16px; border-top: 1px solid #e2e8f0;
               color: #94a3b8; font-size: 12px; display: flex;
               justify-content: space-between; }}
</style></head><body>
<div class="stripe"></div>
<div class="report">
  <header class="head">
    <h1>Before / After GEO Report — {_safe(brand)}</h1>
    <div class="meta">Window: last {days} days · generated {_safe(ts)}</div>
  </header>

  <section class="panel">
    <h2>GEO Authority — at a glance</h2>
    <div class="score-card">
      <div class="bigscore">{int(round(latest_total))}</div>
      <div>
        <div class="lbl">Current GEO Authority</div>
        <div style="font-size:13px;color:#334155">
          Started window at <b>{int(round(first_total))}</b> ·
          delta <span style="color:{_delta_color(auth_delta)};font-weight:600">
          {'+' if auth_delta>=0 else ''}{auth_delta:.1f}</span>
        </div>
      </div>
    </div>
  </section>

  <section class="panel">
    <h2>Movement summary</h2>
    <table><thead><tr>
      <th>Metric</th><th>Before</th><th>After</th><th>Delta</th>
    </tr></thead><tbody>
      {_row("Prompts owned",
            (own.get("before") or {}).get("owned"),
            (own.get("after") or {}).get("owned"),
            float(own.get("owned_delta") or 0))}
      {_row("Prompts lost",
            (own.get("before") or {}).get("lost"),
            (own.get("after") or {}).get("lost"),
            float(own.get("lost_delta") or 0))}
      {_row("AI Overview observations",
            (aio.get("before") or {}).get("observations"),
            (aio.get("after") or {}).get("observations"),
            float(aio.get("delta_observations") or 0))}
      {_row("Our brand in AI Overview",
            (aio.get("before") or {}).get("our_brand_in_aio"),
            (aio.get("after") or {}).get("our_brand_in_aio"),
            float(aio.get("delta_our_brand_in_aio") or 0))}
    </tbody></table>
  </section>

  <section class="panel">
    <h2>Competitor capability movements</h2>
    <table><thead><tr>
      <th>Competitor</th><th>Axis</th><th>Score</th><th>Delta</th>
    </tr></thead><tbody>{cap_rows}</tbody></table>
  </section>

  <section class="panel">
    <h2>Revenue at stake</h2>
    <p>€{_safe(format(int(round(data.get('current_revenue_at_stake_eur') or 0)), ','))}
       — sum of estimated EUR value across prompts a competitor still leads.</p>
  </section>

  <section class="panel">
    <h2>Actions completed in window</h2>
    <table><thead><tr>
      <th>Type</th><th>Title</th><th>Completed</th>
    </tr></thead><tbody>{completed_rows}</tbody></table>
  </section>

  <section class="panel">
    <h2>Top 5 next moves</h2>
    <table><thead><tr>
      <th>Prompt</th><th>Rev. score</th><th>Leader</th><th>Priority</th>
    </tr></thead><tbody>{rec_rows}</tbody></table>
  </section>

  <footer class="foot">
    <span>Generated by Momentus AI</span>
    <span>workspace: {_safe(ws.get('slug') or workspace_id)}</span>
  </footer>
</div></body></html>"""


__all__ = ["workspace_report", "action_report", "html_report"]
