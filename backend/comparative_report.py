"""
Momentus AI — Comparative Competitive Report

Generates a printable, self-contained HTML report ("Aesthetica vs Mabelle
vs You") that agency teams paste into client decks. Sales asset.

Customer prints to PDF via the browser's Save-as-PDF — no weasyprint,
reportlab, or external assets. Inline CSS only.

Public API:
  - generate_html(workspace_id, competitor_domains, include_prompts=True) -> str
  - generate_summary(workspace_id, competitor_domains)                    -> dict
"""
from __future__ import annotations

import html
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .database import fetch_all, fetch_one, from_json

logger = logging.getLogger("geo.comparative_report")

# 7 sub-score axes from authority_scores
AXES = [
    ("citation_score", "Citations"),
    ("prompt_ownership_score", "Prompt Ownership"),
    ("schema_score", "Schema"),
    ("offsite_score", "Offsite"),
    ("reddit_score", "Reddit"),
    ("entity_score", "Entity"),
    ("local_score", "Local"),
]

# Per-competitor strength chart palette (cycled)
COMPETITOR_COLORS = ["#ef4444", "#8b5cf6", "#0ea5e9", "#f97316", "#14b8a6", "#ec4899"]
US_COLOR = "#2563eb"

# Same band palette as badge.py
def _band_color(score: float) -> str:
    if score >= 85:
        return "#10b981"
    if score >= 70:
        return "#22c55e"
    if score >= 55:
        return "#eab308"
    if score >= 40:
        return "#f97316"
    return "#ef4444"


def _band_label(score: float) -> str:
    if score >= 85:
        return "Dominant"
    if score >= 70:
        return "Strong"
    if score >= 55:
        return "Competitive"
    if score >= 40:
        return "Vulnerable"
    return "At risk"


# ═══════════════════════════════════════════════════════════════
# DATA GATHERING
# ═══════════════════════════════════════════════════════════════

async def _latest_authority(workspace_id: str, domain: str, is_us: int) -> Optional[Dict[str, Any]]:
    """Latest authority_scores row for a given subject. Falls back if domain
    filter misses (only is_us=1 lookup) when subject_domain wasn't set."""
    row = await fetch_one(
        "SELECT total_score, citation_score, prompt_ownership_score, schema_score, "
        "offsite_score, reddit_score, entity_score, local_score, observed_at, subject_domain "
        "FROM authority_scores "
        "WHERE workspace_id = ? AND subject_domain = ? "
        "ORDER BY observed_at DESC LIMIT 1",
        (workspace_id, domain),
    )
    if row:
        return row
    if is_us == 1:
        # fallback: latest is_us row regardless of subject_domain
        return await fetch_one(
            "SELECT total_score, citation_score, prompt_ownership_score, schema_score, "
            "offsite_score, reddit_score, entity_score, local_score, observed_at, subject_domain "
            "FROM authority_scores WHERE workspace_id = ? AND is_us = 1 "
            "ORDER BY observed_at DESC LIMIT 1",
            (workspace_id,),
        )
    return None


async def _competitor_capabilities(workspace_id: str, domain: str) -> Optional[Dict[str, Any]]:
    return await fetch_one(
        "SELECT overall_strength, schema_score, reddit_score, youtube_score, "
        "faq_depth_score, decision_support_score, review_score, "
        "entity_consistency_score, pr_score, local_authority_score "
        "FROM competitor_capabilities WHERE workspace_id = ? AND competitor_domain = ?",
        (workspace_id, domain),
    )


async def _our_capabilities(workspace_id: str) -> Dict[str, float]:
    """Map our latest authority sub-scores onto the same 9-axis space used by
    competitor_capabilities so we can compare like-for-like."""
    our = await _latest_authority(workspace_id, "", is_us=1)
    if not our:
        return {}
    cit = float(our.get("citation_score") or 0)
    pr_own = float(our.get("prompt_ownership_score") or 0)
    schema = float(our.get("schema_score") or 0)
    offsite = float(our.get("offsite_score") or 0)
    reddit = float(our.get("reddit_score") or 0)
    entity = float(our.get("entity_score") or 0)
    local = float(our.get("local_score") or 0)
    return {
        "schema_score": schema,
        "reddit_score": reddit,
        "youtube_score": offsite,                # closest proxy
        "faq_depth_score": pr_own,               # proxy: depth of prompt coverage
        "decision_support_score": cit,           # proxy: cited as decision source
        "review_score": local,                   # proxy: local reviews
        "entity_consistency_score": entity,
        "pr_score": offsite,
        "local_authority_score": local,
    }


async def _losing_prompts(workspace_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    """High-revenue prompts where competitor leads."""
    return await fetch_all(
        "SELECT p.id, p.text, p.revenue_score, p.estimated_value_eur, "
        "po.our_score, po.leader_domain, po.leader_score "
        "FROM prompt_ownership po "
        "JOIN prompts p ON p.id = po.prompt_id AND p.workspace_id = po.workspace_id "
        "WHERE po.workspace_id = ? AND p.revenue_score >= 60 "
        "AND po.our_score < po.leader_score "
        "ORDER BY p.revenue_score DESC, (po.leader_score - po.our_score) DESC LIMIT ?",
        (workspace_id, limit),
    )


# ═══════════════════════════════════════════════════════════════
# SVG HELPERS — pure stdlib, no deps
# ═══════════════════════════════════════════════════════════════

def _svg_gauge(score: float, label: str, size: int = 140) -> str:
    """Half-circle gauge gauge for a 0..100 score, color-banded."""
    color = _band_color(score)
    cx = size / 2
    cy = size * 0.72
    r = size * 0.40
    # arc from 180deg (left) to 0deg (right), 100% sweep at score=100
    pct = max(0.0, min(100.0, score)) / 100.0
    angle = math.pi * (1 - pct)  # radians, from pi (left) → 0 (right)
    end_x = cx + r * math.cos(angle)
    end_y = cy - r * math.sin(angle)
    start_x = cx - r
    start_y = cy
    large_arc = 0  # always <=180deg
    # Background half ring
    bg_path = f"M {start_x:.1f} {start_y:.1f} A {r:.1f} {r:.1f} 0 0 1 {cx + r:.1f} {cy:.1f}"
    # Score arc
    if pct > 0.001:
        score_path = f"M {start_x:.1f} {start_y:.1f} A {r:.1f} {r:.1f} 0 {large_arc} 1 {end_x:.1f} {end_y:.1f}"
        score_arc = (
            f'<path d="{score_path}" fill="none" stroke="{color}" '
            f'stroke-width="{size * 0.10:.1f}" stroke-linecap="round"/>'
        )
    else:
        score_arc = ""
    score_txt = f"{int(round(score))}"
    label_esc = html.escape(label)
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" '
        f'width="{size}" height="{size}" role="img" aria-label="{label_esc} score {score_txt}">'
        f'<path d="{bg_path}" fill="none" stroke="#e2e8f0" stroke-width="{size * 0.10:.1f}" stroke-linecap="round"/>'
        f"{score_arc}"
        f'<text x="{cx}" y="{cy - 4}" text-anchor="middle" '
        f'font-family="-apple-system,Segoe UI,sans-serif" font-size="{size * 0.28:.0f}" '
        f'font-weight="700" fill="#0f172a">{score_txt}</text>'
        f'<text x="{cx}" y="{cy + size * 0.18:.0f}" text-anchor="middle" '
        f'font-family="-apple-system,Segoe UI,sans-serif" font-size="{size * 0.10:.0f}" '
        f'fill="#64748b">{label_esc}</text>'
        "</svg>"
    )


def _svg_bar(series: List[Dict[str, Any]], width: int = 920, row_h: int = 32) -> str:
    """Grouped horizontal bar chart: one row per axis, one bar per subject.

    `series` = [{"name": str, "color": str, "values": {axis_key: float}}]
    """
    pad_l = 160
    pad_r = 60
    pad_t = 24
    inner_w = width - pad_l - pad_r
    n_subjects = len(series)
    bar_h = max(6, (row_h - 8) // max(1, n_subjects))
    height = pad_t + len(AXES) * row_h + 30

    rows = []
    # Axis grid + ticks at 25/50/75/100
    for t in (0, 25, 50, 75, 100):
        x = pad_l + (t / 100.0) * inner_w
        rows.append(
            f'<line x1="{x:.1f}" y1="{pad_t - 6}" x2="{x:.1f}" y2="{pad_t + len(AXES)*row_h:.0f}" '
            f'stroke="#e2e8f0" stroke-width="1"/>'
            f'<text x="{x:.1f}" y="{pad_t + len(AXES)*row_h + 16:.0f}" '
            f'font-family="-apple-system,Segoe UI,sans-serif" font-size="10" '
            f'fill="#94a3b8" text-anchor="middle">{t}</text>'
        )

    for i, (key, label) in enumerate(AXES):
        y_row = pad_t + i * row_h
        rows.append(
            f'<text x="{pad_l - 10}" y="{y_row + row_h/2 + 4:.1f}" '
            f'font-family="-apple-system,Segoe UI,sans-serif" font-size="12" '
            f'fill="#334155" text-anchor="end">{html.escape(label)}</text>'
        )
        for j, s in enumerate(series):
            val = float(s["values"].get(key, 0) or 0)
            bar_w = max(0.0, min(100.0, val)) / 100.0 * inner_w
            y_bar = y_row + 4 + j * bar_h
            rows.append(
                f'<rect x="{pad_l:.1f}" y="{y_bar:.1f}" width="{bar_w:.1f}" '
                f'height="{bar_h - 1}" fill="{s["color"]}" rx="2"/>'
            )

    # Legend
    legend_y = height - 8
    legend_x = pad_l
    legend_parts = []
    for s in series:
        legend_parts.append(
            f'<rect x="{legend_x:.0f}" y="{legend_y - 9:.0f}" width="10" height="10" '
            f'fill="{s["color"]}" rx="2"/>'
            f'<text x="{legend_x + 14:.0f}" y="{legend_y:.0f}" '
            f'font-family="-apple-system,Segoe UI,sans-serif" font-size="11" '
            f'fill="#334155">{html.escape(s["name"])}</text>'
        )
        legend_x += 16 + len(s["name"]) * 7 + 18

    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height + 16}" '
        f'width="100%" height="{height + 16}" role="img" aria-label="Sub-score comparison">'
        + "".join(rows)
        + "".join(legend_parts)
        + "</svg>"
    )


# ═══════════════════════════════════════════════════════════════
# DATA SHAPING
# ═══════════════════════════════════════════════════════════════

async def generate_summary(
    workspace_id: str,
    competitor_domains: List[str],
) -> Dict[str, Any]:
    """Return the raw data dict the HTML template renders from.
    Useful for JSON API consumers."""
    ws = await fetch_one(
        "SELECT id, slug, name, brand_name, domains, color_primary, color_accent "
        "FROM workspaces WHERE id = ?",
        (workspace_id,),
    )
    if not ws:
        return {
            "workspace_id": workspace_id,
            "found": False,
            "us": None,
            "competitors": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    our_authority = await _latest_authority(workspace_id, "", is_us=1)
    our_caps = await _our_capabilities(workspace_id)

    competitors: List[Dict[str, Any]] = []
    for dom in competitor_domains:
        auth = await _latest_authority(workspace_id, dom, is_us=0)
        caps = await _competitor_capabilities(workspace_id, dom)
        competitors.append({
            "domain": dom,
            "authority": auth,
            "capabilities": caps or {},
        })

    return {
        "workspace_id": workspace_id,
        "found": True,
        "slug": ws["slug"],
        "brand_name": ws.get("brand_name") or ws.get("name") or ws["slug"],
        "color_primary": ws.get("color_primary") or "#2563EB",
        "color_accent": ws.get("color_accent") or "#10B981",
        "domains": from_json(ws.get("domains") or "[]", []),
        "us": {
            "authority": our_authority,
            "capabilities": our_caps,
        },
        "competitors": competitors,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _beat_axes(us_caps: Dict[str, float], comp_caps: Dict[str, Any], reverse: bool = False) -> List[Dict[str, Any]]:
    """Return top-3 capability axes ranked by competitor - us delta.
    reverse=True returns the axes where we beat them."""
    axes_map = [
        ("schema_score", "Schema markup"),
        ("reddit_score", "Reddit authority"),
        ("youtube_score", "YouTube presence"),
        ("faq_depth_score", "FAQ depth"),
        ("decision_support_score", "Decision support content"),
        ("review_score", "Reviews"),
        ("entity_consistency_score", "Entity consistency"),
        ("pr_score", "PR / press"),
        ("local_authority_score", "Local authority"),
    ]
    rows = []
    for key, label in axes_map:
        them = float(comp_caps.get(key) or 0)
        us = float(us_caps.get(key) or 0)
        delta = them - us
        if reverse:
            delta = -delta
        if delta > 0.5:
            rows.append({"axis": label, "us": us, "them": them, "delta": delta})
    rows.sort(key=lambda r: r["delta"], reverse=True)
    return rows[:3]


# ═══════════════════════════════════════════════════════════════
# HTML RENDERING
# ═══════════════════════════════════════════════════════════════

def _gauge_card(name: str, total: float, sub: str = "") -> str:
    label = _band_label(total)
    return (
        f'<div class="gauge"><div class="gauge-name">{html.escape(name)}</div>'
        f'{_svg_gauge(total, label)}'
        f'<div class="gauge-sub">{html.escape(sub)}</div></div>'
    )


def _render_score_panel(data: Dict[str, Any]) -> str:
    cards = []
    us_auth = (data.get("us") or {}).get("authority") or {}
    us_total = float(us_auth.get("total_score") or 0)
    cards.append(_gauge_card(data["brand_name"] + " (You)", us_total, "GEO Authority"))
    for c in data["competitors"]:
        auth = c.get("authority") or {}
        total = float(auth.get("total_score") or 0)
        cards.append(_gauge_card(c["domain"], total, "GEO Authority"))
    return f'<section class="panel"><h2>GEO Authority Score</h2><div class="gauge-row">{"".join(cards)}</div></section>'


def _render_bar_section(data: Dict[str, Any]) -> str:
    series = []
    us_auth = (data.get("us") or {}).get("authority") or {}
    series.append({
        "name": data["brand_name"] + " (You)",
        "color": US_COLOR,
        "values": {k: float(us_auth.get(k) or 0) for k, _ in AXES},
    })
    for i, c in enumerate(data["competitors"]):
        auth = c.get("authority") or {}
        series.append({
            "name": c["domain"],
            "color": COMPETITOR_COLORS[i % len(COMPETITOR_COLORS)],
            "values": {k: float(auth.get(k) or 0) for k, _ in AXES},
        })
    return (
        '<section class="panel"><h2>Sub-score breakdown</h2>'
        '<p class="muted">All 7 GEO Authority dimensions on a 0–100 scale.</p>'
        f'<div class="chart-wrap">{_svg_bar(series)}</div>'
        "</section>"
    )


def _render_competitor_diffs(data: Dict[str, Any]) -> str:
    us_caps = (data.get("us") or {}).get("capabilities") or {}
    blocks = []
    for c in data["competitors"]:
        wins = _beat_axes(us_caps, c.get("capabilities") or {}, reverse=False)
        losses = _beat_axes(us_caps, c.get("capabilities") or {}, reverse=True)

        def _rows(items: List[Dict[str, Any]], cls: str) -> str:
            if not items:
                return f'<li class="empty">No significant gap detected.</li>'
            lis = []
            for it in items:
                lis.append(
                    f'<li class="{cls}"><span class="ax">{html.escape(it["axis"])}</span>'
                    f'<span class="delta">{it["delta"]:.0f} pts</span>'
                    f'<span class="muted">you {int(round(it["us"]))} · them {int(round(it["them"]))}</span></li>'
                )
            return "".join(lis)

        blocks.append(
            f'<div class="diff-card">'
            f'<h3>{html.escape(c["domain"])}</h3>'
            f'<div class="diff-cols">'
            f'<div><h4 class="bad">Where they beat you</h4><ul>{_rows(wins, "bad")}</ul></div>'
            f'<div><h4 class="good">Where you beat them</h4><ul>{_rows(losses, "good")}</ul></div>'
            f"</div></div>"
        )
    return f'<section class="panel"><h2>Head-to-head gaps</h2>{"".join(blocks)}</section>'


def _render_prompts_table(prompts: List[Dict[str, Any]]) -> str:
    if not prompts:
        return (
            '<section class="panel"><h2>High-value prompts at risk</h2>'
            '<p class="muted">No high-revenue prompts where you trail. Nice.</p></section>'
        )
    rows = []
    for p in prompts:
        eur = float(p.get("estimated_value_eur") or 0)
        gap = float(p.get("leader_score") or 0) - float(p.get("our_score") or 0)
        eur_at_stake = eur * (gap / 100.0) if eur > 0 else 0
        eur_str = f"€{eur_at_stake:,.0f}" if eur_at_stake > 0 else "—"
        rows.append(
            "<tr>"
            f'<td class="prompt-text">{html.escape((p.get("text") or "")[:140])}</td>'
            f'<td>{int(round(float(p.get("our_score") or 0)))}</td>'
            f'<td>{html.escape(p.get("leader_domain") or "—")}</td>'
            f'<td>{int(round(float(p.get("leader_score") or 0)))}</td>'
            f"<td>{eur_str}</td>"
            "</tr>"
        )
    return (
        '<section class="panel"><h2>High-value prompts at risk</h2>'
        '<p class="muted">High-revenue questions (revenue_score ≥ 60) where a competitor outranks you.</p>'
        '<table><thead><tr>'
        "<th>Prompt</th><th>You</th><th>Leader</th><th>Score</th><th>€ at stake</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></section>"
    )


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def generate_html(
    workspace_id: str,
    competitor_domains: List[str],
    include_prompts: bool = True,
) -> str:
    """Render a fully self-contained HTML comparative report.

    Print to PDF via the browser's Save-as-PDF. No external assets.
    """
    data = await generate_summary(workspace_id, competitor_domains)

    if not data.get("found"):
        return (
            "<!doctype html><html><body><h1>Workspace not found</h1>"
            f"<p>workspace_id={html.escape(workspace_id)}</p></body></html>"
        )

    prompts = await _losing_prompts(workspace_id) if include_prompts else []

    primary = data["color_primary"]
    accent = data["color_accent"]
    brand = data["brand_name"]
    slug = data["slug"]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    comps_str = ", ".join(c["domain"] for c in data["competitors"]) or "(no competitors)"

    score_panel = _render_score_panel(data)
    bar_section = _render_bar_section(data)
    diff_section = _render_competitor_diffs(data)
    prompts_section = _render_prompts_table(prompts) if include_prompts else ""

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Comparative GEO Report — {html.escape(brand)}</title>
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
.muted {{ color: #64748b; font-size: 13px; margin: 4px 0 12px; }}
.gauge-row {{ display: flex; gap: 16px; flex-wrap: wrap; }}
.gauge {{ flex: 1 1 180px; min-width: 180px; background: #f8fafc;
          border: 1px solid #e2e8f0; border-radius: 10px; padding: 16px;
          text-align: center; }}
.gauge-name {{ font-size: 13px; font-weight: 600; color: #334155;
               margin-bottom: 6px; min-height: 36px;
               display: flex; align-items: center; justify-content: center; }}
.gauge-sub {{ font-size: 11px; color: #94a3b8; margin-top: 2px; text-transform: uppercase; letter-spacing: .05em; }}
.chart-wrap {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 16px; }}
.diff-card {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px;
              padding: 16px 20px; margin-bottom: 14px; page-break-inside: avoid; }}
.diff-card h3 {{ font-size: 15px; margin: 0 0 12px; color: #0f172a; }}
.diff-cols {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
.diff-cols h4 {{ font-size: 12px; text-transform: uppercase; letter-spacing: .06em;
                 margin: 0 0 8px; }}
.diff-cols h4.bad {{ color: #b91c1c; }}
.diff-cols h4.good {{ color: #047857; }}
.diff-cols ul {{ list-style: none; padding: 0; margin: 0; font-size: 13px; }}
.diff-cols li {{ display: flex; justify-content: space-between; gap: 8px;
                 padding: 6px 0; border-bottom: 1px dashed #e2e8f0; }}
.diff-cols li.empty {{ color: #94a3b8; font-style: italic; justify-content: flex-start; }}
.diff-cols li .ax {{ font-weight: 500; flex: 1; }}
.diff-cols li .delta {{ font-variant-numeric: tabular-nums; color: #0f172a; font-weight: 600; }}
.diff-cols li .muted {{ font-size: 11px; color: #94a3b8; margin: 0; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ padding: 8px 10px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
th {{ background: #f1f5f9; font-weight: 600; color: #334155; font-size: 12px;
      text-transform: uppercase; letter-spacing: .04em; }}
td.prompt-text {{ max-width: 460px; }}
footer.foot {{ margin-top: 40px; padding-top: 16px; border-top: 1px solid #e2e8f0;
               color: #94a3b8; font-size: 12px; display: flex; justify-content: space-between; }}
@media print {{
  .report {{ padding: 0; max-width: none; }}
  .panel {{ margin: 18px 0; }}
}}
</style>
</head><body>
<div class="stripe"></div>
<div class="report">
  <header class="head">
    <h1>Competitive GEO Report — {html.escape(brand)}</h1>
    <div class="meta">vs {html.escape(comps_str)} · generated {html.escape(ts)}</div>
  </header>
  {score_panel}
  {bar_section}
  {diff_section}
  {prompts_section}
  <footer class="foot">
    <span>Generated by Momentus AI</span>
    <span>workspace: {html.escape(slug)}</span>
  </footer>
</div>
</body></html>"""
