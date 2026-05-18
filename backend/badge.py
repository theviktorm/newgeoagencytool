"""
Momentus AI — Public GEO Authority Badge

Shields.io-style SVG badge customers embed on their site:

    ![GEO Score](https://app.momentus.ai/badge/aesthetic-klinika.svg)

Free distribution + social proof. Public endpoint — only renders
non-sensitive fields (brand name + total score). NO emails, NO
compliance rules, NO prompts.

Public API:
  - render_badge_svg(slug, style='flat')  -> SVG string
  - badge_html(slug)                      -> HTML preview / snippets page
"""
from __future__ import annotations

import html
import logging
from typing import Any, Dict, Optional, Tuple

from .database import fetch_one

logger = logging.getLogger("geo.badge")

# ─── Shields.io-flat palette ───
COLOR_UNKNOWN = "#9ca3af"   # gray-400
COLOR_LABEL_BG = "#555555"  # standard shields label background


def _band_color(score: float) -> str:
    if score >= 85:
        return "#10b981"  # emerald
    if score >= 70:
        return "#22c55e"  # green
    if score >= 55:
        return "#eab308"  # amber
    if score >= 40:
        return "#f97316"  # orange
    return "#ef4444"      # rose


def _text_width(text: str) -> int:
    """Approximate Verdana 11px text width in pixels. Shields uses ~6px per
    average char; widen for caps/digits. Pure heuristic — keeps SVG small
    and avoids font metrics libraries."""
    width = 0.0
    for ch in text:
        if ch in "ilI1.,:;|!":
            width += 3.2
        elif ch in "mwMW":
            width += 9.0
        elif ch.isupper() or ch.isdigit():
            width += 7.0
        else:
            width += 6.0
    return int(width + 6)


async def _lookup(slug: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (workspace, latest_us_score_row) — either may be None."""
    ws = await fetch_one(
        "SELECT id, slug, brand_name, name, color_primary, color_accent "
        "FROM workspaces WHERE slug = ? AND is_active = 1",
        (slug,),
    )
    if not ws:
        return None, None
    score = await fetch_one(
        "SELECT total_score, observed_at FROM authority_scores "
        "WHERE workspace_id = ? AND is_us = 1 "
        "ORDER BY observed_at DESC LIMIT 1",
        (ws["id"],),
    )
    return ws, score


def _build_svg(label: str, value: str, value_color: str, style: str = "flat") -> str:
    """Build a shields.io-flat-style two-tone pill SVG.
    The whole pill auto-sizes. Total < 2KB. Pure f-string, no deps."""
    label = label[:32]
    value = value[:24]
    label_w = _text_width(label)
    value_w = _text_width(value)
    total_w = label_w + value_w
    height = 20
    radius = 3 if style == "flat" else 0
    # Center positions (×10 for sub-pixel accuracy in text rendering)
    label_cx = (label_w * 10) // 2
    value_cx = (label_w * 10) + (value_w * 10) // 2
    label_esc = html.escape(label)
    value_esc = html.escape(value)
    aria = html.escape(f"{label}: {value}")
    # Gradient overlay for the soft shields-flat sheen
    grad = (
        '<linearGradient id="s" x2="0" y2="100%">'
        '<stop offset="0" stop-color="#fff" stop-opacity=".7"/>'
        '<stop offset=".1" stop-color="#aaa" stop-opacity=".1"/>'
        '<stop offset=".9" stop-color="#000" stop-opacity=".3"/>'
        '<stop offset="1" stop-color="#000" stop-opacity=".5"/>'
        "</linearGradient>"
    )
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{total_w}" height="{height}" role="img" aria-label="{aria}">'
        f"<title>{aria}</title>"
        f"<defs>{grad}"
        f'<clipPath id="r"><rect width="{total_w}" height="{height}" rx="{radius}" fill="#fff"/></clipPath>'
        "</defs>"
        f'<g clip-path="url(#r)">'
        f'<rect width="{label_w}" height="{height}" fill="{COLOR_LABEL_BG}"/>'
        f'<rect x="{label_w}" width="{value_w}" height="{height}" fill="{value_color}"/>'
        f'<rect width="{total_w}" height="{height}" fill="url(#s)"/>'
        "</g>"
        '<g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" '
        'text-rendering="geometricPrecision" font-size="110">'
        f'<text aria-hidden="true" x="{label_cx}" y="150" fill="#010101" fill-opacity=".3" '
        f'transform="scale(.1)" textLength="{label_w * 10 - 20}">{label_esc}</text>'
        f'<text x="{label_cx}" y="140" transform="scale(.1)" fill="#fff" '
        f'textLength="{label_w * 10 - 20}">{label_esc}</text>'
        f'<text aria-hidden="true" x="{value_cx}" y="150" fill="#010101" fill-opacity=".3" '
        f'transform="scale(.1)" textLength="{value_w * 10 - 20}">{value_esc}</text>'
        f'<text x="{value_cx}" y="140" transform="scale(.1)" fill="#fff" '
        f'textLength="{value_w * 10 - 20}">{value_esc}</text>'
        "</g>"
        # Faint attribution — bottom-right tiny text inside the pill
        f'<text x="{total_w - 2}" y="{height - 1}" text-anchor="end" '
        'font-family="Verdana,sans-serif" font-size="3" fill="#fff" fill-opacity=".35">momentus.ai</text>'
        "</svg>"
    )
    return svg


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def render_badge_svg(slug: str, style: str = "flat") -> str:
    """Render an embeddable GEO Authority badge for `slug`.

    Always returns a valid SVG string (gray "unknown" badge if the workspace
    or score is missing). Caller should respond with Content-Type:
    image/svg+xml. Never raises for missing data.
    """
    try:
        ws, score = await _lookup(slug)
    except Exception as exc:  # noqa: BLE001 — public endpoint, never 500
        logger.warning("badge lookup failed for slug=%s: %s", slug, exc)
        ws, score = None, None

    if not ws or not score or score.get("total_score") is None:
        return _build_svg("GEO Authority", "unknown", COLOR_UNKNOWN, style)

    total = float(score["total_score"])
    value = f"{int(round(total))}/100"
    color = _band_color(total)
    return _build_svg("GEO Authority", value, color, style)


async def badge_html(slug: str) -> str:
    """HTML preview page: shows the live badge + copy-paste embed snippets.

    Mounted by the API at e.g. /badge/{slug} (vs /badge/{slug}.svg which
    returns raw SVG). Pure HTML — no external assets.
    """
    try:
        ws, score = await _lookup(slug)
    except Exception:  # noqa: BLE001
        ws, score = None, None

    brand = (ws or {}).get("brand_name") or (ws or {}).get("name") or slug
    primary = (ws or {}).get("color_primary") or "#2563EB"
    brand_esc = html.escape(brand)
    slug_esc = html.escape(slug)

    if score and score.get("total_score") is not None:
        total = float(score["total_score"])
        score_label = f"{int(round(total))}/100"
        score_color = _band_color(total)
    else:
        score_label = "unknown"
        score_color = COLOR_UNKNOWN

    badge_url = f"https://app.momentus.ai/badge/{slug_esc}.svg"
    page_url = f"https://app.momentus.ai/badge/{slug_esc}"
    md_snippet = f"[![GEO Authority]({badge_url})]({page_url})"
    html_snippet = (
        f'<a href="{page_url}"><img src="{badge_url}" alt="GEO Authority Score" /></a>'
    )

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>GEO Authority badge — {brand_esc}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root {{ color-scheme: light; }}
* {{ box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       margin: 0; background: #f8fafc; color: #0f172a; line-height: 1.5; }}
.stripe {{ height: 6px; background: {primary}; }}
.wrap {{ max-width: 720px; margin: 0 auto; padding: 48px 24px; }}
h1 {{ font-size: 28px; margin: 0 0 8px; letter-spacing: -.01em; }}
.muted {{ color: #64748b; font-size: 14px; }}
.card {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 12px;
         padding: 24px; margin: 24px 0; }}
.badge-preview {{ display: flex; align-items: center; justify-content: center;
                  background: #f1f5f9; border-radius: 8px; padding: 32px;
                  font-family: Verdana, sans-serif; font-size: 11px; color: #fff; }}
.pill {{ display: inline-flex; border-radius: 4px; overflow: hidden;
         box-shadow: 0 1px 2px rgba(0,0,0,.08); }}
.pill .l {{ background: {COLOR_LABEL_BG}; padding: 4px 8px; }}
.pill .v {{ background: {score_color}; padding: 4px 8px; font-weight: 600; }}
h2 {{ font-size: 16px; margin: 20px 0 8px; }}
pre {{ background: #0f172a; color: #e2e8f0; padding: 14px 16px; border-radius: 8px;
       overflow-x: auto; font-size: 13px; margin: 0; }}
footer {{ text-align: center; color: #94a3b8; font-size: 12px; padding: 24px; }}
footer a {{ color: #94a3b8; }}
</style>
</head><body>
<div class="stripe"></div>
<div class="wrap">
  <h1>{brand_esc}</h1>
  <p class="muted">GEO Authority Score badge</p>

  <div class="card">
    <div class="badge-preview">
      <span class="pill"><span class="l">GEO Authority</span><span class="v">{html.escape(score_label)}</span></span>
    </div>
  </div>

  <div class="card">
    <h2>Markdown</h2>
    <pre>{html.escape(md_snippet)}</pre>
    <h2>HTML</h2>
    <pre>{html.escape(html_snippet)}</pre>
    <h2>Direct SVG URL</h2>
    <pre>{html.escape(badge_url)}</pre>
  </div>
</div>
<footer>Powered by <a href="https://momentus.ai">Momentus AI</a></footer>
</body></html>"""
