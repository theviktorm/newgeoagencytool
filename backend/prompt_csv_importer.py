"""
Momentus AI — Peec Prompts Export CSV importer

Peec exports two different CSV shapes:

  1. **Citations export** (the one peec_connector.py handles): one row per
     mention with `url`, `model`, `citation_count`, etc.

  2. **Prompts export** (THIS module): one row per tracked prompt with
     `topic_name`, `prompt`, `visibility`, `sentiment`, `position`,
     `mentions` (a COMMA-SEPARATED list of brand names — not a count!),
     `share_of_voice`, etc.

The two shapes share almost nothing, which is why the legacy importer
chokes on the prompts export ("No URL column detected").

This importer:
  - auto-detects the prompts-export shape from the header
  - upserts a prompt row per CSV line (into the `prompts` table)
  - synthesises a `prompt_observations` row from each line capturing
    visibility / position / sentiment / brand list (model = "peec_aggregate")
  - refreshes prompt_ownership rollups
  - returns a summary identical in shape to the URL importer
"""
from __future__ import annotations

import csv
import io
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from .database import execute, fetch_one, gen_id, to_json
from . import prompt_engine

logger = logging.getLogger("geo.prompt_csv_import")

# Header → canonical field. Lower-cased match. Multiple aliases allowed per field.
HEADER_MAP = {
    "prompt": ("prompt", "query", "question", "text"),
    "topic_name": ("topic_name", "topic", "cluster", "topic name"),
    "topic_id": ("topic_id", "topic id"),
    "external_id": ("id", "prompt_id", "prompt id"),
    "visibility": ("visibility", "visibility score"),
    "visibility_delta": ("visibility_delta", "visibility delta"),
    "sentiment": ("sentiment", "sentiment score", "sentiment_score"),
    "sentiment_delta": ("sentiment_delta", "sentiment delta"),
    "position": ("position", "rank", "avg_position", "avg position"),
    "position_delta": ("position_delta", "position delta"),
    "mentions": ("mentions", "brands", "brands_mentioned"),
    "volume": ("volume",),
    "tags": ("tags",),
    "location": ("location", "country", "locale"),
    "share_of_voice": ("share_of_voice", "share of voice", "sov"),
    "share_of_voice_delta": ("share_of_voice_delta", "share of voice delta", "sov_delta"),
    "added_at": ("added_at", "added at", "created_at", "imported_at"),
    "status": ("status",),
}


# ═══════════════════════════════════════════════════════════════
# DETECTION
# ═══════════════════════════════════════════════════════════════

def is_prompts_export(headers: List[str]) -> bool:
    """Return True if this CSV looks like a Peec prompts export, not a
    citations export. Prompts export has a 'prompt' column AND lacks 'url'."""
    h = [x.strip().lower() for x in headers]
    has_prompt = any(a in h for a in HEADER_MAP["prompt"])
    has_url = any(a in h for a in ("url", "cited_url", "source_url", "page_url"))
    # Strong signal: visibility/share_of_voice/topic_name are unique to the prompts export
    prompts_signals = sum(
        1 for fld in ("topic_name", "visibility", "share_of_voice", "mentions")
        if any(a in h for a in HEADER_MAP[fld])
    )
    return has_prompt and (not has_url or prompts_signals >= 2)


def detect_mapping(headers: List[str]) -> Dict[str, int]:
    h = [x.strip().lower() for x in headers]
    out: Dict[str, int] = {}
    for field, aliases in HEADER_MAP.items():
        for a in aliases:
            if a in h:
                out[field] = h.index(a)
                break
    return out


# ═══════════════════════════════════════════════════════════════
# IMPORT
# ═══════════════════════════════════════════════════════════════

async def import_csv(
    workspace_id: str,
    content: str,
    target_brand: str = "",
    classify: bool = False,
) -> Dict[str, Any]:
    """Top-level: ingest a Peec prompts CSV into prompts + prompt_observations.

    `target_brand` is matched (case-insensitive substring) against each row's
    mentions list to mark `our_brand_present`.
    """
    if not content:
        return {"success": False, "error": "empty CSV"}
    reader = csv.reader(io.StringIO(content))
    try:
        headers = next(reader)
    except StopIteration:
        return {"success": False, "error": "CSV has no header row"}

    if not is_prompts_export(headers):
        return {
            "success": False,
            "error": ("This CSV doesn't look like a Peec Prompts Export "
                      "(no prompt column or also has url column). Use the "
                      "Citations CSV upload instead."),
            "headers_seen": headers,
        }

    # Resolve target brand from workspace if not supplied
    if not target_brand:
        ws = await fetch_one(
            "SELECT brand_name, name FROM workspaces WHERE id = ?",
            (workspace_id,),
        )
        if ws:
            target_brand = (ws.get("brand_name") or ws.get("name") or "")
    target_brand_lc = target_brand.strip().lower()

    mapping = detect_mapping(headers)
    if "prompt" not in mapping:
        return {"success": False, "error": "No 'prompt' column found"}

    batch_id = gen_id("pcsv-")
    summary: Dict[str, Any] = {
        "success": True, "batch_id": batch_id, "rows": 0,
        "prompts_upserted": 0, "observations": 0, "our_brand_hits": 0,
        "errors": [], "skipped": 0,
    }

    for row in reader:
        summary["rows"] += 1
        if not row or not any(x.strip() for x in row):
            summary["skipped"] += 1
            continue
        try:
            await _process_row(
                workspace_id, row, mapping, target_brand_lc, batch_id, summary,
            )
        except Exception as e:
            summary["errors"].append({"row": summary["rows"], "error": str(e)})

    # Refresh ownership rollups for the prompts we just touched.
    try:
        top_prompts = await prompt_engine.list_prompts(workspace_id, limit=500)
        for p in top_prompts[:200]:
            try:
                await prompt_engine.recompute_ownership(workspace_id, p["id"])
            except Exception:
                continue
    except Exception:
        pass

    # Seed competitor_capabilities rows from brand mentions so the Attack
    # Map page has something to show without requiring a Peec MCP sync.
    try:
        await _seed_competitor_capabilities_from_csv(workspace_id)
    except Exception as e:
        logger.warning("competitor seeding failed: %s", e)

    # Optional: Claude classification for the top-revenue prompts so the
    # battlefield filters & priority calc reflect real intent. Capped so a
    # huge CSV import doesn't stall the request.
    if classify:
        try:
            await prompt_engine.reclassify_workspace(workspace_id, max_n=40)
        except Exception:
            pass

    return summary


async def _seed_competitor_capabilities_from_csv(workspace_id: str) -> None:
    """Build a first-pass competitor_capabilities row for every brand we saw
    in the Prompts CSV. Scores come from prompt_observations alone — they get
    refined later by attack_map.analyze_competitor (which scrapes pages)."""
    from .database import fetch_all as _fa, execute as _ex
    obs_rows = await _fa(
        "SELECT brands_appeared FROM prompt_observations WHERE workspace_id = ?",
        (workspace_id,),
    )
    if not obs_rows:
        return
    counts: Dict[str, int] = {}
    pos_sum: Dict[str, float] = {}
    for o in obs_rows:
        try:
            import json as _json
            brands = _json.loads(o.get("brands_appeared") or "[]")
        except Exception:
            brands = []
        for b in brands or []:
            name = (b.get("name") if isinstance(b, dict) else str(b)) or ""
            name = name.strip().lower()
            if not name:
                continue
            counts[name] = counts.get(name, 0) + 1
            pos_sum[name] = pos_sum.get(name, 0.0) + (1.0 / max(1, int(
                (b.get("position") if isinstance(b, dict) else 1) or 1)))
    if not counts:
        return
    # Normalise to 0-100 against the strongest brand
    max_count = max(counts.values()) or 1
    for brand, n in counts.items():
        strength = min(100.0, (n / max_count) * 100.0)
        avg_pos_score = min(100.0, (pos_sum[brand] / n) * 100.0)
        # Local-authority proxy = how often they get cited at all
        await _ex(
            """INSERT INTO competitor_capabilities
               (workspace_id, competitor_domain, schema_score, reddit_score,
                youtube_score, faq_depth_score, decision_support_score,
                review_score, entity_consistency_score, pr_score,
                local_authority_score, overall_strength, weakest_axis,
                weakest_axis_score, summary, analyzed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(workspace_id, competitor_domain) DO UPDATE SET
                 decision_support_score = MAX(decision_support_score, excluded.decision_support_score),
                 local_authority_score = MAX(local_authority_score, excluded.local_authority_score),
                 overall_strength = MAX(overall_strength, excluded.overall_strength),
                 analyzed_at = excluded.analyzed_at""",
            (
                workspace_id, brand,
                0.0, 0.0, 0.0, 0.0,         # schema/reddit/youtube/faq — need scrape
                avg_pos_score,              # decision_support proxy from position
                0.0, 0.0, 0.0,
                strength,                   # local_authority proxy from raw frequency
                (avg_pos_score + strength) / 2.0,
                "schema_score", 0.0,
                f"Seeded from CSV: cited in {n} prompt observation(s).",
            ),
        )


async def _process_row(
    workspace_id: str, row: List[str], mapping: Dict[str, int],
    target_brand_lc: str, batch_id: str, summary: Dict[str, Any],
) -> None:
    def g(field: str, default: str = "") -> str:
        idx = mapping.get(field)
        if idx is None or idx >= len(row):
            return default
        return (row[idx] or "").strip().strip('"')

    text = g("prompt")
    if not text:
        summary["skipped"] += 1
        return

    topic_name = g("topic_name")
    topic_id = g("topic_id")
    cluster_id = topic_id or _slug(topic_name) or ""

    # Upsert prompt (heuristic classification only — fast).
    upserted = await prompt_engine.upsert_prompt(
        workspace_id, text,
        target_brand=target_brand_lc,
        cluster_id=cluster_id,
        classify=False,  # bulk import; reclassify_workspace handles it after
        source="peec_import",
        confidence="imported",
    )
    summary["prompts_upserted"] += 1
    prompt_id = upserted["id"]

    # Build an observation from Peec's aggregate data
    mentions_raw = g("mentions")
    brands = _split_brands(mentions_raw)
    sources: List[Dict[str, Any]] = []  # Peec prompt-export doesn't carry URLs
    our_present = bool(target_brand_lc) and any(
        target_brand_lc in (b.get("name") or "").lower() for b in brands
    )
    our_pos = 0
    if our_present:
        for b in brands:
            if target_brand_lc in (b.get("name") or "").lower():
                our_pos = int(b.get("position") or 0)
                break

    visibility = _f(g("visibility"))
    position = _f(g("position"))
    sentiment_score = _f(g("sentiment"))
    sov = _f(g("share_of_voice"))

    await execute(
        "INSERT INTO prompt_observations "
        "(id, prompt_id, workspace_id, model, brands_appeared, sources_cited, "
        " our_brand_present, our_brand_position, ai_overview_present, "
        " answer_text, sentiment_score, sync_batch_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            gen_id("po-"), prompt_id, workspace_id, "peec_aggregate",
            to_json(brands), to_json(sources),
            1 if our_present else 0, our_pos, 0,
            f"visibility={visibility} sov={sov} position={position}",
            sentiment_score,
            batch_id,
        ),
    )
    summary["observations"] += 1
    if our_present:
        summary["our_brand_hits"] += 1


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════

def _split_brands(raw: str) -> List[Dict[str, Any]]:
    if not raw:
        return []
    # Peec uses comma-separated; many brand names contain dots / spaces.
    parts = [p.strip() for p in raw.split(",")]
    out: List[Dict[str, Any]] = []
    for i, name in enumerate(parts, start=1):
        if not name:
            continue
        out.append({"name": name, "position": i, "snippet": ""})
    return out


def _f(s: str) -> float:
    if not s:
        return 0.0
    try:
        return float(s.replace(",", "."))
    except Exception:
        return 0.0


def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:80]
