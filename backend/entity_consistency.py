"""
Momentus AI — Entity Consistency Engine (Phase 4).

Audits how consistently a brand's identity (name, NAP, services, sameAs, etc.)
is expressed across the surfaces AI engines crawl: the workspace's own pages,
JSON-LD schema captured in schema_audits, and brand aliases. Inconsistencies
erode entity disambiguation and citation rates.

Pure stdlib + the shared `.database` helpers. Defensive: an empty workspace
must return `axes: []`, `overall_score: 0`, `confidence: 'needs_review'` and
never raise. We log observations to `entity_consistency_results` so later
runs can show drift over time.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

from .database import execute, fetch_all, fetch_one, from_json, gen_id, get_db

logger = logging.getLogger("geo.entity_consistency")


# ─── SCHEMA ─────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS entity_consistency_results (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    axis            TEXT NOT NULL,
    source          TEXT NOT NULL,
    value           TEXT DEFAULT '',
    status          TEXT NOT NULL,
    notes           TEXT DEFAULT '',
    confidence      TEXT DEFAULT 'estimated',
    observed_at     TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_entcons_ws_axis
    ON entity_consistency_results(workspace_id, axis);
"""

_AXES = (
    "brand_name", "legal_name", "address", "phone", "services",
    "experts", "social_links", "categories", "descriptions", "sameAs",
)

_JSONLD_KEYS = {
    "brand_name": ("name", "brand"),
    "legal_name": ("legalName",),
    "address": ("address",),
    "phone": ("telephone", "phone"),
    "services": ("makesOffer", "hasOfferCatalog", "knowsAbout"),
    "experts": ("employee", "founder", "member"),
    "social_links": ("sameAs",),
    "categories": ("@type", "category", "additionalType"),
    "descriptions": ("description",),
    "sameAs": ("sameAs",),
}


async def init() -> None:
    """Create the entity_consistency_results table + index. Idempotent."""
    db = await get_db()
    await db.executescript(_SCHEMA)
    await db.commit()


# ─── HELPERS ────────────────────────────────────────────────────────────────

def _norm(s: Any) -> str:
    return "" if s is None else " ".join(str(s).lower().split())


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


async def _load_workspace(workspace_id: str) -> Dict[str, Any]:
    try:
        row = await fetch_one(
            "SELECT id, name, brand_name, domains, settings "
            "FROM workspaces WHERE id = ?",
            (workspace_id,),
        )
    except Exception as e:
        logger.warning("entity_consistency: workspaces read failed: %s", e)
        row = None
    return row or {}


async def _load_aliases(workspace_id: str) -> List[Dict[str, Any]]:
    try:
        return await fetch_all(
            "SELECT alias, normalized_alias, source, confidence "
            "FROM brand_aliases WHERE workspace_id = ?",
            (workspace_id,),
        ) or []
    except Exception as e:
        logger.warning("entity_consistency: brand_aliases read failed: %s", e)
        return []


async def _load_schema_audits(workspace_id: str) -> List[Dict[str, Any]]:
    try:
        return await fetch_all(
            "SELECT page_url, is_competitor, schema_types, raw_jsonld, "
            "diagnosis FROM schema_audits WHERE workspace_id = ?",
            (workspace_id,),
        ) or []
    except Exception as e:
        logger.warning("entity_consistency: schema_audits read failed: %s", e)
        return []


def _flatten_jsonld(blob: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(blob, list):
        for item in blob:
            out.extend(_flatten_jsonld(item))
    elif isinstance(blob, dict):
        out.append(blob)
        for v in blob.values():
            if isinstance(v, (list, dict)):
                out.extend(_flatten_jsonld(v))
    return out


def _extract_axis_value(jsonld_objects: List[Dict[str, Any]], axis: str) -> List[str]:
    keys = _JSONLD_KEYS.get(axis, ())
    values: List[str] = []
    for obj in jsonld_objects:
        for k in keys:
            if k not in obj:
                continue
            v = obj[k]
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, (str, int, float)):
                        values.append(str(item))
                    elif isinstance(item, dict):
                        name = item.get("name") or item.get("@id") or item.get("url")
                        if name:
                            values.append(str(name))
            elif isinstance(v, dict):
                name = v.get("name") or v.get("@id") or v.get("url")
                if name:
                    values.append(str(name))
            elif isinstance(v, (str, int, float)):
                values.append(str(v))
    # dedupe preserving order
    seen, out = set(), []
    for v in values:
        n = _norm(v)
        if n and n not in seen:
            seen.add(n)
            out.append(v)
    return out


# ─── AXIS EVALUATION ────────────────────────────────────────────────────────

def _evaluate_brand_name(
    ws: Dict[str, Any], aliases: List[Dict[str, Any]],
    schema_vals: Dict[str, List[str]],
) -> Dict[str, Any]:
    canonical = _norm(ws.get("brand_name") or ws.get("name") or "")
    statuses: Dict[str, str] = {}
    inconsistencies: List[str] = []
    values_seen: List[Tuple[str, str]] = []

    if canonical:
        statuses["website"] = "consistent"
        values_seen.append(("website", canonical))
    else:
        statuses["website"] = "missing"

    schema_brand = schema_vals.get("brand_name") or []
    if schema_brand:
        match = any(_norm(v) == canonical for v in schema_brand) if canonical else False
        statuses["schema"] = "consistent" if match else "inconsistent"
        for v in schema_brand:
            values_seen.append(("schema", v))
        if not match and canonical:
            inconsistencies.append(
                f"Schema brand variants ({', '.join(schema_brand)}) differ from '{ws.get('brand_name','')}'"
            )
    else:
        statuses["schema"] = "missing"

    if aliases:
        non_matching = [
            a["alias"] for a in aliases
            if canonical and _norm(a.get("normalized_alias") or a.get("alias")) != canonical
        ]
        statuses["directory"] = "inconsistent" if non_matching else "consistent"
        if non_matching:
            inconsistencies.append(
                f"{len(non_matching)} alias variants on record (e.g. {', '.join(non_matching[:3])})"
            )
        for a in aliases[:25]:
            values_seen.append(("directory", a["alias"]))
    else:
        statuses["directory"] = "missing"

    fix = (
        "Pin one canonical brand name across <title>, JSON-LD `name`, GBP, "
        "and review-platform profiles."
        if any(s != "consistent" for s in statuses.values())
        else "Brand name is consistent across observed surfaces."
    )
    return {
        "statuses_per_source": statuses,
        "inconsistencies": inconsistencies,
        "values_seen": values_seen,
        "recommended_fix": fix,
    }


def _evaluate_schema_axis(
    axis: str, schema_vals: Dict[str, List[str]], have_schema: bool,
) -> Dict[str, Any]:
    """Generic evaluator for axes whose truth lives in JSON-LD."""
    statuses: Dict[str, str] = {}
    inconsistencies: List[str] = []
    values_seen: List[Tuple[str, str]] = []
    vals = schema_vals.get(axis) or []

    if not have_schema:
        statuses["schema"] = "missing"
        return {
            "statuses_per_source": statuses,
            "inconsistencies": inconsistencies,
            "values_seen": values_seen,
            "recommended_fix": (
                f"No JSON-LD captured yet for {axis} — run the schema audit "
                "on your primary pages."
            ),
        }

    if not vals:
        statuses["schema"] = "missing"
        inconsistencies.append(f"No `{axis}` field present in any captured JSON-LD block.")
        return {
            "statuses_per_source": statuses,
            "inconsistencies": inconsistencies,
            "values_seen": values_seen,
            "recommended_fix": f"Add `{axis}` to your Organization / LocalBusiness JSON-LD.",
        }

    unique = {_norm(v) for v in vals if _norm(v)}
    for v in vals[:25]:
        values_seen.append(("schema", v))
    if len(unique) <= 1:
        statuses["schema"] = "consistent"
        fix = f"`{axis}` is uniform across captured schema."
    else:
        statuses["schema"] = "inconsistent"
        inconsistencies.append(
            f"{len(unique)} distinct `{axis}` values across pages: {', '.join(list(unique)[:3])}"
        )
        fix = f"Standardize `{axis}` to a single canonical value across all JSON-LD blocks."
    return {
        "statuses_per_source": statuses,
        "inconsistencies": inconsistencies,
        "values_seen": values_seen,
        "recommended_fix": fix,
    }


def _score_axis(axis_result: Dict[str, Any]) -> float:
    statuses = list(axis_result.get("statuses_per_source", {}).values())
    if not statuses:
        return 0.0
    weights = {"consistent": 100.0, "inconsistent": 40.0, "missing": 0.0}
    return sum(weights.get(s, 0.0) for s in statuses) / float(len(statuses))


# ─── PERSISTENCE ────────────────────────────────────────────────────────────

async def _persist_observations(
    workspace_id: str, axes: List[Dict[str, Any]], confidence: str,
) -> None:
    for axis in axes:
        ax_name = axis["axis"]
        statuses = axis.get("statuses_per_source", {})
        values = axis.get("values_seen") or []
        notes = "; ".join(axis.get("inconsistencies") or [])[:500]
        per_source_values: Dict[str, List[str]] = {}
        for src, val in values:
            per_source_values.setdefault(src, []).append(val)
        for source, status in statuses.items():
            vals = per_source_values.get(source) or [""]
            for v in vals[:10]:
                try:
                    await execute(
                        "INSERT INTO entity_consistency_results "
                        "(id, workspace_id, axis, source, value, status, notes, confidence) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (gen_id("ec-"), workspace_id, ax_name, source,
                         str(v)[:500], status, notes, confidence),
                    )
                except Exception as e:
                    logger.debug("entity_consistency persist row failed: %s", e)


# ─── PUBLIC API ─────────────────────────────────────────────────────────────

async def check_workspace(workspace_id: str) -> Dict[str, Any]:
    """Scan available surfaces, score each axis, return a shaped audit. Never
    raises."""
    try:
        ws = await _load_workspace(workspace_id)
        aliases = await _load_aliases(workspace_id)
        audits = await _load_schema_audits(workspace_id)
    except Exception as e:
        logger.warning("entity_consistency.check_workspace fatal-load: %s", e)
        ws, aliases, audits = {}, [], []

    if not ws and not aliases and not audits:
        return {
            "workspace_id": workspace_id,
            "axes": [],
            "overall_score": 0,
            "confidence": "needs_review",
            "observed_at": _now_iso(),
            "disclaimer": (
                "Entity consistency needs at least one workspace, alias, or "
                "schema audit. None found — run a schema audit first."
            ),
        }

    have_schema = bool(audits)
    all_jsonld: List[Dict[str, Any]] = []
    for a in audits:
        raw = a.get("raw_jsonld")
        if isinstance(raw, str):
            raw = from_json(raw, [])
        if raw:
            all_jsonld.extend(_flatten_jsonld(raw))

    schema_vals = {axis: _extract_axis_value(all_jsonld, axis) for axis in _AXES}
    confidence = "estimated" if (have_schema and ws) else "needs_review"

    axes_out: List[Dict[str, Any]] = []
    for axis in _AXES:
        ev = (_evaluate_brand_name(ws, aliases, schema_vals)
              if axis == "brand_name"
              else _evaluate_schema_axis(axis, schema_vals, have_schema))
        ev["axis"] = axis
        ev["confidence"] = confidence
        ev["score"] = round(_score_axis(ev), 1)
        axes_out.append(ev)

    overall = (round(sum(a["score"] for a in axes_out) / float(len(axes_out)))
               if axes_out else 0)

    try:
        await _persist_observations(workspace_id, axes_out, confidence)
    except Exception as e:
        logger.warning("entity_consistency persist failed: %s", e)

    return {
        "workspace_id": workspace_id,
        "axes": axes_out,
        "overall_score": overall,
        "confidence": confidence,
        "observed_at": _now_iso(),
        "disclaimer": (
            "Confidence labels reflect the freshness and breadth of observed "
            "surfaces. Re-run after each schema or directory update."
        ),
    }


async def list_recent(workspace_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Most recent observation rows. Defensive on bad limits."""
    try:
        n = max(1, min(int(limit or 50), 500))
    except (TypeError, ValueError):
        n = 50
    try:
        return await fetch_all(
            "SELECT id, workspace_id, axis, source, value, status, notes, "
            "confidence, observed_at FROM entity_consistency_results "
            "WHERE workspace_id = ? ORDER BY observed_at DESC LIMIT ?",
            (workspace_id, n),
        ) or []
    except Exception as e:
        logger.warning("entity_consistency.list_recent failed: %s", e)
        return []


__all__ = ["init", "check_workspace", "list_recent"]
