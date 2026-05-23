"""
geo-engine — backend smoke suite.

Purpose: catch import-time and wiring crashes before they reach production.
A real outage happened because a startup path referenced a name that didn't
exist and no test ever imported / ran the app. These tests:

  1. Import every backend module (catches ImportError / SyntaxError / bad
     top-level references).
  2. Assert the FastAPI app wires up its full route table and that the
     business-critical paths are registered.
  3. Exercise pure helper functions that need no DB or network.

The DB lifespan itself is exercised by tests/test_startup.py — that is the
real anti-outage guard.

Run:
    cd /Users/viktormozsa/geo-engine && python3 -m pytest tests/ -q

Note: the optional `mcp` package needs Python 3.10+. Where it is absent, the
modules that depend on it (momentus_mcp_server, peec_mcp_client) are xfailed
on import rather than failing the suite.
"""
from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest


# ── Build the backend module list by scanning backend/*.py ──────────
# Discovering modules dynamically means a newly-added backend module is
# automatically import-tested without editing this file.
_BACKEND_DIR = Path(__file__).resolve().parent.parent / "backend"
BACKEND_MODULES = sorted(
    p.stem
    for p in _BACKEND_DIR.glob("*.py")
    if p.stem != "__init__"
)

# Modules that legitimately need the optional `mcp` package (Python 3.10+).
# On a host without `mcp`, importing these raises ImportError that mentions
# 'mcp'; we xfail those rather than failing the suite.
_MCP_DEPENDENT = {"momentus_mcp_server", "peec_mcp_client"}


# ═══════════════════════════════════════════════════════════════
# TEST 1 — every backend module imports cleanly
# ═══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("mod_name", BACKEND_MODULES)
def test_module_imports(mod_name):
    """backend.<module> imports without ImportError.

    Modules that depend on the optional `mcp` package are skipped/xfailed
    gracefully when `mcp` is unavailable (Python < 3.10). Every other import
    failure (ImportError, SyntaxError, AttributeError at import time) is a
    real failure and surfaces here.
    """
    full = f"backend.{mod_name}"
    try:
        importlib.import_module(full)
    except ImportError as exc:
        msg = str(exc).lower()
        if mod_name in _MCP_DEPENDENT and "mcp" in msg:
            pytest.skip(f"{full} requires optional 'mcp' package: {exc}")
        raise


# ═══════════════════════════════════════════════════════════════
# TEST 2 — FastAPI app exposes its full route table
# ═══════════════════════════════════════════════════════════════

def test_app_routes_and_critical_paths():
    """backend.main.app has >150 routes and every business-critical path is
    registered. A typo'd or removed route fails here instead of in prod."""
    main = importlib.import_module("backend.main")
    app = getattr(main, "app", None)
    assert app is not None, "backend.main must expose `app`"

    assert len(app.routes) > 150, (
        f"expected >150 routes, got {len(app.routes)}"
    )

    paths = {getattr(r, "path", None) for r in app.routes}
    paths.discard(None)

    for required in (
        "/api/health",
        "/api/health/deep",
        "/api/actions/{ws_id}/harvest",
        "/api/backtest/{ws_id}/project",
        "/api/integrations/status",
        "/badge/{slug}.svg",
        "/api/prompts/{ws_id}/battlefield",
    ):
        assert required in paths, f"missing critical route {required}"


# ═══════════════════════════════════════════════════════════════
# TEST 3 — pure functions (no DB / no network)
# ═══════════════════════════════════════════════════════════════

def test_classify_prompt_heuristic_high_value():
    """A clearly commercial, local, high-intent query scores high."""
    prompt_engine = importlib.import_module("backend.prompt_engine")
    result = prompt_engine.classify_prompt_heuristic(
        "best private surgeon Budapest"
    )
    assert isinstance(result, dict)
    assert result["revenue_score"] > 50, result


def test_authority_weights_sum_to_one():
    """authority_score.WEIGHTS must sum to ~1.0 so the weighted blend stays
    inside 0..100."""
    authority_score = importlib.import_module("backend.authority_score")
    weights = authority_score.WEIGHTS
    assert abs(sum(weights.values()) - 1.0) < 0.01, sum(weights.values())
