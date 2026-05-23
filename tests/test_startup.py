"""
geo-engine — startup / lifespan regression guard.

THIS IS THE ANTI-OUTAGE TEST.

A production outage happened because the app's startup path referenced a name
that did not exist (e.g. `job_runner.init`) and NO test ever ran the lifespan,
so the broken reference shipped and crash-looped the container.

`test_full_lifespan_against_temp_db` runs the REAL `startup()` against a
throwaway SQLite DB and then hits /api/health over ASGI. If `startup()` raises
anything (AttributeError from a bad init reference, a missing function, a
schema crash that isn't swallowed, ...), this test FAILS — turning a prod
outage into a red CI run.

The DB path is redirected to a temp file via GEO_DB_PATH *before* any backend
module is imported, so the real production momentus.db is never touched.
"""
from __future__ import annotations

import importlib
import os
import tempfile

import pytest


# ── Redirect the DB to a throwaway temp file BEFORE importing backend ──
# config.Settings reads GEO_DB_PATH at import time (settings is a module-level
# singleton), so this env var must be set before `backend.config` is imported
# anywhere. Setting it at module import of this test file guarantees that for
# every test here.
_TMP_DB_FD, _TMP_DB_PATH = tempfile.mkstemp(prefix="geo_test_", suffix=".db")
os.close(_TMP_DB_FD)
os.environ["GEO_DB_PATH"] = _TMP_DB_PATH


def _import_main():
    """Import backend.main fresh, ensuring config picked up the temp DB path."""
    main = importlib.import_module("backend.main")
    return main


# ═══════════════════════════════════════════════════════════════
# THE CRITICAL TEST — run the full lifespan, hit health.
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_full_lifespan_against_temp_db():
    """Run startup() -> health -> shutdown() against a throwaway DB.

    Regression guard: if startup() raises (e.g. AttributeError from a bad
    init reference like `job_runner.init`), this test fails and CI goes red
    before the bad commit can reach production.
    """
    import httpx

    main = _import_main()

    # Sanity: the temp DB must be in use, never the production file.
    from backend.config import settings
    assert settings.db_path == _TMP_DB_PATH, (
        f"temp DB not in effect: {settings.db_path!r}"
    )

    # 1) The full startup lifespan must complete without raising. THIS is the
    #    line that would have caught the `job_runner.init` outage.
    await main.startup()

    # 2) The app must actually serve /api/health 200 with a JSON status field.
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        resp = await client.get("/api/health")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert "status" in body, body

    # 3) Shutdown must also complete cleanly.
    await main.shutdown()


# ═══════════════════════════════════════════════════════════════
# Lightweight static check: every name the startup init list references
# must resolve on its module (or be guarded by getattr).
# ═══════════════════════════════════════════════════════════════

def test_startup_init_references_resolve():
    """Every background-init reference used by startup() must exist on its
    module, OR be resolved defensively via getattr.

    The lifespan test above is the real guard; this is a fast, no-DB tripwire
    that points straight at the offending name when a reference goes stale.
    """
    main = _import_main()

    from backend import (
        brand_resolver, entity_graph, alert_engine, job_runner,
        scheduler, action_engine, backtest,
    )

    # Modules whose init() is resolved via getattr(mod, "init", None) in
    # startup() — these are allowed to NOT have an init().
    getattr_guarded = (
        ("brand_resolver", brand_resolver),
        ("entity_graph", entity_graph),
        ("alert_engine", alert_engine),
        ("action_engine", action_engine),
        ("backtest", backtest),
    )
    for name, mod in getattr_guarded:
        fn = getattr(mod, "init", None)
        # getattr-guarded: either a callable init or explicitly None.
        assert fn is None or callable(fn), f"{name}.init exists but isn't callable"

    # Names startup() calls DIRECTLY (not via getattr) MUST exist — a stale
    # reference here is exactly the class of bug that caused the outage.
    assert callable(getattr(job_runner, "start_worker", None)), (
        "job_runner.start_worker missing — startup() calls it directly"
    )
    assert callable(getattr(scheduler, "start_scheduler", None)), (
        "scheduler.start_scheduler missing — startup() calls it directly"
    )

    # Core init helpers imported into main and awaited directly in startup().
    assert callable(getattr(main, "init_db", None)), "main.init_db missing"
    assert callable(getattr(main, "init_auth", None)), "main.init_auth missing"
