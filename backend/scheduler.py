"""
Momentus AI — Background Scheduler

Pure-asyncio scheduler. Walks every workspace on a configurable tick and
dispatches engine jobs at staggered cadences. State in `scheduler_state`.
Soft-fail: errors are logged at WARN and persisted; the loop keeps running.

Env:
    GEO_SCHEDULER_ENABLED       true/false. Default: true if /data exists.
    GEO_SCHEDULER_TICK_SECONDS  Default 300 (5 min).
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional

from .database import execute, fetch_all, fetch_one, get_db, to_json
from . import (
    ai_overview,
    attack_map,
    authority_score,
    prompt_tracker,
    reddit_engine,
)

logger = logging.getLogger("geo.scheduler")


# ── CONFIG ──
def _default_enabled() -> bool:
    """Auto-on when running in Railway (presence of /data volume)."""
    env = os.environ.get("GEO_SCHEDULER_ENABLED")
    if env is not None:
        return env.strip().lower() in ("1", "true", "yes", "on")
    return os.path.isdir("/data")


ENABLED = _default_enabled()
TICK_SECONDS = int(os.environ.get("GEO_SCHEDULER_TICK_SECONDS", "300"))


# ═══════════════════════════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════════════════════════

_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS scheduler_state (
    job_name        TEXT NOT NULL,
    workspace_id    TEXT NOT NULL,
    last_run_at     TEXT,
    last_status     TEXT DEFAULT '',
    last_error      TEXT DEFAULT '',
    next_run_at     TEXT,
    PRIMARY KEY (job_name, workspace_id)
);
CREATE INDEX IF NOT EXISTS idx_sched_next ON scheduler_state(next_run_at);
"""


async def _ensure_schema() -> None:
    db = await get_db()
    await db.executescript(_STATE_SCHEMA)
    await db.commit()


# ═══════════════════════════════════════════════════════════════
# JOB REGISTRY
# ═══════════════════════════════════════════════════════════════

JobFn = Callable[[str], Awaitable[Any]]


def _wrap(fn: Callable[..., Awaitable[Any]], **kwargs: Any) -> JobFn:
    async def _run(workspace_id: str) -> Any:
        return await fn(workspace_id, **kwargs)
    return _run


JOBS: Dict[str, Dict[str, Any]] = {
    "track_prompts": {"fn": _wrap(prompt_tracker.track_workspace, only_high_value=True, max_prompts=30), "interval_h": 6, "offset_h": 0},
    "rebuild_authority": {"fn": _wrap(authority_score.rebuild_all), "interval_h": 24, "offset_h": 0},
    "analyze_attack_map": {"fn": _wrap(attack_map.analyze_all_known, max_competitors=8), "interval_h": 168, "offset_h": 0},
    "detect_aio_movements": {"fn": _wrap(ai_overview.detect_movements), "interval_h": 24, "offset_h": 12},
    "harvest_reddit": {"fn": _wrap(reddit_engine.harvest_workspace), "interval_h": 24, "offset_h": 12},
}


# ═══════════════════════════════════════════════════════════════
# STATE HELPERS
# ═══════════════════════════════════════════════════════════════

def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _parse(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


async def _get_state(job_name: str, workspace_id: str) -> Optional[Dict[str, Any]]:
    return await fetch_one(
        "SELECT * FROM scheduler_state WHERE job_name = ? AND workspace_id = ?",
        (job_name, workspace_id),
    )


async def _upsert_state(
    job_name: str,
    workspace_id: str,
    last_run_at: str,
    last_status: str,
    last_error: str,
    next_run_at: str,
) -> None:
    await execute(
        """INSERT INTO scheduler_state
           (job_name, workspace_id, last_run_at, last_status, last_error, next_run_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(job_name, workspace_id) DO UPDATE SET
             last_run_at = excluded.last_run_at,
             last_status = excluded.last_status,
             last_error  = excluded.last_error,
             next_run_at = excluded.next_run_at""",
        (job_name, workspace_id, last_run_at, last_status, last_error, next_run_at),
    )


# ═══════════════════════════════════════════════════════════════
# EXECUTION
# ═══════════════════════════════════════════════════════════════

async def _run_one(job_name: str, workspace_id: str) -> Dict[str, Any]:
    spec = JOBS[job_name]
    fn: JobFn = spec["fn"]
    interval_h: int = spec["interval_h"]
    now = datetime.utcnow()
    next_at = now + timedelta(hours=interval_h)
    status = "ok"
    error = ""
    try:
        await fn(workspace_id)
    except Exception as exc:  # noqa: BLE001 — soft-fail
        status = "error"
        error = f"{type(exc).__name__}: {exc}"
        logger.warning("scheduler job %s failed for ws=%s: %s", job_name, workspace_id, error)
    await _upsert_state(job_name, workspace_id, _iso(now), status, error, _iso(next_at))
    return {"job": job_name, "workspace_id": workspace_id, "status": status, "error": error}


async def _due(job_name: str, workspace_id: str, now: datetime) -> bool:
    spec = JOBS[job_name]
    state = await _get_state(job_name, workspace_id)
    if not state:
        offset_h = int(spec.get("offset_h", 0))
        # First run: respect offset to stagger
        if offset_h <= 0:
            return True
        # Seed state with a deferred first run
        seed_at = now + timedelta(hours=offset_h)
        await _upsert_state(job_name, workspace_id, "", "pending", "", _iso(seed_at))
        return False
    next_at = _parse(state.get("next_run_at"))
    if next_at is None:
        return True
    return now >= next_at


async def _tick() -> None:
    workspaces = await fetch_all("SELECT id FROM workspaces WHERE is_active = 1")
    if not workspaces:
        return
    now = datetime.utcnow()
    for ws in workspaces:
        ws_id = ws["id"]
        for job_name in JOBS:
            try:
                if await _due(job_name, ws_id, now):
                    await _run_one(job_name, ws_id)
            except Exception:
                logger.exception("scheduler tick error: job=%s ws=%s", job_name, ws_id)


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

_task: Optional[asyncio.Task] = None


async def _loop() -> None:
    await _ensure_schema()
    logger.info("scheduler loop started (tick=%ss, jobs=%s)", TICK_SECONDS, list(JOBS))
    while True:
        try:
            await _tick()
        except Exception:
            logger.exception("scheduler tick crashed")
        await asyncio.sleep(TICK_SECONDS)


async def start_scheduler() -> None:
    """Start the background scheduler if enabled. Idempotent."""
    global _task
    if not ENABLED:
        logger.info("scheduler disabled (GEO_SCHEDULER_ENABLED=false, no /data)")
        return
    if _task and not _task.done():
        return
    await _ensure_schema()
    _task = asyncio.create_task(_loop())


async def stop_scheduler() -> None:
    """Cancel the background loop. Safe to call multiple times."""
    global _task
    if _task and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
    _task = None


async def list_runs(workspace_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Return scheduler_state rows for a workspace, newest run first."""
    return await fetch_all(
        "SELECT * FROM scheduler_state WHERE workspace_id = ? "
        "ORDER BY COALESCE(last_run_at, '') DESC LIMIT ?",
        (workspace_id, limit),
    )


async def run_job_now(job_name: str, workspace_id: str) -> Dict[str, Any]:
    """Trigger a single job immediately for a workspace."""
    if job_name not in JOBS:
        return {"ok": False, "error": f"unknown job: {job_name}", "available": list(JOBS)}
    await _ensure_schema()
    result = await _run_one(job_name, workspace_id)
    return {"ok": result["status"] == "ok", **result}
