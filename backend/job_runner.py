"""
Momentus AI — Single-Worker Job Runner

In-DB job queue backed by the existing `job_queue` table (defined in
automation.py). One worker coroutine pops `status='queued'` rows in FIFO
order and dispatches to the appropriate engine. Serial execution keeps
Claude rate-limit pressure predictable.

Supported job types:
    track_workspace        -> prompt_tracker.track_workspace
    rebuild_authority      -> authority_score.rebuild_all
    analyze_attack_map     -> attack_map.analyze_all_known
    detect_aio_movements   -> ai_overview.detect_movements
    harvest_reddit         -> reddit_engine.harvest_workspace
    diagnose_workspace     -> citation_intelligence.diagnose_workspace
    audit_workspace_schema -> schema_engine.audit_workspace
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from .database import execute, fetch_all, fetch_one, from_json, gen_id, to_json
from . import (
    ai_overview,
    attack_map,
    authority_score,
    citation_intelligence,
    prompt_tracker,
    reddit_engine,
    schema_engine,
)

logger = logging.getLogger("geo.job_runner")


# ═══════════════════════════════════════════════════════════════
# HANDLER REGISTRY
# ═══════════════════════════════════════════════════════════════

Handler = Callable[[str, Dict[str, Any]], Awaitable[Any]]


async def _h_track_workspace(workspace_id: str, payload: Dict[str, Any]) -> Any:
    return await prompt_tracker.track_workspace(
        workspace_id,
        only_high_value=bool(payload.get("only_high_value", True)),
        max_prompts=int(payload.get("max_prompts", 50)),
        models=payload.get("models"),
    )


async def _h_rebuild_authority(workspace_id: str, payload: Dict[str, Any]) -> Any:
    return await authority_score.rebuild_all(workspace_id)


async def _h_analyze_attack_map(workspace_id: str, payload: Dict[str, Any]) -> Any:
    return await attack_map.analyze_all_known(
        workspace_id,
        max_competitors=int(payload.get("max_competitors", 12)),
    )


async def _h_detect_aio_movements(workspace_id: str, payload: Dict[str, Any]) -> Any:
    return await ai_overview.detect_movements(workspace_id)


async def _h_harvest_reddit(workspace_id: str, payload: Dict[str, Any]) -> Any:
    return await reddit_engine.harvest_workspace(workspace_id)


async def _h_diagnose_workspace(workspace_id: str, payload: Dict[str, Any]) -> Any:
    return await citation_intelligence.diagnose_workspace(
        workspace_id,
        top_n=int(payload.get("top_n", 10)),
    )


async def _h_audit_workspace_schema(workspace_id: str, payload: Dict[str, Any]) -> Any:
    return await schema_engine.audit_workspace(
        workspace_id,
        top_n=int(payload.get("top_n", 20)),
    )


HANDLERS: Dict[str, Handler] = {
    "track_workspace": _h_track_workspace,
    "rebuild_authority": _h_rebuild_authority,
    "analyze_attack_map": _h_analyze_attack_map,
    "detect_aio_movements": _h_detect_aio_movements,
    "harvest_reddit": _h_harvest_reddit,
    "diagnose_workspace": _h_diagnose_workspace,
    "audit_workspace_schema": _h_audit_workspace_schema,
}


# ═══════════════════════════════════════════════════════════════
# QUEUE OPERATIONS — reuse existing job_queue table
# Columns we touch: id, workspace_id, job_type, params(JSON), status,
# result(JSON), error, started_at, completed_at, created_at.
# ═══════════════════════════════════════════════════════════════

async def enqueue(workspace_id: str, job_type: str, payload: Dict[str, Any]) -> str:
    """Push a job onto the queue. Returns the new job id."""
    if job_type not in HANDLERS:
        raise ValueError(f"unknown job_type: {job_type}. known: {list(HANDLERS)}")
    job_id = gen_id("job-")
    await execute(
        "INSERT INTO job_queue (id, workspace_id, job_type, params, status) "
        "VALUES (?, ?, ?, ?, 'queued')",
        (job_id, workspace_id, job_type, to_json(payload or {})),
    )
    logger.info("enqueued job %s type=%s ws=%s", job_id, job_type, workspace_id)
    return job_id


async def list_jobs(workspace_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """List recent jobs for a workspace, newest first."""
    return await fetch_all(
        "SELECT * FROM job_queue WHERE workspace_id = ? "
        "ORDER BY created_at DESC LIMIT ?",
        (workspace_id, limit),
    )


async def cancel(job_id: str) -> bool:
    """Cancel a queued job. Running jobs are NOT interrupted (serial worker)."""
    rows = await execute(
        "UPDATE job_queue SET status = 'cancelled', completed_at = datetime('now') "
        "WHERE id = ? AND status = 'queued'",
        (job_id,),
    )
    return rows > 0


# ═══════════════════════════════════════════════════════════════
# WORKER LOOP
# ═══════════════════════════════════════════════════════════════

_worker_task: Optional[asyncio.Task] = None
_POLL_SECONDS = 5


async def _claim_next() -> Optional[Dict[str, Any]]:
    """Atomically pull the oldest queued job and mark it running."""
    row = await fetch_one(
        "SELECT id, workspace_id, job_type, params FROM job_queue "
        "WHERE status = 'queued' "
        "AND (scheduled_for IS NULL OR scheduled_for = '' OR scheduled_for <= datetime('now')) "
        "ORDER BY priority ASC, created_at ASC LIMIT 1"
    )
    if not row:
        return None
    updated = await execute(
        "UPDATE job_queue SET status = 'running', started_at = datetime('now'), "
        "assigned_to = 'job_runner' WHERE id = ? AND status = 'queued'",
        (row["id"],),
    )
    if updated == 0:
        # Lost the race; another worker grabbed it. Retry next tick.
        return None
    return row


async def _execute_job(job: Dict[str, Any]) -> None:
    job_id = job["id"]
    job_type = job["job_type"]
    workspace_id = job["workspace_id"]
    payload = from_json(job.get("params") or "{}", {}) or {}

    handler = HANDLERS.get(job_type)
    if handler is None:
        await execute(
            "UPDATE job_queue SET status = 'failed', error = ?, completed_at = datetime('now') "
            "WHERE id = ?",
            (f"unknown job_type: {job_type}", job_id),
        )
        return

    logger.info("running job %s type=%s ws=%s", job_id, job_type, workspace_id)
    try:
        result = await handler(workspace_id, payload)
        await execute(
            "UPDATE job_queue SET status = 'done', result = ?, completed_at = datetime('now') "
            "WHERE id = ?",
            (to_json(result if result is not None else {}), job_id),
        )
        logger.info("job %s done", job_id)
    except asyncio.CancelledError:
        await execute(
            "UPDATE job_queue SET status = 'cancelled', completed_at = datetime('now') "
            "WHERE id = ?",
            (job_id,),
        )
        raise
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        logger.warning("job %s failed: %s", job_id, err)
        await execute(
            "UPDATE job_queue SET status = 'failed', error = ?, completed_at = datetime('now') "
            "WHERE id = ?",
            (err, job_id),
        )


async def _worker_loop() -> None:
    logger.info("job_runner worker loop started (poll=%ss, handlers=%s)",
                _POLL_SECONDS, list(HANDLERS))
    while True:
        try:
            job = await _claim_next()
            if job is None:
                await asyncio.sleep(_POLL_SECONDS)
                continue
            await _execute_job(job)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("worker loop iteration crashed")
            await asyncio.sleep(_POLL_SECONDS)


async def start_worker() -> None:
    """Start the single background worker. Idempotent."""
    global _worker_task
    if _worker_task and not _worker_task.done():
        return
    _worker_task = asyncio.create_task(_worker_loop())


async def stop_worker() -> None:
    """Stop the background worker. Safe to call multiple times."""
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    _worker_task = None
