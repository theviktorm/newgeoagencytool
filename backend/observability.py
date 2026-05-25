"""
Momentus AI — Optional Observability Layer.

Zero-dependency-by-default: per-request structured logging + timing +
request IDs, optional JSON logs, optional Sentry. NEVER raises at import
or setup and NEVER breaks the app when Sentry is missing/unconfigured.

To enable Sentry (intentionally NOT in requirements.txt):
    pip install sentry-sdk  &&  export SENTRY_DSN="https://...@oX.ingest.sentry.io/Y"

Env vars consumed:
    GEO_LOG_FORMAT          "json" => single-line JSON logs (default: human)
    SENTRY_DSN / GEO_SENTRY_DSN   Sentry DSN (presence = try to enable Sentry)
    GEO_SENTRY_TRACES_RATE  float traces sample rate (default 0.0)
    GEO_ENV                 Sentry environment (default "production")
    GEO_RELEASE             Sentry release string (optional)
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid

# ──────────────────────────────────────────────────────────────────
# Module state — keeps setup_observability idempotent
# ──────────────────────────────────────────────────────────────────
_STATE = {"sentry": False, "structured_logs": False, "middleware": False, "logging": False}

_HEALTH_PATHS = {"/api/health"}


def get_logger(name: str) -> logging.Logger:
    """Convenience: return logging.getLogger('geo.<name>')."""
    return logging.getLogger(f"geo.{name}")


class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record (timestamp, level, logger, msg, exc)."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        rid = getattr(record, "request_id", None)
        if rid:
            payload["request_id"] = rid
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        try:
            return json.dumps(payload, default=str)
        except Exception:
            # Last-resort: never let logging itself blow up.
            return f'{{"level":"{record.levelname}","message":"{record.getMessage()!r}"}}'


def _install_json_logging() -> bool:
    """Attach the JSON formatter to the 'geo' logger family. Opt-in only."""
    if os.environ.get("GEO_LOG_FORMAT", "").strip().lower() != "json":
        return False
    geo_logger = logging.getLogger("geo")
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    # Replace inherited handlers for the geo family so output stays single-line.
    geo_logger.handlers = [handler]
    geo_logger.propagate = False
    if geo_logger.level == logging.NOTSET:
        geo_logger.setLevel(logging.INFO)
    return True


def _init_sentry() -> bool:
    """Initialize Sentry iff a DSN is present and sentry_sdk imports. Never raises."""
    dsn = (os.environ.get("SENTRY_DSN") or os.environ.get("GEO_SENTRY_DSN") or "").strip()
    if not dsn:
        return False
    try:
        import sentry_sdk  # lazy, optional
    except Exception:
        return False
    try:
        try:
            rate = float(os.environ.get("GEO_SENTRY_TRACES_RATE", "0.0"))
        except (TypeError, ValueError):
            rate = 0.0
        kwargs = {
            "dsn": dsn,
            "traces_sample_rate": rate,
            "environment": os.environ.get("GEO_ENV", "production"),
        }
        release = os.environ.get("GEO_RELEASE", "").strip()
        if release:
            kwargs["release"] = release
        # Add the FastAPI/ASGI integration if it's available in this sentry build.
        try:
            from sentry_sdk.integrations.fastapi import FastApiIntegration
            kwargs["integrations"] = [FastApiIntegration()]
        except Exception:
            pass
        sentry_sdk.init(**kwargs)
        return True
    except Exception:
        return False


def _capture_exception() -> None:
    """Forward an active exception to Sentry if available. Never raises."""
    try:
        import sentry_sdk
        sentry_sdk.capture_exception()
    except Exception:
        pass


def setup_observability(app) -> dict:
    """Idempotent. Wire structured logging, optional Sentry, and request middleware.

    Returns {"sentry": bool, "structured_logs": bool}. Never raises.
    """
    log = get_logger("observability")

    if not _STATE["logging"]:
        try:
            _STATE["structured_logs"] = _install_json_logging()
        except Exception:
            _STATE["structured_logs"] = False
        _STATE["logging"] = True

    if not _STATE["sentry"]:
        try:
            _STATE["sentry"] = _init_sentry()
        except Exception:
            _STATE["sentry"] = False

    if not _STATE["middleware"]:
        try:
            _register_middleware(app)
            _STATE["middleware"] = True
        except Exception:
            # If middleware registration fails (e.g. app already started), don't crash.
            pass

    try:
        log.info(
            "observability ready (sentry=%s structured_logs=%s project=%s)",
            _STATE["sentry"], _STATE["structured_logs"], _safe_project_name(),
        )
    except Exception:
        pass

    return {"sentry": _STATE["sentry"], "structured_logs": _STATE["structured_logs"]}


def _safe_project_name() -> str:
    try:
        from .config import settings
        return settings.project_name
    except Exception:
        return "?"


def _register_middleware(app) -> None:
    """Attach the request-timing / request-id / error-capturing HTTP middleware."""
    req_log = get_logger("request")

    @app.middleware("http")
    async def _observability_middleware(request, call_next):
        request_id = uuid.uuid4().hex[:12]
        path = request.url.path
        skip = path in _HEALTH_PATHS
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            req_log.error(
                "request failed method=%s path=%s duration_ms=%s request_id=%s",
                request.method, path, duration_ms, request_id,
                exc_info=True, extra={"request_id": request_id},
            )
            if _STATE["sentry"]:
                _capture_exception()
            raise
        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        try:
            response.headers["X-Request-ID"] = request_id
        except Exception:
            pass
        if not skip:
            req_log.info(
                "method=%s path=%s status=%s duration_ms=%s request_id=%s",
                request.method, path, getattr(response, "status_code", "?"),
                duration_ms, request_id, extra={"request_id": request_id},
            )
        return response
