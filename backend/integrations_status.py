"""
Momentus AI — Integration Status + Deep Health.

The engine silently no-ops when an API key is missing: no Perplexity key means
no live Perplexity tracking, no SerpAPI key means no Google AI Overview, etc.
Users never see *which* integrations are live. This module turns that hidden
state into one structured report so the UI can paint a green/red panel and
drive upsell ("connect Perplexity to unlock live tracking").

Detection precedence: `settings` attributes first (these come from the GEO_*
env prefix + .env), then a raw os.environ fallback for the keys that the
trackers read directly from the environment (OPENAI_API_KEY, PERPLEXITY_API_KEY,
GOOGLE_API_KEY / GEMINI_API_KEY, SERPAPI_KEY, plus an optional Slack webhook).

SECURITY: only booleans ever leave this module — never the secret values.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

from .config import settings
from .database import get_db

logger = logging.getLogger("geo.integrations")


# ═══════════════════════════════════════════════════════════════
# DETECTION HELPERS
# ═══════════════════════════════════════════════════════════════

def _settings_truthy(*attrs: str) -> bool:
    """True if any named settings attribute is set to a truthy value."""
    for attr in attrs:
        try:
            if bool(getattr(settings, attr, "")):
                return True
        except Exception:
            continue
    return False


def _env_truthy(*names: str) -> bool:
    """True if any named environment variable is set to a non-empty value."""
    for name in names:
        if (os.environ.get(name) or "").strip():
            return True
    return False


def _anthropic_configured() -> bool:
    # settings.anthropic_api_key (GEO_ANTHROPIC_API_KEY) or raw env fallbacks.
    return (
        bool(getattr(settings, "has_claude_api", False))
        or _settings_truthy("anthropic_api_key")
        or _env_truthy("ANTHROPIC_API_KEY", "GEO_ANTHROPIC_API_KEY")
    )


def _peec_mcp_configured() -> bool:
    # has_peec_mcp is True whenever an MCP URL exists; tighten by also accepting
    # the legacy static token or a configured public base URL for OAuth.
    return (
        bool(getattr(settings, "has_peec_mcp", False))
        or _settings_truthy("peec_mcp_url", "peec_mcp_token")
    )


def _peec_api_configured() -> bool:
    return bool(getattr(settings, "has_peec_api", False)) or _settings_truthy("peec_api_key")


def _openai_configured() -> bool:
    return _settings_truthy("openai_api_key") or _env_truthy("OPENAI_API_KEY")


def _perplexity_configured() -> bool:
    return _settings_truthy("perplexity_api_key") or _env_truthy("PERPLEXITY_API_KEY")


def _gemini_configured() -> bool:
    return (
        _settings_truthy("google_api_key", "gemini_api_key")
        or _env_truthy("GOOGLE_API_KEY", "GEMINI_API_KEY")
    )


def _serpapi_configured() -> bool:
    return _settings_truthy("serpapi_key") or _env_truthy("SERPAPI_KEY")


def _slack_configured() -> bool:
    # Slack is per-rule/per-workspace in alert_engine, but a global webhook may
    # be set on settings (alerts.py) or via the environment.
    return (
        _settings_truthy("alerts_slack_webhook_url", "slack_webhook_url")
        or _env_truthy("SLACK_WEBHOOK_URL", "ALERTS_SLACK_WEBHOOK_URL")
    )


def _wordpress_configured() -> bool:
    return bool(getattr(settings, "has_wordpress", False)) or _settings_truthy(
        "wordpress_url", "wordpress_username", "wordpress_app_password"
    )


def _webflow_configured() -> bool:
    return bool(getattr(settings, "has_webflow", False)) or _settings_truthy(
        "webflow_api_token", "webflow_collection_id"
    )


# ═══════════════════════════════════════════════════════════════
# INTEGRATION CATALOG
# Each entry is fully static metadata + a detector callable. We never put a
# secret in here; `configured` is computed at call time.
# ═══════════════════════════════════════════════════════════════

# (key, label, category, required, detector, hint, unlocks)
_CATALOG = [
    (
        "anthropic", "Claude (Anthropic)", "llm", True, _anthropic_configured,
        "Set ANTHROPIC_API_KEY / GEO_ANTHROPIC_API_KEY",
        "all AI diagnosis + content",
    ),
    (
        "peec_mcp", "Peec MCP", "data", True, _peec_mcp_configured,
        "Connect Peec via OAuth or set GEO_PEEC_MCP_TOKEN",
        "live visibility, sentiment + competitor data",
    ),
    (
        "peec_api", "Peec REST (legacy)", "data", False, _peec_api_configured,
        "Set GEO_PEEC_API_KEY",
        "legacy REST import fallback",
    ),
    (
        "openai", "ChatGPT (OpenAI)", "tracking", False, _openai_configured,
        "Set OPENAI_API_KEY",
        "live ChatGPT prompt tracking",
    ),
    (
        "perplexity", "Perplexity", "tracking", False, _perplexity_configured,
        "Set PERPLEXITY_API_KEY",
        "live Perplexity prompt tracking",
    ),
    (
        "gemini", "Gemini (Google)", "tracking", False, _gemini_configured,
        "Set GOOGLE_API_KEY / GEMINI_API_KEY",
        "live Gemini prompt tracking",
    ),
    (
        "serpapi", "Google AI Overview (SerpAPI)", "tracking", False, _serpapi_configured,
        "Set SERPAPI_KEY",
        "live Google AI Overview tracking",
    ),
    (
        "slack", "Slack", "notify", False, _slack_configured,
        "Set SLACK_WEBHOOK_URL or add a Slack channel to an alert rule",
        "Slack alert delivery",
    ),
    (
        "wordpress", "WordPress", "cms", False, _wordpress_configured,
        "Set GEO_WORDPRESS_URL / _USERNAME / _APP_PASSWORD",
        "one-click WordPress publishing",
    ),
    (
        "webflow", "Webflow", "cms", False, _webflow_configured,
        "Set GEO_WEBFLOW_API_TOKEN + GEO_WEBFLOW_COLLECTION_ID",
        "one-click Webflow publishing",
    ),
]

# Tracking integrations whose presence flips the engine from replay → live.
_LIVE_TRACKING_KEYS = ("openai", "perplexity", "gemini")


# ═══════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════

async def integration_status() -> Dict[str, Any]:
    """Structured report of every integration. Booleans only — no secrets.

    Returns:
        {
          "integrations": [{key, label, category, configured, required,
                            hint, unlocks}, ...],
          "summary": {"configured": N, "total": M, "required_missing": [keys]},
          "tracking_mode": "live" | "peec_replay",
        }
    """
    integrations: List[Dict[str, Any]] = []
    configured_count = 0
    required_missing: List[str] = []
    configured_by_key: Dict[str, bool] = {}

    for key, label, category, required, detector, hint, unlocks in _CATALOG:
        try:
            configured = bool(detector())
        except Exception as exc:  # detector must never break the panel
            logger.warning("integration detector %s failed: %s", key, exc)
            configured = False
        configured_by_key[key] = configured
        if configured:
            configured_count += 1
        if required and not configured:
            required_missing.append(key)
        integrations.append({
            "key": key,
            "label": label,
            "category": category,
            "configured": configured,
            "required": bool(required),
            "hint": hint,
            "unlocks": unlocks,
        })

    tracking_mode = (
        "live"
        if any(configured_by_key.get(k) for k in _LIVE_TRACKING_KEYS)
        else "peec_replay"
    )

    return {
        "integrations": integrations,
        "summary": {
            "configured": configured_count,
            "total": len(_CATALOG),
            "required_missing": required_missing,
        },
        "tracking_mode": tracking_mode,
    }


async def health_deep() -> Dict[str, Any]:
    """Deep health probe: DB connectivity, writability, table count, and the
    integration summary. Soft-fails to "error" with the exception string."""
    db_status = "ok"
    db_writable = False
    table_count = 0

    try:
        db = await get_db()
        cursor = await db.execute(
            "SELECT count(*) AS c FROM sqlite_master WHERE type='table'"
        )
        row = await cursor.fetchone()
        if row is not None:
            # Row factory is aiosqlite.Row; support index + key access.
            try:
                table_count = int(row["c"])
            except (KeyError, IndexError, TypeError):
                table_count = int(row[0])
        # Probe writability without leaving anything behind.
        try:
            await db.execute(
                "CREATE TEMP TABLE IF NOT EXISTS _health_write_probe (x INTEGER)"
            )
            await db.execute("DROP TABLE IF EXISTS _health_write_probe")
            db_writable = True
        except Exception as exc:
            logger.warning("db write probe failed: %s", exc)
            db_writable = False
    except Exception as exc:
        logger.warning("health_deep db check failed: %s", exc)
        db_status = "error: " + str(exc)

    summary: Dict[str, Any]
    try:
        status = await integration_status()
        summary = status["summary"]
    except Exception as exc:
        logger.warning("health_deep integration summary failed: %s", exc)
        summary = {"configured": 0, "total": len(_CATALOG), "required_missing": []}

    return {
        "db": db_status,
        "db_path": settings.db_path,
        "db_writable": db_writable,
        "tables": table_count,
        "integrations": summary,
    }


__all__ = ["integration_status", "health_deep"]
