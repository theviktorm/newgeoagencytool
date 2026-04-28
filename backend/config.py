"""
Momentus AI — Configuration Management
Loads from environment variables and .env file.
All secrets stored here, never in frontend.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Central configuration — every setting the engine needs."""

    # ── Project ──
    project_name: str = "Momentus AI"
    project_id: str = "default"
    debug: bool = False

    # ── Server ──
    host: str = "0.0.0.0"
    port: int = 8100
    cors_origins: str = "*"  # comma-separated in env

    # ── Database ──
    database_url: str = Field(
        default="sqlite+aiosqlite:///momentus.db",
        description="SQLite path or Postgres connection string",
    )
    db_path: str = "momentus.db"  # raw path for aiosqlite

    # ── Peec (legacy REST) ──
    peec_api_key: str = ""
    peec_api_base_url: str = "https://api.peec.ai/v1"
    peec_api_timeout: int = 30

    # ── Peec MCP (preferred — live, sentiment, competitors) ──
    peec_mcp_url: str = "https://api.peec.ai/mcp"
    peec_mcp_token: str = ""              # legacy fallback bearer (Peec now uses OAuth)
    peec_mcp_timeout: int = 30
    peec_mcp_auto_sync_minutes: int = 0   # 0 = manual only, >0 = background poll cadence
    # Public-facing base URL of *this* backend, used to build the OAuth
    # redirect URI registered with Peec. Override via GEO_PUBLIC_BASE_URL.
    public_base_url: str = "http://localhost:8000"

    # ── Anthropic / Claude ──
    anthropic_api_key: str = ""
    claude_default_model: str = "claude-sonnet-4-20250514"
    claude_max_tokens: int = 4096
    claude_default_temperature: float = 0.7
    claude_daily_cost_limit: float = 50.0  # USD
    claude_monthly_cost_limit: float = 500.0  # USD

    # ── Scraping ──
    scraper_timeout: int = 20
    scraper_max_concurrent: int = 5
    scraper_retry_attempts: int = 3
    scraper_user_agent: str = (
        "Mozilla/5.0 (compatible; MomentusAI/1.0; +https://momentus.ai)"
    )
    scraper_respect_robots: bool = True

    # ── CMS: WordPress ──
    wordpress_url: str = ""
    wordpress_username: str = ""
    wordpress_app_password: str = ""

    # ── CMS: Webflow ──
    webflow_api_token: str = ""
    webflow_collection_id: str = ""

    # ── Batch Processing ──
    batch_max_concurrent: int = 10
    batch_poll_interval: int = 5  # seconds

    # ── Security ──
    api_secret_key: str = ""  # for signing internal tokens
    rate_limit_per_minute: int = 60

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_prefix = "GEO_"
        case_sensitive = False

    @property
    def cors_origin_list(self) -> list:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def has_peec_api(self) -> bool:
        return bool(self.peec_api_key and self.peec_api_base_url)

    @property
    def has_peec_mcp(self) -> bool:
        # Either OAuth-per-workspace (preferred — checked at runtime) or
        # the legacy static token fallback gives us a usable MCP setup.
        return bool(self.peec_mcp_url)

    @property
    def has_claude_api(self) -> bool:
        return bool(self.anthropic_api_key)

    @property
    def has_wordpress(self) -> bool:
        return bool(self.wordpress_url and self.wordpress_username and self.wordpress_app_password)

    @property
    def has_webflow(self) -> bool:
        return bool(self.webflow_api_token and self.webflow_collection_id)


# Singleton
settings = Settings()
