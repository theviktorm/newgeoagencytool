"""
Momentus AI — Peec MCP OAuth 2.1 client (per-workspace)

Peec's MCP endpoint at https://api.peec.ai/mcp speaks OAuth 2.1 with
Dynamic Client Registration (RFC 7591) and PKCE (RFC 7636).

Flow per workspace:

  1. discover()           — fetch /.well-known/oauth-authorization-server/mcp
  2. ensure_client()      — DCR POST to registration_endpoint, persist creds
  3. start_authorization()— generate state + code_verifier, return authorize URL
  4. handle_callback()    — exchange code, persist access + refresh tokens
  5. get_valid_token()    — return live bearer; auto-refresh if expired

Tokens are stored in `peec_oauth_clients` keyed by (workspace_id, server_url).
The redirect URI MUST be registered with whatever public base URL the
backend runs on; configurable via GEO_PUBLIC_BASE_URL.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode, urlparse

import httpx

from .config import settings
from .database import execute, fetch_one, fetch_all

logger = logging.getLogger("geo.peec_oauth")

# How long before actual expiry we consider a token "stale" and refresh.
REFRESH_LEEWAY_SECONDS = 60
# How long an authorize state row is valid (cleaned up afterwards).
STATE_TTL_SECONDS = 600


# ═══════════════════════════════════════════════════════════════
# DISCOVERY
# ═══════════════════════════════════════════════════════════════

def _well_known_urls(server_url: str):
    """Yield candidate AS-metadata URLs in priority order."""
    p = urlparse(server_url)
    base = f"{p.scheme}://{p.netloc}"
    path = p.path.rstrip("/")
    # RFC 8414: path-aware first, then root.
    if path:
        yield f"{base}/.well-known/oauth-authorization-server{path}"
    yield f"{base}/.well-known/oauth-authorization-server"
    yield f"{base}{path}/.well-known/oauth-authorization-server"


async def discover(server_url: str) -> Dict[str, Any]:
    """Return AS metadata dict, or {} if none of the well-known URLs respond."""
    async with httpx.AsyncClient(timeout=15.0) as http:
        for url in _well_known_urls(server_url):
            try:
                r = await http.get(url)
                if r.status_code == 200:
                    data = r.json()
                    if "authorization_endpoint" in data and "token_endpoint" in data:
                        logger.info("Discovered Peec AS metadata at %s", url)
                        return data
            except Exception as e:
                logger.debug("Discover %s failed: %s", url, e)
    logger.warning("No AS metadata found for %s", server_url)
    return {}


# ═══════════════════════════════════════════════════════════════
# DYNAMIC CLIENT REGISTRATION
# ═══════════════════════════════════════════════════════════════

async def register_client(
    metadata: Dict[str, Any],
    redirect_uri: str,
    client_name: str = "Momentus AI",
) -> Dict[str, Any]:
    """RFC 7591 DCR. Returns the registration response dict (client_id,
    optionally client_secret, etc.)."""
    reg_endpoint = metadata.get("registration_endpoint")
    if not reg_endpoint:
        raise RuntimeError("Peec AS metadata is missing registration_endpoint")
    body = {
        "client_name": client_name,
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",  # public client + PKCE
        "application_type": "web",
    }
    async with httpx.AsyncClient(timeout=15.0) as http:
        r = await http.post(reg_endpoint, json=body)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"DCR failed: {r.status_code} {r.text}")
        return r.json()


# ═══════════════════════════════════════════════════════════════
# PKCE helpers
# ═══════════════════════════════════════════════════════════════

def _pkce_pair() -> Tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


# ═══════════════════════════════════════════════════════════════
# CLIENT REGISTRATION CACHE (per workspace + server)
# ═══════════════════════════════════════════════════════════════

async def _load_client_row(workspace_id: str, server_url: str) -> Optional[Dict[str, Any]]:
    return await fetch_one(
        "SELECT * FROM peec_oauth_clients WHERE workspace_id = ? AND server_url = ?",
        (workspace_id, server_url),
    )


async def _upsert_client_row(row: Dict[str, Any]) -> None:
    cols = [
        "workspace_id", "server_url", "issuer",
        "authorization_endpoint", "token_endpoint", "registration_endpoint",
        "client_id", "client_secret", "redirect_uri", "scope",
        "access_token", "refresh_token", "expires_at",
        "created_at", "updated_at",
    ]
    placeholders = ", ".join("?" for _ in cols)
    set_clause = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in ("workspace_id", "server_url", "created_at"))
    sql = (
        f"INSERT INTO peec_oauth_clients ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(workspace_id, server_url) DO UPDATE SET {set_clause}"
    )
    await execute(sql, tuple(row.get(c, "") or "" for c in cols))


async def ensure_client(
    workspace_id: str,
    server_url: str,
    redirect_uri: str,
) -> Dict[str, Any]:
    """Discover + register if needed. Returns the persisted client row."""
    existing = await _load_client_row(workspace_id, server_url)
    if existing and existing.get("client_id") and existing.get("redirect_uri") == redirect_uri:
        return existing

    metadata = await discover(server_url)
    if not metadata:
        raise RuntimeError(
            f"Peec MCP at {server_url} did not advertise OAuth metadata. "
            f"Cannot start authorization flow."
        )
    reg = await register_client(metadata, redirect_uri)

    now = datetime.now(timezone.utc).isoformat()
    row = {
        "workspace_id": workspace_id,
        "server_url": server_url,
        "issuer": metadata.get("issuer", ""),
        "authorization_endpoint": metadata.get("authorization_endpoint", ""),
        "token_endpoint": metadata.get("token_endpoint", ""),
        "registration_endpoint": metadata.get("registration_endpoint", ""),
        "client_id": reg.get("client_id", ""),
        "client_secret": reg.get("client_secret", "") or "",
        "redirect_uri": redirect_uri,
        "scope": reg.get("scope", "") or "",
        "access_token": existing.get("access_token", "") if existing else "",
        "refresh_token": existing.get("refresh_token", "") if existing else "",
        "expires_at": existing.get("expires_at") if existing else "",
        "created_at": existing.get("created_at") if existing else now,
        "updated_at": now,
    }
    await _upsert_client_row(row)
    return row


# ═══════════════════════════════════════════════════════════════
# AUTHORIZATION FLOW
# ═══════════════════════════════════════════════════════════════

async def start_authorization(
    workspace_id: str,
    server_url: str,
    redirect_uri: str,
) -> Dict[str, Any]:
    """Step 1 of the user-facing flow. Returns:
        {authorize_url, state}
    Caller (the API endpoint) sends authorize_url back to the browser; the
    user logs into Peec, Peec redirects back to redirect_uri with ?code+state.
    """
    row = await ensure_client(workspace_id, server_url, redirect_uri)

    state = secrets.token_urlsafe(24)
    verifier, challenge = _pkce_pair()
    await execute(
        "INSERT OR REPLACE INTO peec_oauth_states (state, workspace_id, server_url, code_verifier) "
        "VALUES (?, ?, ?, ?)",
        (state, workspace_id, server_url, verifier),
    )
    # Best-effort cleanup of stale state rows
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=STATE_TTL_SECONDS)).isoformat()
    try:
        await execute("DELETE FROM peec_oauth_states WHERE created_at < ?", (cutoff,))
    except Exception:
        pass

    params = {
        "response_type": "code",
        "client_id": row["client_id"],
        "redirect_uri": redirect_uri,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    if row.get("scope"):
        params["scope"] = row["scope"]
    url = f"{row['authorization_endpoint']}?{urlencode(params)}"
    return {"authorize_url": url, "state": state}


async def handle_callback(
    state: str,
    code: str,
    redirect_uri_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Step 2: exchange code for tokens, persist them, drop state."""
    state_row = await fetch_one(
        "SELECT * FROM peec_oauth_states WHERE state = ?",
        (state,),
    )
    if not state_row:
        raise RuntimeError("Unknown or expired OAuth state")

    workspace_id = state_row["workspace_id"]
    server_url = state_row["server_url"]
    verifier = state_row["code_verifier"]

    client = await _load_client_row(workspace_id, server_url)
    if not client:
        raise RuntimeError("OAuth client row missing — re-run /auth/start")

    redirect_uri = redirect_uri_override or client["redirect_uri"]

    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client["client_id"],
        "code_verifier": verifier,
    }
    if client.get("client_secret"):
        body["client_secret"] = client["client_secret"]

    async with httpx.AsyncClient(timeout=15.0) as http:
        r = await http.post(client["token_endpoint"], data=body)
        if r.status_code != 200:
            raise RuntimeError(f"Token exchange failed: {r.status_code} {r.text}")
        tok = r.json()

    await _persist_tokens(workspace_id, server_url, tok)
    # state is single-use
    await execute("DELETE FROM peec_oauth_states WHERE state = ?", (state,))
    return {"workspace_id": workspace_id, "server_url": server_url, "ok": True}


async def _persist_tokens(workspace_id: str, server_url: str, tok: Dict[str, Any]) -> None:
    expires_in = int(tok.get("expires_in") or 3600)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    await execute(
        "UPDATE peec_oauth_clients SET access_token = ?, refresh_token = COALESCE(NULLIF(?,''), refresh_token), "
        "expires_at = ?, updated_at = ? WHERE workspace_id = ? AND server_url = ?",
        (
            tok.get("access_token", ""),
            tok.get("refresh_token", "") or "",
            expires_at,
            now,
            workspace_id,
            server_url,
        ),
    )


# ═══════════════════════════════════════════════════════════════
# REFRESH
# ═══════════════════════════════════════════════════════════════

async def refresh(workspace_id: str, server_url: str) -> Optional[str]:
    client = await _load_client_row(workspace_id, server_url)
    if not client or not client.get("refresh_token"):
        return None
    body = {
        "grant_type": "refresh_token",
        "refresh_token": client["refresh_token"],
        "client_id": client["client_id"],
    }
    if client.get("client_secret"):
        body["client_secret"] = client["client_secret"]
    async with httpx.AsyncClient(timeout=15.0) as http:
        r = await http.post(client["token_endpoint"], data=body)
        if r.status_code != 200:
            logger.warning("Peec refresh failed for %s: %s %s",
                           workspace_id, r.status_code, r.text)
            return None
        tok = r.json()
    await _persist_tokens(workspace_id, server_url, tok)
    return tok.get("access_token")


# ═══════════════════════════════════════════════════════════════
# PUBLIC ACCESSORS
# ═══════════════════════════════════════════════════════════════

async def get_valid_token(workspace_id: str, server_url: str) -> Optional[str]:
    """Return a fresh access token, refreshing if expired. None if not connected."""
    client = await _load_client_row(workspace_id, server_url)
    if not client or not client.get("access_token"):
        return None
    expires_at = client.get("expires_at") or ""
    needs_refresh = False
    if expires_at:
        try:
            exp = datetime.fromisoformat(expires_at)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp - timedelta(seconds=REFRESH_LEEWAY_SECONDS) <= datetime.now(timezone.utc):
                needs_refresh = True
        except Exception:
            needs_refresh = True
    if needs_refresh:
        new_token = await refresh(workspace_id, server_url)
        if new_token:
            return new_token
        return client.get("access_token")  # try stale; upstream will 401 → retry
    return client.get("access_token")


async def get_status(workspace_id: str, server_url: str) -> Dict[str, Any]:
    client = await _load_client_row(workspace_id, server_url)
    if not client:
        return {"connected": False, "registered": False}
    return {
        "connected": bool(client.get("access_token")),
        "registered": bool(client.get("client_id")),
        "expires_at": client.get("expires_at") or "",
        "scope": client.get("scope") or "",
        "redirect_uri": client.get("redirect_uri") or "",
        "issuer": client.get("issuer") or "",
    }


async def disconnect(workspace_id: str, server_url: str) -> None:
    """Wipe tokens (keep client registration so a re-auth is one click)."""
    await execute(
        "UPDATE peec_oauth_clients SET access_token='', refresh_token='', expires_at='' "
        "WHERE workspace_id = ? AND server_url = ?",
        (workspace_id, server_url),
    )


def public_redirect_uri(base_url: Optional[str] = None) -> str:
    """Resolve our OAuth callback URL. Configure via GEO_PUBLIC_BASE_URL.

    For local dev set GEO_PUBLIC_BASE_URL=http://localhost:8000
    """
    base = (base_url or getattr(settings, "public_base_url", "") or "").rstrip("/")
    if not base:
        base = "http://localhost:8000"
    return f"{base}/api/peec/mcp/auth/callback"
