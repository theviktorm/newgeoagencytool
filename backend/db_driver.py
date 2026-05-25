"""
Momentus AI — Optional Postgres driver (ADDITIVE, OFF BY DEFAULT)
================================================================

This module is the *only* place that knows how to talk to Postgres. It is a
thin async abstraction that mirrors the public helpers in `database.py`
(`get_db`, `execute`, `execute_many`, `fetch_one`, `fetch_all`, `get_pool`,
`close_pool`) so the rest of the app does not change.

HARD GUARANTEES
---------------
* asyncpg is imported LAZILY. Importing this module never requires asyncpg.
  If asyncpg is missing, only the Postgres code path raises — never import,
  never the SQLite path.
* Postgres activates ONLY when DATABASE_URL (env GEO_DATABASE_URL or
  DATABASE_URL) starts with ``postgres://`` or ``postgresql://``. For an
  empty value or a ``sqlite``-prefixed value, ``_is_postgres()`` is False and
  ``database.py`` runs its existing aiosqlite code unchanged.

SQL TRANSLATION
---------------
``translate_sql()`` converts the handful of SQLite-isms the app's QUERY
helpers rely on into Postgres equivalents at runtime. It is intentionally
conservative — see the docstring on ``translate_sql`` for the exact list of
patterns covered (and the ones deliberately left alone).
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

# NOTE: asyncpg is imported lazily inside _get_asyncpg(). Do NOT import it at
# module top-level — its absence must never break import or the SQLite path.


# ═══════════════════════════════════════════════════════════════
# DRIVER SELECTION
# ═══════════════════════════════════════════════════════════════

def _database_url() -> str:
    """Resolve the configured DATABASE_URL.

    Order: GEO_DATABASE_URL, then DATABASE_URL. We read the env directly here
    (rather than importing settings) so this helper is dependency-free and so
    a missing/!= postgres value cleanly yields the SQLite path.
    """
    return (
        os.environ.get("GEO_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or ""
    ).strip()


def _is_postgres() -> bool:
    """True ONLY for postgres:// or postgresql:// URLs. Everything else
    (empty, sqlite, sqlite+aiosqlite, etc.) is False → SQLite path."""
    url = _database_url().lower()
    return url.startswith("postgres://") or url.startswith("postgresql://")


def _get_asyncpg():
    """Lazily import asyncpg. Only ever called on the Postgres path."""
    try:
        import asyncpg  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on env
        raise RuntimeError(
            "DATABASE_URL points at Postgres but the optional 'asyncpg' "
            "dependency is not installed. Run `pip install asyncpg==0.29.0` "
            "or unset DATABASE_URL to use SQLite."
        ) from exc
    return asyncpg


def _normalized_dsn() -> str:
    """asyncpg accepts postgres:// and postgresql:// natively. We strip any
    SQLAlchemy-style ``+driver`` suffix (e.g. postgresql+asyncpg://) which
    asyncpg does not understand."""
    url = _database_url()
    # postgresql+asyncpg:// → postgresql://   ;   postgres+psycopg:// → postgres://
    return re.sub(r"^(postgres(?:ql)?)\+[a-z0-9]+://", r"\1://", url, flags=re.IGNORECASE)


# ═══════════════════════════════════════════════════════════════
# SQL TRANSLATION (SQLite → Postgres) — QUERY HELPERS ONLY
# ═══════════════════════════════════════════════════════════════

# Regex to turn `?` placeholders into `$1, $2, ...`. We must NOT touch `?`
# characters that appear inside single-quoted string literals. The app's
# queries are simple parameterized statements, so a literal-aware scan is
# enough and safe.

def _convert_placeholders(sql: str) -> str:
    """Replace each top-level `?` with `$1`, `$2`, ... left-to-right,
    skipping any `?` inside single-quoted string literals.

    SQLite uses positional `?`; Postgres/asyncpg uses `$n`. The app always
    passes params positionally, so left-to-right numbering is correct.
    """
    out: List[str] = []
    n = 0
    in_str = False
    i = 0
    length = len(sql)
    while i < length:
        ch = sql[i]
        if ch == "'":
            # Handle escaped '' inside a string literal.
            if in_str and i + 1 < length and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_str = not in_str
            out.append(ch)
            i += 1
            continue
        if ch == "?" and not in_str:
            n += 1
            out.append(f"${n}")
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


# datetime('now', <modifier>) → now() + <interval>
#   datetime('now')                 → now()
#   datetime('now', '-30 days')     → (now() + interval '-30 days')
#   datetime('now', '+1 minutes')   → (now() + interval '+1 minutes')
#   datetime('now', '-24 hours')    → (now() + interval '-24 hours')
# The modifier strings SQLite uses ('-30 days', '+1 minutes', '-24 hours',
# '-48 hours', '-30 days', etc.) are valid Postgres interval literals, so we
# can map them directly. NOTE: the dynamic forms `datetime('now', ?)` and
# `datetime('now', ? || ' days')` are NOT statically rewritable here because
# the modifier is a bound/concatenated param; those are documented as a caveat
# below and intentionally left for the (few) call sites to handle if/when
# Postgres is actually adopted. They are listed in the task report.
_RE_DT_NOW_MOD = re.compile(
    r"datetime\(\s*'now'\s*,\s*'([^']*)'\s*\)", re.IGNORECASE
)
_RE_DT_NOW = re.compile(r"datetime\(\s*'now'\s*\)", re.IGNORECASE)

# date('now') → current_date ; date(col) is valid in both, leave alone except
# the 'now' literal form.
_RE_DATE_NOW = re.compile(r"date\(\s*'now'\s*\)", re.IGNORECASE)


def _convert_datetime(sql: str) -> str:
    sql = _RE_DT_NOW_MOD.sub(lambda m: f"(now() + interval '{m.group(1)}')", sql)
    sql = _RE_DT_NOW.sub("now()", sql)
    sql = _RE_DATE_NOW.sub("current_date", sql)
    return sql


# INSERT OR IGNORE INTO  → INSERT INTO ... ON CONFLICT DO NOTHING
# We append `ON CONFLICT DO NOTHING` only if the statement does not already
# carry an explicit ON CONFLICT clause.
_RE_INSERT_OR_IGNORE = re.compile(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", re.IGNORECASE)
_RE_INSERT_OR_REPLACE = re.compile(r"\bINSERT\s+OR\s+REPLACE\s+INTO\b", re.IGNORECASE)


def _convert_insert_or(sql: str) -> str:
    if _RE_INSERT_OR_IGNORE.search(sql):
        sql = _RE_INSERT_OR_IGNORE.sub("INSERT INTO", sql)
        if "ON CONFLICT" not in sql.upper():
            sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    # INSERT OR REPLACE: kept as a documented CAVEAT. SQLite's OR REPLACE
    # deletes-then-inserts on ANY unique conflict; an exact Postgres analogue
    # needs the conflict target + DO UPDATE SET, which differs per table.
    # We rewrite the verb to plain INSERT so the statement is at least valid
    # Postgres syntax, but callers relying on REPLACE semantics on Postgres
    # MUST add an explicit ON CONFLICT ... DO UPDATE. Documented in report.
    if _RE_INSERT_OR_REPLACE.search(sql):
        sql = _RE_INSERT_OR_REPLACE.sub("INSERT INTO", sql)
    return sql


def translate_sql(sql: str) -> str:
    """Translate a SQLite query string into Postgres dialect.

    COVERED (statically, for the QUERY helpers):
      * ``?``                       → ``$1, $2, ...`` (literal-aware)
      * ``datetime('now')``         → ``now()``
      * ``datetime('now','<mod>')`` → ``(now() + interval '<mod>')``
      * ``date('now')``             → ``current_date``
      * ``INSERT OR IGNORE INTO``   → ``INSERT INTO ... ON CONFLICT DO NOTHING``
      * ``INSERT OR REPLACE INTO``  → ``INSERT INTO`` (verb only; see CAVEAT)

    NOT covered (left as-is; documented caveats):
      * ``datetime('now', ?)`` / ``datetime('now', ? || ' days')`` — modifier
        is a bound/concatenated param, not statically rewritable.
      * ``strftime(...)`` — used by a couple of analytics queries; Postgres
        needs ``to_char()``. Left untouched.
      * ``INSERT OR REPLACE`` upsert *semantics* — only the verb is fixed.
      * Multi-arg ``MAX(a,b)`` (SQLite) vs ``GREATEST(a,b)`` (Postgres).
      * ``sqlite_master`` introspection (health probe) — Postgres has no such
        table; that probe degrades gracefully via its own try/except.
    """
    sql = _convert_datetime(sql)
    sql = _convert_insert_or(sql)
    # Placeholder conversion MUST run last so any `?` we may have introduced
    # are handled and so earlier regexes see the original `?` text.
    sql = _convert_placeholders(sql)
    return sql


# ═══════════════════════════════════════════════════════════════
# CONNECTION POOL (asyncpg)
# ═══════════════════════════════════════════════════════════════

import asyncio

_pool: Any = None  # asyncpg.Pool when active
_pool_lock = asyncio.Lock()


async def get_pool():
    """Get (lazy-create) the shared asyncpg pool. Postgres path only."""
    global _pool
    if _pool is None:
        async with _pool_lock:
            if _pool is None:
                asyncpg = _get_asyncpg()
                _pool = await asyncpg.create_pool(
                    dsn=_normalized_dsn(),
                    min_size=1,
                    max_size=10,
                )
    return _pool


async def close_pool():
    """Close the pool (call on shutdown). Safe to call when no pool exists."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def _row_to_dict(record: Any) -> Dict[str, Any]:
    """asyncpg Record → plain dict (matches aiosqlite.Row → dict shape)."""
    return dict(record) if record is not None else None  # type: ignore[return-value]


# ═══════════════════════════════════════════════════════════════
# QUERY HELPERS — Postgres equivalents of database.py's helpers
# ═══════════════════════════════════════════════════════════════

async def fetch_one(query: str, params: Tuple = ()) -> Optional[Dict[str, Any]]:
    pool = await get_pool()
    sql = translate_sql(query)
    async with pool.acquire() as conn:
        record = await conn.fetchrow(sql, *params)
    return dict(record) if record is not None else None


async def fetch_all(query: str, params: Tuple = ()) -> List[Dict[str, Any]]:
    pool = await get_pool()
    sql = translate_sql(query)
    async with pool.acquire() as conn:
        records = await conn.fetch(sql, *params)
    return [dict(r) for r in records]


async def execute(query: str, params: Tuple = ()) -> int:
    pool = await get_pool()
    sql = translate_sql(query)
    async with pool.acquire() as conn:
        status = await conn.execute(sql, *params)
    return _rowcount_from_status(status)


async def execute_many(query: str, params_list: List[Tuple]) -> int:
    pool = await get_pool()
    sql = translate_sql(query)
    async with pool.acquire() as conn:
        await conn.executemany(sql, params_list)
    return len(params_list)


def _rowcount_from_status(status: Any) -> int:
    """asyncpg returns a command tag like 'INSERT 0 1' / 'UPDATE 3'. Parse the
    trailing integer to mirror sqlite cursor.rowcount as best we can."""
    try:
        return int(str(status).split()[-1])
    except (ValueError, IndexError, AttributeError):
        return 0


# ═══════════════════════════════════════════════════════════════
# CONNECTION-LIKE PROXY
# ═══════════════════════════════════════════════════════════════
# Several self-registering modules do `db = await get_db()` then call raw
# `db.execute(...)`, `db.executescript(...)`, `db.executemany(...)`,
# `db.commit()`, and read `cursor.fetchone()/fetchall()`. To keep those files
# UNTOUCHED under Postgres, get_db() (in database.py) returns the proxy below
# when _is_postgres(). On Postgres the DDL is already applied from
# pg_schema.sql, so executescript() is a safe no-op.

class _PGCursorProxy:
    """Mimics the small slice of aiosqlite's cursor used by the app."""

    def __init__(self, rows: List[Any], rowcount: int):
        self._rows = rows
        self.rowcount = rowcount

    async def fetchone(self):
        return dict(self._rows[0]) if self._rows else None

    async def fetchall(self):
        return [dict(r) for r in self._rows]


class _PGConnectionProxy:
    """Connection-like wrapper over the asyncpg pool.

    Implements only what the app touches on a raw connection. Commits are a
    no-op because each statement runs in its own pool-acquired connection
    (autocommit-style), matching the app's per-call commit behaviour.
    """

    async def execute(self, query: str, params: Tuple = ()):
        pool = await get_pool()
        sql = translate_sql(query)
        async with pool.acquire() as conn:
            stripped = sql.lstrip().upper()
            if stripped.startswith("SELECT") or " RETURNING " in sql.upper():
                rows = await conn.fetch(sql, *params)
                return _PGCursorProxy(list(rows), len(rows))
            status = await conn.execute(sql, *params)
            return _PGCursorProxy([], _rowcount_from_status(status))

    async def executemany(self, query: str, params_list: List[Tuple]):
        pool = await get_pool()
        sql = translate_sql(query)
        async with pool.acquire() as conn:
            await conn.executemany(sql, params_list)
        return _PGCursorProxy([], len(params_list))

    async def executescript(self, _script: str):
        # DDL is applied once from pg_schema.sql on init_db(). The per-module
        # SQLite DDL scripts are intentionally NOT run on Postgres. No-op.
        return None

    async def commit(self):
        # Each statement above auto-commits via its own pooled connection.
        return None

    async def close(self):
        await close_pool()
