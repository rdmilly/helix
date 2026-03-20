"""pg_sync — psycopg2 connection with sqlite3-compatible interface.

Drop-in replacement for sqlite3 usage throughout Helix.
Handles:
  - ? -> %s placeholder conversion
  - HybridRow: supports both row[0] (int index) and row['key'] (dict key)
  - conn.execute() convenience method
  - Identical context manager / commit / close behaviour
  - lastrowid populated via RETURNING clause auto-detection
"""
import os
import re
import logging
from contextlib import contextmanager
from typing import Optional, Any, List

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# Superuser DSN (migrations, admin ops)
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix"
)
# App role DSN (runtime, subject to RLS)
POSTGRES_APP_DSN = os.getenv(
    "POSTGRES_APP_DSN",
    "host=helix-postgres user=helix_app password=helix_app_9f3c2a1b dbname=helix"
)
# Current user_id for this process (set per-request via set_current_user)
_current_user_id: str = "system"


def set_current_user(user_id: str):
    """Set the user_id injected into all new connections (process-global default)."""
    global _current_user_id
    _current_user_id = user_id or "system"


def get_current_user() -> str:
    return _current_user_id

# Convert SQLite ? placeholders -> psycopg2 %s
_Q_RE = re.compile(r'\?')


class HybridRow:
    """
    Wraps a psycopg2 RealDictRow so that both integer-index and key-based
    access work:
        row[0]      -> first column value  (sqlite3 compat)
        row['name'] -> named column value  (RealDictRow compat)
        row.get()   -> dict .get()
    """
    __slots__ = ('_data', '_keys')

    def __init__(self, real_dict_row):
        # real_dict_row can be None (fetchone on empty result)
        if real_dict_row is None:
            self._data = {}
            self._keys = []
        else:
            self._data = dict(real_dict_row)
            self._keys = list(self._data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._data[self._keys[key]]
        return self._data[key]

    def __contains__(self, key):
        return key in self._data

    def __bool__(self):
        return bool(self._data)

    def get(self, key, default=None):
        if isinstance(key, int):
            try:
                return self._data[self._keys[key]]
            except IndexError:
                return default
        return self._data.get(key, default)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def __repr__(self):
        return f'HybridRow({self._data!r})'


class PgCursor:
    """Wraps psycopg2 RealDictCursor to behave like sqlite3 cursor."""

    def __init__(self, raw_cursor):
        self._c = raw_cursor
        self.lastrowid: Optional[Any] = None
        self.rowcount: int = 0

    def execute(self, sql: str, params=None):
        sql = _Q_RE.sub('%s', sql)
        # Strip SQLite-only clauses that are no-ops in PG
        sql = re.sub(r'\bIF NOT EXISTS\b', 'IF NOT EXISTS', sql)  # keep (PG supports it)
        if params is None:
            self._c.execute(sql)
        else:
            self._c.execute(sql, params)
        self.rowcount = self._c.rowcount
        # Capture lastrowid if RETURNING clause present
        if 'returning' in sql.lower():
            row = self._c.fetchone()
            if row:
                vals = list(row.values())
                self.lastrowid = vals[0] if vals else None
        return self

    def executemany(self, sql: str, params_list):
        sql = _Q_RE.sub('%s', sql)
        self._c.executemany(sql, params_list)

    def fetchone(self) -> Optional[HybridRow]:
        row = self._c.fetchone()
        if row is None:
            return None
        return HybridRow(row)

    def fetchall(self) -> List[HybridRow]:
        return [HybridRow(r) for r in self._c.fetchall()]

    def __iter__(self):
        for row in self._c:
            yield HybridRow(row)


class PgConn:
    """Wraps psycopg2 connection to behave like sqlite3 connection."""

    def __init__(self, raw_conn):
        self._conn = raw_conn
        self.row_factory = None  # ignored (sqlite3 compat)

    def cursor(self) -> PgCursor:
        return PgCursor(self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor))

    def execute(self, sql: str, params=None) -> PgCursor:
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()
        return False


@contextmanager
def get_pg_conn(timeout: int = 30, user_id: str = None, admin: bool = False):
    """Context manager yielding a PgConn (non-autocommit).

    admin=True uses the superuser DSN (bypasses RLS).
    Default: helix_app role with helix.user_id set for RLS.
    """
    dsn = POSTGRES_DSN if admin else POSTGRES_APP_DSN
    conn = psycopg2.connect(dsn, connect_timeout=timeout)
    conn.autocommit = False
    if not admin:
        uid = user_id or _current_user_id or "system"
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL helix.user_id = %s", (uid,))
    pg = PgConn(conn)
    try:
        yield pg
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def sqlite_conn(path_or_str=None, timeout: int = 30, admin: bool = False, **kwargs) -> PgConn:
    """Drop-in for sqlite3.connect(). Uses helix_app DSN by default (RLS-aware)."""
    dsn = POSTGRES_DSN if admin else POSTGRES_APP_DSN
    conn = psycopg2.connect(dsn, connect_timeout=timeout)
    conn.autocommit = False
    if not admin:
        uid = _current_user_id or "system"
        with conn.cursor() as cur:
            cur.execute("SET LOCAL helix.user_id = %s", (uid,))
    return PgConn(conn)

import json as _json

def dejson(val, default=None):
    """Safe JSON decode that handles both str (SQLite) and dict/list (Postgres JSONB).
    Drop-in for json.loads() on columns that may be JSONB or TEXT.
    """
    if val is None:
        return default if default is not None else {}
    if isinstance(val, (dict, list)):
        return val
    if not isinstance(val, (str, bytes, bytearray)):
        return default if default is not None else {}
    val = val.strip()
    if not val:
        return default if default is not None else {}
    try:
        return _json.loads(val)
    except Exception:
        return default if default is not None else {}
