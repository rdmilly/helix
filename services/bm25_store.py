"""BM25 keyword search via SQLite FTS5.

Companion to ChromaDB vector search for hybrid retrieval.
FTS5 excels at exact keyword matches (container names, error codes,
command names) where vector similarity may miss.
"""

import sqlite3
import json
from services import pg_sync
from pathlib import Path
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

from config import FTS_DB_PATH
import logging
logger = logging.getLogger(__name__)

_initialized = False


@contextmanager
def _get_conn():
    """Get SQLite connection with WAL mode."""
    conn = sqlite3.connect(str(FTS_DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialize():
    """Create FTS5 table if not exists."""
    global _initialized
    FTS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with _get_conn() as conn:
        # Main FTS5 virtual table
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS conversation_fts USING fts5(
                chunk_id,
                session_id UNINDEXED,
                source UNINDEXED,
                timestamp UNINDEXED,
                topic_hint,
                content,
                metadata UNINDEXED,
                tokenize='porter unicode61'
            )
        """)
        # Stats tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fts_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

    _initialized = True
    count = get_count()
    logger.info(f"BM25 store initialized: {count} chunks in FTS index")


def _ensure_init():
    if not _initialized:
        initialize()


def index_chunk(
    chunk_id: str,
    session_id: str,
    content: str,
    source: str = "",
    timestamp: str = "",
    topic_hint: str = "",
    metadata: Dict = None,
) -> bool:
    """Index a single chunk for BM25 search."""
    _ensure_init()
    try:
        with _get_conn() as conn:
            # Upsert: delete then insert (FTS5 doesn't support UPDATE)
            conn.execute(
                "DELETE FROM conversation_fts WHERE chunk_id = ?",
                (chunk_id,)
            )
            conn.execute(
                "INSERT INTO conversation_fts (chunk_id, session_id, source, timestamp, topic_hint, content, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (chunk_id, session_id, source, timestamp, topic_hint, content, json.dumps(metadata or {}))
            )
        return True
    except Exception as e:
        logger.error(f"BM25 index failed for {chunk_id}: {e}")
        return False


def index_batch(chunks: List[Dict]) -> int:
    """Index multiple chunks in a single transaction."""
    _ensure_init()
    count = 0
    try:
        with _get_conn() as conn:
            for c in chunks:
                conn.execute(
                    "DELETE FROM conversation_fts WHERE chunk_id = ?",
                    (c["chunk_id"],)
                )
                conn.execute(
                    "INSERT OR IGNORE INTO conversation_fts (chunk_id, session_id, source, timestamp, topic_hint, content, metadata) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (c["chunk_id"], c.get("session_id", ""), c.get("source", ""),
                     c.get("timestamp", ""), c.get("topic_hint", ""),
                     c["content"], json.dumps(c.get("metadata", {})))
                )
                count += 1
    except Exception as e:
        logger.error(f"BM25 batch index failed: {e}")
    return count


def search(
    query: str,
    limit: int = 20,
    session_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """BM25 keyword search.

    Returns list of {chunk_id, session_id, content, topic_hint, rank, snippet}.
    """
    _ensure_init()
    try:
        with _get_conn() as conn:
            # FTS5 match query with BM25 ranking
            sql = """
                SELECT chunk_id, session_id, source, timestamp, topic_hint,
                       content, metadata,
                       rank AS bm25_score,
                       snippet(conversation_fts, 5, '<b>', '</b>', '...', 40) AS snippet
                FROM conversation_fts
                WHERE conversation_fts MATCH ?
            """
            params = [query]

            if session_filter:
                sql += " AND session_id = ?"
                params.append(session_filter)

            sql += " ORDER BY rank LIMIT ?"
            params.append(limit)

            rows = conn.execute(sql, params).fetchall()

            results = []
            for row in rows:
                results.append({
                    "chunk_id": row["chunk_id"],
                    "session_id": row["session_id"],
                    "source": row["source"],
                    "timestamp": row["timestamp"],
                    "topic_hint": row["topic_hint"],
                    "content": row["content"],
                    "metadata": pg_sync.dejson(row["metadata"]) if row["metadata"] else {},
                    "bm25_score": row["bm25_score"],
                    "snippet": row["snippet"],
                })
            return results
    except Exception as e:
        logger.error(f"BM25 search failed: {e}")
        return []


def get_count() -> int:
    """Get total indexed chunks."""
    _ensure_init()
    try:
        with _get_conn() as conn:
            row = conn.execute("SELECT COUNT(*) FROM conversation_fts").fetchone()
            return row[0] if row else 0
    except Exception:
        return 0


def delete_session(session_id: str) -> int:
    """Remove all chunks for a session."""
    _ensure_init()
    try:
        with _get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM conversation_fts WHERE session_id = ?",
                (session_id,)
            )
            return cursor.rowcount
    except Exception as e:
        logger.error(f"BM25 delete failed for {session_id}: {e}")
        return 0




def get_chunks(limit: int = 100, offset: int = 0, session_id: str = None) -> List[Dict]:
    """Retrieve chunks from FTS store for processing."""
    _ensure_init()
    with _get_conn() as conn:
        if session_id:
            rows = conn.execute(
                "SELECT chunk_id, session_id, content, source, timestamp FROM conversation_fts WHERE session_id = ? LIMIT ? OFFSET ?",
                (session_id, limit, offset)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT chunk_id, session_id, content, source, timestamp FROM conversation_fts LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
    return [{"chunk_id": r[0], "session_id": r[1], "content": r[2], "source": r[3], "timestamp": r[4]} for r in rows]

def get_stats() -> Dict:
    """Get FTS index statistics."""
    _ensure_init()
    try:
        with _get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM conversation_fts").fetchone()[0]
            sessions = conn.execute("SELECT COUNT(DISTINCT session_id) FROM conversation_fts").fetchone()[0]
            db_size = FTS_DB_PATH.stat().st_size if FTS_DB_PATH.exists() else 0
            return {
                "total_chunks": total,
                "unique_sessions": sessions,
                "db_size_bytes": db_size,
                "db_size_mb": round(db_size / 1024 / 1024, 2),
            }
    except Exception as e:
        return {"error": str(e)}
