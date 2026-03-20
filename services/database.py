"""Database Service - SQLite with Epigenetic Schema

Initializes and manages Cortex database with full epigenetic architecture.
ALL tables have meta TEXT DEFAULT '{}' columns.
Algorithm versioning baked in (fp_version, dictionary_versions, etc).
"""
import logging
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
from config import DB_PATH
from services import pg_sync

logger = logging.getLogger(__name__)


class Database:
    """SQLite database with epigenetic schema"""
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()
    
    @contextmanager
    def get_connection(self):
        """Get database connection context manager (Postgres via pg_sync)"""
        with pg_sync.get_pg_conn() as conn:
            yield conn
    
    def _initialize_schema(self):
        """Schema managed by PostgreSQL migrations (001_initial_postgres.sql).
        This is a no-op — all tables already exist.
        """
        logger.info("Database: using PostgreSQL (schema pre-applied)")


# Global database instance
db = Database()


def get_db():
    """Get database instance"""
    return db

def get_db_path() -> Path:
    """Return the path to the main cortex database."""
    return DB_PATH
