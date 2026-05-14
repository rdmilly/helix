"""The Forge — SQLite database for workspace metadata and pattern catalog."""

import sqlite3
import hashlib
import json
import logging
from pathlib import Path
from config import DB_PATH

logger = logging.getLogger("forge.db")


def get_db() -> sqlite3.Connection:
    """Get SQLite connection."""
    db = sqlite3.connect(str(DB_PATH), timeout=10)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=5000")
    db.row_factory = sqlite3.Row
    return db


def init_db():
    """Create all tables."""
    db = get_db()
    try:
        db.executescript("""
            -- ============================================
            -- WORKSPACE: File metadata and search
            -- ============================================
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL UNIQUE,
                project TEXT,
                filename TEXT NOT NULL,
                extension TEXT,
                language TEXT,
                size_bytes INTEGER,
                line_count INTEGER,
                token_estimate INTEGER,
                content_hash TEXT,
                minio_version TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                deleted INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
            CREATE INDEX IF NOT EXISTS idx_files_project ON files(project);
            CREATE INDEX IF NOT EXISTS idx_files_language ON files(language);

            -- Full-text search on file content
            CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
                path, project, filename, content,
                tokenize='porter'
            );

            -- ============================================
            -- FORGE: Pattern catalog
            -- ============================================
            CREATE TABLE IF NOT EXISTS atoms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fingerprint TEXT NOT NULL UNIQUE,
                name TEXT,
                code TEXT NOT NULL,
                language TEXT,
                section TEXT,
                token_count INTEGER,
                first_seen TEXT DEFAULT (datetime('now')),
                last_seen TEXT DEFAULT (datetime('now')),
                occurrence_count INTEGER DEFAULT 1,
                projects_json TEXT DEFAULT '[]',
                files_json TEXT DEFAULT '[]'
            );

            CREATE INDEX IF NOT EXISTS idx_atoms_fingerprint ON atoms(fingerprint);
            CREATE INDEX IF NOT EXISTS idx_atoms_language ON atoms(language);
            CREATE INDEX IF NOT EXISTS idx_atoms_section ON atoms(section);
            CREATE INDEX IF NOT EXISTS idx_atoms_count ON atoms(occurrence_count DESC);

            CREATE TABLE IF NOT EXISTS molecules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                atom_fingerprints_json TEXT NOT NULL,
                atom_count INTEGER,
                language TEXT,
                description TEXT,
                co_occurrence_count INTEGER DEFAULT 1,
                first_seen TEXT DEFAULT (datetime('now')),
                last_seen TEXT DEFAULT (datetime('now')),
                projects_json TEXT DEFAULT '[]'
            );

            CREATE INDEX IF NOT EXISTS idx_molecules_count ON molecules(co_occurrence_count DESC);

            CREATE TABLE IF NOT EXISTS organisms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                molecule_ids_json TEXT NOT NULL,
                molecule_count INTEGER,
                description TEXT,
                project_count INTEGER DEFAULT 1,
                first_seen TEXT DEFAULT (datetime('now')),
                last_seen TEXT DEFAULT (datetime('now')),
                projects_json TEXT DEFAULT '[]'
            );

            -- ============================================
            -- SCAN LOG: Track what's been scanned
            -- ============================================
            CREATE TABLE IF NOT EXISTS scan_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                atoms_found INTEGER DEFAULT 0,
                molecules_matched INTEGER DEFAULT 0,
                scanned_at TEXT DEFAULT (datetime('now')),
                source TEXT DEFAULT 'workspace'
            );

            CREATE INDEX IF NOT EXISTS idx_scan_hash ON scan_log(content_hash);

            -- ============================================
            -- VERSION HISTORY
            -- ============================================
            CREATE TABLE IF NOT EXISTS file_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER REFERENCES files(id),
                version_num INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                size_bytes INTEGER,
                minio_version TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_versions_file ON file_versions(file_id);
        """)
        db.commit()
        logger.info("Database tables initialized")
    finally:
        db.close()


def get_stats() -> dict:
    """Get database statistics."""
    db = get_db()
    try:
        files = db.execute("SELECT COUNT(*) as c FROM files WHERE deleted=0").fetchone()['c']
        atoms = db.execute("SELECT COUNT(*) as c FROM atoms").fetchone()['c']
        molecules = db.execute("SELECT COUNT(*) as c FROM molecules").fetchone()['c']
        organisms = db.execute("SELECT COUNT(*) as c FROM organisms").fetchone()['c']
        scans = db.execute("SELECT COUNT(*) as c FROM scan_log").fetchone()['c']
        return {
            "workspace_files": files,
            "atoms": atoms,
            "molecules": molecules,
            "organisms": organisms,
            "total_scans": scans
        }
    finally:
        db.close()
