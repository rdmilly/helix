"""Dictionary Service — Append-Only Shorthand Dictionary

Manages the compression dictionary that maps shorthand symbols to atoms.
Core rule: A symbol, once assigned, means the same thing FOREVER.
New patterns get new symbols. Old symbols are never reassigned.

Dictionary is versioned. Every change creates a new version with a delta.
The extension stores historical versions and expands using the correct
version for old conversations.
"""
import json
from services import pg_sync
import logging
import uuid
from datetime import datetime
from typing import Dict, Optional, List, Any, Tuple

from services.database import get_db
from services.meta import get_meta_service

logger = logging.getLogger(__name__)


def _generate_shorthand(name: str, existing: set) -> str:
    """Generate a short, readable symbol from an atom name.

    Strategy: use dot-notation based on name parts.
    e.g. verify_api_key → auth.verify_key
         rate_limit_middleware → rate.limit_mw
         calculate_fibonacci → calc.fibonacci

    Falls back to abbreviated hash if collision occurs.
    """
    parts = name.lower().replace("-", "_").split("_")

    # Try progressively shorter forms
    candidates = []

    # Full dotted: first_part.rest
    if len(parts) >= 2:
        candidates.append(f"{parts[0]}.{'_'.join(parts[1:])}")
        candidates.append(f"{parts[0]}.{parts[-1]}")
        if len(parts) >= 3:
            candidates.append(f"{parts[0]}.{parts[1]}")

    # Single word
    candidates.append(name.lower()[:20])

    # Try abbreviated
    if len(parts) >= 2:
        abbrev = parts[0][:4] + "." + parts[-1][:6]
        candidates.append(abbrev)

    for candidate in candidates:
        if candidate not in existing:
            return candidate

    # Hash fallback — guaranteed unique
    short_hash = uuid.uuid4().hex[:6]
    return f"{parts[0][:4]}.{short_hash}"


class DictionaryService:
    """Manages the append-only compression dictionary.

    Invariants:
    - Symbols are immutable: once assigned, never reassigned
    - Dictionary is append-only: new entries added, never removed
    - Every change creates a new version with delta tracking
    - Atom IDs are the canonical reference, symbols are human-readable shortcuts
    """

    def __init__(self):
        self.db = get_db()
        self.meta = get_meta_service()
        self._current_version: Optional[str] = None
        self._current_dict: Dict[str, str] = {}  # symbol → atom_id
        self._reverse_dict: Dict[str, str] = {}  # atom_id → symbol
        self._loaded = False

    def load(self) -> bool:
        """Load current dictionary from DB."""
        try:
            with self.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT version, dictionary FROM dictionary_versions
                    ORDER BY created_at DESC LIMIT 1
                """)
                row = cursor.fetchone()
                if row:
                    self._current_version = row[0]
                    self._current_dict = pg_sync.dejson(row[1])
                    self._reverse_dict = {v: k for k, v in self._current_dict.items()}
                    self._loaded = True
                    logger.info(
                        f"Dictionary loaded: v{self._current_version} "
                        f"({len(self._current_dict)} entries)"
                    )
                    return True
                else:
                    # No dictionary yet — create v1
                    self._current_version = "v1"
                    self._current_dict = {}
                    self._reverse_dict = {}
                    self._loaded = True
                    return True
        except Exception as e:
            logger.error(f"Failed to load dictionary: {e}")
            return False

    def get_current(self) -> Dict[str, Any]:
        """Get current dictionary state."""
        if not self._loaded:
            self.load()
        return {
            "version": self._current_version,
            "entries": dict(self._current_dict),
            "count": len(self._current_dict),
        }

    def lookup_symbol(self, symbol: str) -> Optional[str]:
        """Look up atom_id by shorthand symbol."""
        if not self._loaded:
            self.load()
        return self._current_dict.get(symbol)

    def lookup_atom(self, atom_id: str) -> Optional[str]:
        """Look up shorthand symbol by atom_id."""
        if not self._loaded:
            self.load()
        return self._reverse_dict.get(atom_id)

    def add_entries(self, entries: Dict[str, str]) -> Dict[str, Any]:
        """Add new entries to dictionary. Creates new version.

        Args:
            entries: Dict of {symbol: atom_id} to add

        Returns:
            Dict with new version info

        Raises:
            ValueError if trying to reassign an existing symbol
        """
        if not self._loaded:
            self.load()

        # Validate: no symbol reassignment
        new_entries = {}
        for symbol, atom_id in entries.items():
            if symbol in self._current_dict:
                existing_atom = self._current_dict[symbol]
                if existing_atom != atom_id:
                    raise ValueError(
                        f"Cannot reassign symbol '{symbol}': "
                        f"already maps to {existing_atom}"
                    )
                # Same mapping — skip (idempotent)
                continue
            new_entries[symbol] = atom_id

        if not new_entries:
            return {
                "version": self._current_version,
                "added": 0,
                "message": "No new entries (all already exist)",
            }

        # Build new version
        new_dict = {**self._current_dict, **new_entries}
        prev_version = self._current_version
        # Increment version: v1 → v2, v23 → v24
        version_num = int(prev_version.lstrip("v")) + 1
        new_version = f"v{version_num}"

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO dictionary_versions
                    (version, entries_count, dictionary, delta_from, delta)
                VALUES (?, ?, ?, ?, ?)
            """, (
                new_version,
                len(new_dict),
                json.dumps(new_dict),
                prev_version,
                json.dumps(new_entries),
            ))
            conn.commit()

        # Update in-memory state
        self._current_dict = new_dict
        self._reverse_dict = {v: k for k, v in new_dict.items()}
        self._current_version = new_version

        logger.info(
            f"Dictionary updated: {prev_version} → {new_version} "
            f"(+{len(new_entries)} entries, {len(new_dict)} total)"
        )

        return {
            "version": new_version,
            "previous_version": prev_version,
            "added": len(new_entries),
            "total": len(new_dict),
            "new_entries": new_entries,
        }

    def build_from_atoms(self) -> Dict[str, Any]:
        """Build/update dictionary from all atoms in the database.

        Scans atoms table, generates symbols for any atoms not yet
        in the dictionary. Append-only — existing symbols unchanged.
        """
        if not self._loaded:
            self.load()

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name FROM atoms ORDER BY first_seen ASC")
            atoms = cursor.fetchall()

        existing_symbols = set(self._current_dict.keys())
        existing_atoms = set(self._reverse_dict.keys())
        new_entries = {}

        for atom_id, atom_name in atoms:
            if atom_id in existing_atoms:
                continue  # Already has a symbol

            symbol = _generate_shorthand(atom_name, existing_symbols)
            new_entries[symbol] = atom_id
            existing_symbols.add(symbol)

        if new_entries:
            return self.add_entries(new_entries)
        else:
            return {
                "version": self._current_version,
                "added": 0,
                "total": len(self._current_dict),
                "message": "All atoms already in dictionary",
            }

    def get_version_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get dictionary version history."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT version, created_at, entries_count, delta_from, delta
                FROM dictionary_versions
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))

            history = []
            for row in cursor.fetchall():
                delta = pg_sync.dejson(row[4]) if row[4] else {}
                history.append({
                    "version": row[0],
                    "created_at": row[1],
                    "entries_count": row[2],
                    "delta_from": row[3],
                    "delta_count": len(delta),
                })
            return history

    def get_version(self, version: str) -> Optional[Dict[str, Any]]:
        """Get a specific dictionary version."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT version, created_at, entries_count, dictionary
                FROM dictionary_versions
                WHERE version = ?
            """, (version,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "version": row[0],
                "created_at": row[1],
                "entries_count": row[2],
                "entries": pg_sync.dejson(row[3]),
            }

    @property
    def version(self) -> str:
        if not self._loaded:
            self.load()
        return self._current_version or "v1"

    @property
    def entry_count(self) -> int:
        if not self._loaded:
            self.load()
        return len(self._current_dict)


# Global singleton
_dictionary_service = DictionaryService()


def get_dictionary_service() -> DictionaryService:
    """Get dictionary service instance."""
    return _dictionary_service
