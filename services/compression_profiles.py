"""Compression Profiles — Auto-Learning Personal Compression

Builds and maintains personal compression profiles that improve over time.
Analyzes transcript data to discover compressible patterns, tracks their
frequency across sessions, and auto-promotes patterns that cross thresholds.

The profile compounds: each analysis run discovers new patterns AND
reinforces existing ones. Patterns that stop appearing naturally decay.
The result is a personal dictionary that gets better the more you talk.

Lifecycle:
  discover → candidate (seen 5+ times)
  candidate → active (seen 20+ times, stable across 3+ sessions)
  active → proven (seen 50+ times, 10+ sessions, no decay)
  any → decayed (not seen in 30+ days)
"""
import json
import logging
import re
import sqlite3
from services import pg_sync
import os
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

log = logging.getLogger("helix.compression_profiles")

DB_PATH = os.environ.get("DB_PATH", "/app/data/cortex.db")
FTS_DB_PATH = os.environ.get("FTS_DB_PATH", "/app/data/conversations_fts.db")

# Promotion thresholds
CANDIDATE_MIN_FREQ = 5      # Seen 5+ times to become candidate
ACTIVE_MIN_FREQ = 20        # Seen 20+ times to become active
ACTIVE_MIN_SESSIONS = 3     # Across 3+ distinct sessions
PROVEN_MIN_FREQ = 50        # Seen 50+ times to become proven
PROVEN_MIN_SESSIONS = 10    # Across 10+ sessions
DECAY_DAYS = 30             # Not seen in 30 days = decayed

# Schema
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS compression_profiles (
    id SERIAL PRIMARY KEY,
    role TEXT NOT NULL,              -- 'human' or 'assistant'
    phrase TEXT NOT NULL,            -- the compressible phrase
    compressed TEXT NOT NULL DEFAULT '',  -- what it compresses to (empty = delete)
    compression_assigned INTEGER NOT NULL DEFAULT 0,  -- 1 = has verified compression target
    pattern_type TEXT NOT NULL,      -- 'trigram', 'bigram', 'starter', 'filler'
    stage TEXT NOT NULL DEFAULT 'candidate',  -- candidate/active/proven/decayed
    frequency INTEGER NOT NULL DEFAULT 0,
    session_count INTEGER NOT NULL DEFAULT 0,
    tokens_saved_per_use INTEGER NOT NULL DEFAULT 2,
    total_tokens_saved INTEGER NOT NULL DEFAULT 0,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    last_promoted TEXT,
    last_analyzed TEXT NOT NULL,
    UNIQUE(role, phrase)
);

CREATE INDEX IF NOT EXISTS idx_profiles_role_stage
    ON compression_profiles(role, stage);
CREATE INDEX IF NOT EXISTS idx_profiles_frequency
    ON compression_profiles(frequency DESC);
"""

# Known narration patterns that should compress to empty (just delete them)
# These are pure ceremony — the action happens whether announced or not
NARRATION_DELETIONS = {
    "let me check the": "",
    "let me check what": "",
    "now let me": "",
    "let me verify": "",
    "let me search for": "",
    "let me search": "",
    "let me test": "",
    "let me test the": "",
    "let me create": "",
    "let me update": "",
    "let me fix": "",
    "let me try": "",
    "let me pull up": "",
    "let me start by": "",
    "let me find": "",
    "let me look": "",
    "let me look at": "",
    "let me examine": "",
    "let me review": "",
    "you're right, let me": "",
    "you're absolutely right": "",
    "good question \u2014": "",
    "good question.": "",
    "great question.": "",
    "great question -": "",
    "good question -": "",
    "perfect,": "",
    "perfect.": "",
    "great,": "",
    "got it!": "",
    "got it.": "",
    "found it!": "",
}


class CompressionProfileService:
    """Builds and maintains personal compression profiles."""

    def __init__(self):
        self._ensure_table()

    def _db(self) -> sqlite3.Connection:
        conn = pg_sync.sqlite_conn(str(DB_PATH))
        return conn

    def _fts_db(self) -> sqlite3.Connection:
        conn = pg_sync.sqlite_conn(str(FTS_DB_PATH))
        return conn

    def _ensure_table(self):
        conn = self._db()
        try:
            pass  # schema already in PostgreSQL
            conn.commit()
        except Exception as e:
            log.warning(f"Table creation: {e}")
        finally:
            conn.close()

    # ================================================================
    # BUILD / REBUILD PROFILES
    # ================================================================

    def build_profiles(self, rebuild: bool = False) -> Dict[str, Any]:
        """Analyze transcripts and update compression profiles.

        This is the main entry point. Call periodically (e.g. daily)
        or on-demand. Each run:
          1. Scans all transcripts for phrase frequencies
          2. Discovers new candidate patterns
          3. Promotes patterns that cross thresholds
          4. Decays patterns not seen recently
          5. Calculates cumulative savings

        Args:
            rebuild: If True, wipe and rebuild from scratch
        """
        start = time.time()
        now = datetime.now(timezone.utc).isoformat()

        if rebuild:
            conn = self._db()
            conn.execute("DELETE FROM compression_profiles")
            conn.commit()
            conn.close()

        # Step 1: Extract phrases from transcripts
        human_phrases, assistant_phrases, session_phrases = self._extract_phrases()

        # Step 2: Upsert into profiles table
        human_stats = self._upsert_phrases(human_phrases, session_phrases, "human", now)
        assistant_stats = self._upsert_phrases(assistant_phrases, session_phrases, "assistant", now)

        # Step 3: Auto-assign compressions for known narration patterns
        self._assign_narration_compressions(now)

        # Step 4: Promote/decay based on thresholds
        promotions = self._run_promotions(now)
        decays = self._run_decay(now)

        # Step 5: Calculate stats
        stats = self._calculate_stats()

        duration = time.time() - start

        return {
            "status": "built" if rebuild else "updated",
            "duration_ms": round(duration * 1000),
            "human": human_stats,
            "assistant": assistant_stats,
            "promotions": promotions,
            "decays": decays,
            "stats": stats,
            "analyzed_at": now,
        }

    def _extract_phrases(self) -> Tuple[Counter, Counter, Dict]:
        """Extract phrase frequencies from conversation transcripts."""
        try:
            fts_conn = self._fts_db()
            rows = fts_conn.execute(
                "SELECT c1, c5 FROM conversation_fts_content WHERE length(c5) > 20"
            ).fetchall()
            fts_conn.close()
        except Exception as e:
            log.error(f"Failed to read transcripts: {e}")
            return Counter(), Counter(), {}

        human_phrases = Counter()
        assistant_phrases = Counter()
        # Track which sessions each phrase appears in
        session_phrases = {}  # phrase -> set of session_ids

        for row in rows:
            session_id = row[0] or "unknown"
            content = row[1] or ""

            parts = re.split(r'\n(?=Human:|Assistant:)', content)
            for part in parts:
                part = part.strip()
                if part.startswith('Human:'):
                    msg = part[6:].strip().lower()
                    self._count_phrases(msg, human_phrases, session_phrases, session_id)
                elif part.startswith('Assistant:'):
                    msg = part[10:].strip().lower()
                    self._count_phrases(msg, assistant_phrases, session_phrases, session_id)

        return human_phrases, assistant_phrases, session_phrases

    def _count_phrases(self, text: str, counter: Counter, session_map: Dict, session_id: str):
        """Count bigrams and trigrams in text."""
        words = re.findall(r'\b[a-z\']+\b', text)

        # Bigrams
        for i in range(len(words) - 1):
            phrase = f"{words[i]} {words[i+1]}"
            counter[phrase] += 1
            session_map.setdefault(phrase, set()).add(session_id)

        # Trigrams
        for i in range(len(words) - 2):
            phrase = f"{words[i]} {words[i+1]} {words[i+2]}"
            counter[phrase] += 1
            session_map.setdefault(phrase, set()).add(session_id)

        # 4-grams (for longer phrases like "let me check the")
        for i in range(len(words) - 3):
            phrase = f"{words[i]} {words[i+1]} {words[i+2]} {words[i+3]}"
            counter[phrase] += 1
            session_map.setdefault(phrase, set()).add(session_id)

        # Sentence starters (first 3-5 words)
        start_words = text.strip().split()[:5]
        if len(start_words) >= 3:
            starter = ' '.join(start_words[:4]).lower()
            counter[f"STARTER:{starter}"] += 1
            session_map.setdefault(f"STARTER:{starter}", set()).add(session_id)

    def _upsert_phrases(
        self, phrases: Counter, session_map: Dict,
        role: str, now: str
    ) -> Dict[str, int]:
        """Upsert discovered phrases into profiles table."""
        conn = self._db()
        new_candidates = 0
        updated = 0

        try:
            for phrase, freq in phrases.most_common(500):
                if freq < CANDIDATE_MIN_FREQ:
                    continue

                # Determine type
                if phrase.startswith("STARTER:"):
                    pattern_type = "starter"
                    clean_phrase = phrase[8:]  # strip STARTER: prefix
                else:
                    word_count = len(phrase.split())
                    pattern_type = "bigram" if word_count == 2 else "trigram" if word_count == 3 else "ngram"
                    clean_phrase = phrase

                sessions = len(session_map.get(phrase, set()))
                tokens_per_use = max(1, len(clean_phrase.split()) - 1)  # conservative

                existing = conn.execute(
                    "SELECT id, frequency, session_count FROM compression_profiles WHERE role=? AND phrase=?",
                    (role, clean_phrase)
                ).fetchone()

                if existing:
                    conn.execute(
                        """UPDATE compression_profiles SET
                            frequency=?, session_count=?,
                            total_tokens_saved=? * tokens_saved_per_use,
                            last_seen=?, last_analyzed=?
                            WHERE id=?""",
                        (freq, sessions, freq, now, now, existing["id"])
                    )
                    updated += 1
                else:
                    conn.execute(
                        """INSERT INTO compression_profiles
                            (role, phrase, compressed, pattern_type, stage,
                             frequency, session_count, tokens_saved_per_use,
                             total_tokens_saved, first_seen, last_seen, last_analyzed)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING""",
                        (role, clean_phrase, "", pattern_type, "candidate",
                         freq, sessions, tokens_per_use,
                         freq * tokens_per_use, now, now, now)
                    )
                    new_candidates += 1

            conn.commit()
        finally:
            conn.close()

        return {"new_candidates": new_candidates, "updated": updated}

    def _assign_narration_compressions(self, now: str):
        """Auto-assign compression targets for known narration patterns."""
        conn = self._db()
        try:
            for phrase, compressed in NARRATION_DELETIONS.items():
                conn.execute(
                    """UPDATE compression_profiles
                        SET compressed=?, compression_assigned=1,
                        stage=CASE
                            WHEN frequency >= ? THEN 'active'
                            ELSE stage END,
                        last_promoted=CASE
                            WHEN frequency >= ? AND stage='candidate' THEN ?
                            ELSE last_promoted END
                        WHERE phrase=? AND role='assistant'""",
                    (compressed, ACTIVE_MIN_FREQ, ACTIVE_MIN_FREQ, now, phrase)
                )
            conn.commit()
        finally:
            conn.close()

    def _run_promotions(self, now: str) -> Dict[str, int]:
        """Promote patterns that cross frequency thresholds."""
        conn = self._db()
        promotions = {"to_active": 0, "to_proven": 0}

        try:
            # Candidate → Active
            cursor = conn.execute(
                """UPDATE compression_profiles SET stage='active', last_promoted=?
                    WHERE stage='candidate'
                    AND frequency >= ? AND session_count >= ?""",
                (now, ACTIVE_MIN_FREQ, ACTIVE_MIN_SESSIONS)
            )
            promotions["to_active"] = cursor.rowcount

            # Active → Proven
            cursor = conn.execute(
                """UPDATE compression_profiles SET stage='proven', last_promoted=?
                    WHERE stage='active'
                    AND frequency >= ? AND session_count >= ?""",
                (now, PROVEN_MIN_FREQ, PROVEN_MIN_SESSIONS)
            )
            promotions["to_proven"] = cursor.rowcount

            conn.commit()
        finally:
            conn.close()

        return promotions

    def _run_decay(self, now: str) -> int:
        """Decay patterns not seen recently."""
        conn = self._db()
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=DECAY_DAYS)).isoformat()
            cursor = conn.execute(
                """UPDATE compression_profiles SET stage='decayed'
                    WHERE stage IN ('candidate', 'active')
                    AND last_seen < ?""",
                (cutoff,)
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    # ================================================================
    # GET ACTIVE PROFILES
    # ================================================================

    def get_active_compressions(self, role: str = "assistant") -> List[Tuple[str, str]]:
        """Get active+proven compression pairs for use in compression pipeline.

        Returns list of (phrase, compressed) tuples sorted by frequency.
        Only includes patterns that have a KNOWN safe compression target.
        Patterns with empty compressed field are narration deletions.
        Patterns without compression_assigned=1 are just tracked, not used.
        """
        conn = self._db()
        try:
            rows = conn.execute(
                """SELECT phrase, compressed FROM compression_profiles
                    WHERE role=? AND stage IN ('active', 'proven')
                    AND compression_assigned=1
                    ORDER BY frequency DESC""",
                (role,)
            ).fetchall()
            return [(r["phrase"], r["compressed"]) for r in rows]
        finally:
            conn.close()

    def get_profile_summary(self) -> Dict[str, Any]:
        """Get summary of all profiles by role and stage."""
        conn = self._db()
        try:
            rows = conn.execute(
                """SELECT role, stage, COUNT(*) as count,
                    SUM(frequency) as total_freq,
                    SUM(total_tokens_saved) as total_saved
                    FROM compression_profiles
                    GROUP BY role, stage
                    ORDER BY role, stage"""
            ).fetchall()

            summary = {}
            for r in rows:
                key = f"{r['role']}_{r['stage']}"
                summary[key] = {
                    "count": r["count"],
                    "total_frequency": r["total_freq"],
                    "total_tokens_saved": r["total_saved"],
                }

            # Top patterns per role
            for role in ["human", "assistant"]:
                top = conn.execute(
                    """SELECT phrase, compressed, stage, frequency, session_count,
                        tokens_saved_per_use, total_tokens_saved
                        FROM compression_profiles
                        WHERE role=? AND stage IN ('active', 'proven')
                        ORDER BY total_tokens_saved DESC LIMIT 20""",
                    (role,)
                ).fetchall()
                summary[f"{role}_top_patterns"] = [dict(r) for r in top]

            return summary
        finally:
            conn.close()

    def get_compression_history(self) -> Dict[str, Any]:
        """Track how compression improves over time.

        Returns the growth curve: when profiles were built,
        how many patterns at each stage, total potential savings.
        """
        conn = self._db()
        try:
            # Growth by first_seen date
            rows = conn.execute(
                """SELECT date(first_seen) as day,
                    COUNT(*) as new_patterns,
                    SUM(total_tokens_saved) as cumulative_savings
                    FROM compression_profiles
                    GROUP BY date(first_seen)
                    ORDER BY day"""
            ).fetchall()

            # Current totals
            totals = conn.execute(
                """SELECT
                    COUNT(*) as total_patterns,
                    COUNT(CASE WHEN stage='candidate' THEN 1 END) as candidates,
                    COUNT(CASE WHEN stage='active' THEN 1 END) as active,
                    COUNT(CASE WHEN stage='proven' THEN 1 END) as proven,
                    COUNT(CASE WHEN stage='decayed' THEN 1 END) as decayed,
                    SUM(total_tokens_saved) as lifetime_savings,
                    SUM(CASE WHEN stage IN ('active','proven') THEN total_tokens_saved ELSE 0 END) as active_savings
                    FROM compression_profiles"""
            ).fetchone()

            return {
                "totals": dict(totals) if totals else {},
                "growth": [dict(r) for r in rows],
            }
        finally:
            conn.close()

    def _calculate_stats(self) -> Dict[str, Any]:
        """Calculate current compression stats."""
        conn = self._db()
        try:
            row = conn.execute(
                """SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN stage='candidate' THEN 1 END) as candidates,
                    COUNT(CASE WHEN stage='active' THEN 1 END) as active,
                    COUNT(CASE WHEN stage='proven' THEN 1 END) as proven,
                    COUNT(CASE WHEN stage='decayed' THEN 1 END) as decayed,
                    SUM(total_tokens_saved) as lifetime_tokens_saved,
                    SUM(CASE WHEN stage IN ('active','proven') THEN total_tokens_saved ELSE 0 END) as active_tokens_saved
                    FROM compression_profiles"""
            ).fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()


# Singleton
_instance: Optional[CompressionProfileService] = None

def get_profile_service() -> CompressionProfileService:
    global _instance
    if _instance is None:
        _instance = CompressionProfileService()
    return _instance
