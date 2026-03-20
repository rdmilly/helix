"""
Phrase Promoter -- Adaptive Symbol Schema (Epigenetic Layer)

Reads proven phrases from compression_profiles and auto-promotes them
to the shorthand dictionary with auto-generated §symbols.

This is the epigenetic bridge: it takes what the compression profiler
observed (raw frequency) and promotes it into the active compression
dictionary (actionable symbols). The LLM is then taught those symbols
via the SPEC block, and the expander decodes them from SSE responses.

Symbol generation algorithm:
  - Extract first 2 chars of each word's consonant cluster
  - Join max 2 words: "helix cortex" -> §hx, "docker compose" -> §dc
  - Collisions resolved by appending digit: §sw, §sw2, §sw3...
  - Symbols are permanent once assigned (append-only, never reassigned)

Lifecycle:
  compression_profiles (proven stage)
    -> phrase_promoter (generate §symbol)
      -> phrase_shorthand table (persistent symbol map)
        -> /api/v1/synapse/dictionary (served to extension)
          -> compression.js SPEC (taught to LLM)
            -> LLM uses §symbols in responses
              -> expander.js decodes from SSE stream
"""
import json
import logging
import re
import sqlite3
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("helix.phrase_promoter")

DB_PATH = os.environ.get("DB_PATH", "/app/data/cortex.db")

# Minimum stage to promote (proven = 50+ occurrences, 10+ sessions)
PROMOTION_STAGE = "proven"

# Phrases to never assign symbols (too generic, no savings)
BLACKLIST = {
    "can you", "in the", "of the", "to be", "want to",
    "i think", "i want", "do you", "is a", "it is",
    "i am", "we are", "let me", "you can", "to the",
    "and the", "for the", "with the", "from the", "that the",
    "this is", "there is", "it was", "he is", "she is",
}

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS phrase_shorthand (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL UNIQUE,       -- the §symbol
    phrase TEXT NOT NULL UNIQUE,       -- the full phrase it maps to
    tokens_saved_est INTEGER DEFAULT 0, -- estimated tokens saved per use
    frequency INTEGER DEFAULT 0,        -- frequency from compression_profiles
    stage TEXT DEFAULT 'active',        -- active/retired
    promoted_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_phrase_shorthand_symbol
    ON phrase_shorthand(symbol);
CREATE INDEX IF NOT EXISTS idx_phrase_shorthand_phrase
    ON phrase_shorthand(phrase);
"""


class PhrasePromoter:
    """Promotes proven compression phrases to §symbol dictionary."""

    def __init__(self):
        self._db_path = DB_PATH
        self._ensure_table()

    def _db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self):
        with self._db() as conn:
            conn.executescript(CREATE_TABLE)
            conn.commit()

    def _generate_symbol(self, phrase: str, existing_symbols: set) -> str:
        """
        Generate §symbol from phrase.
        Takes first consonant cluster (max 2 chars) from each of first 2 words.

        "helix cortex"    -> h+x, c+x -> hx -> §hx
        "docker compose"  -> d+c, c+m -> dc -> §dc
        "service worker"  -> s+w, w+r -> sw -> §sw
        "chrome extension"-> c+h, e+x -> ch ex -> §cx
        """
        VOWELS = set('aeiou')

        def word_symbol(word):
            word = word.lower()
            # Get first char always
            if not word:
                return ''
            first = word[0]
            # Get first consonant after first char (if any)
            rest_consonants = [c for c in word[1:] if c.isalpha() and c not in VOWELS]
            second = rest_consonants[0] if rest_consonants else ''
            return first + second

        # Clean phrase: remove punctuation, split
        words = re.sub(r'[^a-zA-Z\s-]', '', phrase).replace('-', ' ').split()
        words = [w for w in words if len(w) > 1]  # skip single chars

        if not words:
            return None

        # Build base from first 2 words
        parts = [word_symbol(w) for w in words[:2] if w]
        base = '§' + ''.join(parts)

        if len(base) < 2:  # too short, fallback
            base = '§' + words[0][:3].lower()

        # Resolve collisions
        symbol = base
        n = 2
        while symbol in existing_symbols:
            symbol = base + str(n)
            n += 1

        return symbol

    def promote(self) -> Dict:
        """
        Run promotion cycle:
        1. Load all proven phrases from compression_profiles
        2. Filter out blacklisted and already-promoted phrases
        3. Generate §symbols for new promotions
        4. Write to phrase_shorthand table
        Returns summary of what was promoted.
        """
        now = datetime.now(timezone.utc).isoformat()
        promoted = []
        skipped = []

        with self._db() as conn:
            # Get all proven phrases
            proven = conn.execute("""
                SELECT phrase, frequency, session_count, tokens_saved_per_use
                FROM compression_profiles
                WHERE stage = ?
                ORDER BY frequency DESC
            """, (PROMOTION_STAGE,)).fetchall()

            # Get existing symbols to avoid collisions
            existing = set(
                row[0] for row in
                conn.execute("SELECT symbol FROM phrase_shorthand").fetchall()
            )

            # Get already-promoted phrases
            already_promoted = set(
                row[0] for row in
                conn.execute("SELECT phrase FROM phrase_shorthand").fetchall()
            )

            for row in proven:
                phrase = row['phrase'].strip().lower()

                # Skip blacklist
                if phrase in BLACKLIST:
                    skipped.append({'phrase': phrase, 'reason': 'blacklist'})
                    continue

                # Skip already promoted
                if phrase in already_promoted:
                    # Update frequency + last_seen
                    conn.execute("""
                        UPDATE phrase_shorthand
                        SET frequency = ?, last_seen = ?
                        WHERE phrase = ?
                    """, (row['frequency'], now, phrase))
                    continue

                # Skip single-word phrases (no savings vs abbreviations)
                if len(phrase.split()) < 2:
                    skipped.append({'phrase': phrase, 'reason': 'single_word'})
                    continue

                # Skip very short phrases (< 8 chars, not worth symbolizing)
                if len(phrase) < 8:
                    skipped.append({'phrase': phrase, 'reason': 'too_short'})
                    continue

                # Generate symbol
                symbol = self._generate_symbol(phrase, existing)
                if not symbol:
                    skipped.append({'phrase': phrase, 'reason': 'symbol_generation_failed'})
                    continue

                # Estimate token savings: (phrase_chars - symbol_chars) / 4
                tokens_saved = max(1, (len(phrase) - len(symbol)) // 4)

                # Insert
                try:
                    conn.execute("""
                        INSERT INTO phrase_shorthand
                            (symbol, phrase, tokens_saved_est, frequency, promoted_at, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (symbol, phrase, tokens_saved, row['frequency'], now, now))
                    existing.add(symbol)
                    already_promoted.add(phrase)
                    promoted.append({'symbol': symbol, 'phrase': phrase, 'tokens_saved': tokens_saved})
                    log.info(f"Promoted: {phrase!r} -> {symbol}")
                except sqlite3.IntegrityError:
                    skipped.append({'phrase': phrase, 'reason': 'integrity_error'})

            conn.commit()

        log.info(f"Phrase promotion: {len(promoted)} new, {len(skipped)} skipped")
        return {
            'promoted': len(promoted),
            'skipped': len(skipped),
            'new_symbols': promoted,
            'total_symbols': len(promoted) + len(already_promoted) - len(promoted),
        }

    def get_shorthand_map(self) -> Dict[str, str]:
        """
        Return the full shorthand map: {phrase: symbol}
        Used by the dictionary endpoint and compression SPEC builder.
        """
        with self._db() as conn:
            rows = conn.execute("""
                SELECT symbol, phrase FROM phrase_shorthand
                WHERE stage = 'active'
                ORDER BY frequency DESC
            """).fetchall()
        return {row['phrase']: row['symbol'] for row in rows}

    def get_reverse_map(self) -> Dict[str, str]:
        """
        Return reverse map: {symbol: phrase}
        Used by the inbound expander.
        """
        with self._db() as conn:
            rows = conn.execute("""
                SELECT symbol, phrase FROM phrase_shorthand
                WHERE stage = 'active'
            """).fetchall()
        return {row['symbol']: row['phrase'] for row in rows}

    def get_stats(self) -> Dict:
        with self._db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM phrase_shorthand").fetchone()[0]
            active = conn.execute(
                "SELECT COUNT(*) FROM phrase_shorthand WHERE stage = 'active'"
            ).fetchone()[0]
            top = conn.execute("""
                SELECT symbol, phrase, frequency, tokens_saved_est
                FROM phrase_shorthand
                WHERE stage = 'active'
                ORDER BY frequency DESC LIMIT 10
            """).fetchall()
        return {
            'total': total,
            'active': active,
            'top_symbols': [dict(r) for r in top],
        }

    def build_spec_additions(self) -> str:
        """
        Build the SPEC block additions for the compression module.
        Returns the lines to add to the SPEC that teach the LLM the current symbols.
        Only returns top 20 by frequency to keep SPEC short (~40 tokens).
        """
        with self._db() as conn:
            rows = conn.execute("""
                SELECT symbol, phrase
                FROM phrase_shorthand
                WHERE stage = 'active'
                ORDER BY frequency DESC
                LIMIT 20
            """).fetchall()

        if not rows:
            return ''

        lines = ['SYM: ' + ' '.join(f'{r["phrase"]}={r["symbol"]}' for r in rows)]
        return '\n'.join(lines)


def get_phrase_promoter() -> PhrasePromoter:
    """Singleton accessor."""
    if not hasattr(get_phrase_promoter, '_instance'):
        get_phrase_promoter._instance = PhrasePromoter()
    return get_phrase_promoter._instance
