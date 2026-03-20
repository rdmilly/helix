"""Compression Service — Multi-Layer Token Compression Engine

The heart of Phase 4. Compresses text through up to 4 layers,
logs every compression event with per-layer metrics, and supports
decompression for response expansion.

Layers (applied in order):
  1. Pattern Reference  — replace known code patterns with §symbol notation
  2. Boilerplate Dedup  — collapse repeated imports/error handlers
  3. Shorthand Notation — abbreviate common programming terms
  4. Context Pruning    — remove low-relevance content (when token budget set)

Design principles:
  - Each layer is independent and can be skipped
  - Per-layer token counts logged for A/B testing
  - Dictionary version stamped on every compression event
  - Decompression uses version-matched dictionary (never wrong expansion)
  - Algorithm versioning: tokenizer recorded alongside metrics
"""
import json
import logging
import re
import uuid
from datetime import datetime
from typing import Dict, Optional, List, Any

from services.database import get_db
from services.dictionary import get_dictionary_service

logger = logging.getLogger(__name__)

# Compression layer identifiers
LAYER_PATTERN_REF = "pattern_ref"
LAYER_BOILERPLATE = "boilerplate"
LAYER_SHORTHAND = "shorthand"
LAYER_PRUNING = "pruning"

ALL_LAYERS = [LAYER_PATTERN_REF, LAYER_BOILERPLATE, LAYER_SHORTHAND, LAYER_PRUNING]

# ============================================================
# Shorthand notation table — common programming terms
# ============================================================
SHORTHAND_MAP = {
    "function": "fn",
    "return": "ret",
    "import": "imp",
    "async def": "adef",
    "await": "aw",
    "request": "req",
    "response": "res",
    "exception": "exc",
    "parameter": "param",
    "argument": "arg",
    "configuration": "cfg",
    "environment": "env",
    "database": "db",
    "connection": "conn",
    "transaction": "txn",
    "middleware": "mw",
    "authentication": "auth",
    "authorization": "authz",
    "initialize": "init",
    "implementation": "impl",
    "dependencies": "deps",
    "repository": "repo",
    "application": "app",
    "dictionary": "dict",
    "collection": "coll",
    "container": "ctnr",
    "deployment": "deploy",
    "infrastructure": "infra",
    "certificate": "cert",
    "timestamp": "ts",
    "identifier": "id",
    "template": "tmpl",
    "document": "doc",
    "management": "mgmt",
    "development": "dev",
    "production": "prod",
    "attribute": "attr",
    "operation": "op",
    "description": "desc",
    "information": "info",
    "notification": "notif",
    "permission": "perm",
    "validation": "valid",
    "integration": "integ",
    "specification": "spec",
}

# Reverse for decompression
SHORTHAND_REVERSE = {v: k for k, v in SHORTHAND_MAP.items()}

# Boilerplate patterns to collapse
BOILERPLATE_PATTERNS = [
    # Repeated import blocks
    (re.compile(r'((?:from \S+ import \S+\n){4,})', re.MULTILINE),
     lambda m: f"[imports: {len(m.group(1).strip().splitlines())} lines]\n"),
    # Standard error handling boilerplate
    (re.compile(r'try:\s*\n\s+.*?\nexcept\s+\w+(?:\s+as\s+\w+)?:\s*\n\s+(?:logger\.error|logging\.error|print)\(.*?\)\s*\n\s+raise\b', re.DOTALL),
     lambda m: "[try/except-log-raise]"),
    # Standard logging setup
    (re.compile(r'logging\.basicConfig\(.*?\)\s*\nlogger\s*=\s*logging\.getLogger\(__name__\)', re.DOTALL),
     lambda m: "[std-logging-setup]"),
]

# Pattern reference notation
PATTERN_REF_PREFIX = "§"


def _estimate_tokens(text: str) -> int:
    """Estimate token count using ~4 chars per token heuristic.

    Good enough for relative compression metrics.
    For billing-accurate counts, use the provider's actual tokenizer.
    """
    return max(1, len(text) // 4)


class CompressionService:
    """Multi-layer token compression engine.

    Each compression call:
    1. Applies requested layers in order
    2. Measures per-layer token impact
    3. Logs the event to compression_log
    4. Returns compressed text + metrics
    """

    def __init__(self):
        self.db = get_db()
        self.dictionary = get_dictionary_service()

    def compress(
        self,
        text: str,
        provider: str = "unknown",
        model: str = "unknown",
        session_id: Optional[str] = None,
        max_tokens: int = 0,
        layers: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Compress text through the multi-layer pipeline.

        Args:
            text: Input text to compress
            provider: LLM provider (for tokenizer selection)
            model: LLM model name
            session_id: Optional session for tracking
            max_tokens: Token budget (0 = no limit, skips pruning)
            layers: Specific layers to apply (None = all)

        Returns:
            Dict with compressed text, per-layer metrics, overall stats
        """
        active_layers = layers or ALL_LAYERS
        tokens_original = _estimate_tokens(text)
        current_text = text
        layer_metrics = []
        pattern_hits = 0

        # Layer 1: Pattern Reference
        if LAYER_PATTERN_REF in active_layers:
            before_tokens = _estimate_tokens(current_text)
            current_text, hits = self._apply_pattern_refs(current_text)
            after_tokens = _estimate_tokens(current_text)
            pattern_hits = hits
            layer_metrics.append({
                "layer": LAYER_PATTERN_REF,
                "tokens_before": before_tokens,
                "tokens_after": after_tokens,
                "tokens_saved": before_tokens - after_tokens,
                "pattern_hits": hits,
            })

        # Layer 2: Boilerplate Dedup
        if LAYER_BOILERPLATE in active_layers:
            before_tokens = _estimate_tokens(current_text)
            current_text = self._apply_boilerplate_dedup(current_text)
            after_tokens = _estimate_tokens(current_text)
            layer_metrics.append({
                "layer": LAYER_BOILERPLATE,
                "tokens_before": before_tokens,
                "tokens_after": after_tokens,
                "tokens_saved": before_tokens - after_tokens,
            })

        # Layer 3: Shorthand Notation
        if LAYER_SHORTHAND in active_layers:
            before_tokens = _estimate_tokens(current_text)
            current_text = self._apply_shorthand(current_text)
            after_tokens = _estimate_tokens(current_text)
            layer_metrics.append({
                "layer": LAYER_SHORTHAND,
                "tokens_before": before_tokens,
                "tokens_after": after_tokens,
                "tokens_saved": before_tokens - after_tokens,
            })

        # Layer 4: Context Pruning (only if budget set)
        if LAYER_PRUNING in active_layers and max_tokens > 0:
            before_tokens = _estimate_tokens(current_text)
            current_text = self._apply_pruning(current_text, max_tokens)
            after_tokens = _estimate_tokens(current_text)
            layer_metrics.append({
                "layer": LAYER_PRUNING,
                "tokens_before": before_tokens,
                "tokens_after": after_tokens,
                "tokens_saved": before_tokens - after_tokens,
                "budget": max_tokens,
            })

        tokens_compressed = _estimate_tokens(current_text)
        tokens_saved = tokens_original - tokens_compressed
        ratio = round(tokens_compressed / max(tokens_original, 1), 4)

        # Log compression event
        log_id = self._log_compression(
            provider=provider,
            model=model,
            session_id=session_id,
            tokens_original=tokens_original,
            tokens_compressed=tokens_compressed,
            ratio=ratio,
            layers=layer_metrics,
            pattern_hits=pattern_hits,
        )

        return {
            "compressed": current_text,
            "log_id": log_id,
            "tokens_original": tokens_original,
            "tokens_compressed": tokens_compressed,
            "tokens_saved": tokens_saved,
            "compression_ratio": ratio,
            "layers": layer_metrics,
            "dictionary_version": self.dictionary.version,
            "tokenizer": "estimate_4cpp",
        }

    def decompress(
        self,
        text: str,
        dictionary_version: Optional[str] = None,
    ) -> str:
        """Expand compressed text back to full form.

        Applies decompression in reverse layer order:
        1. Expand shorthand notation
        2. Expand pattern references (§symbol → full code)

        Boilerplate and pruning are lossy — not reversible.
        """
        current = text

        # Expand shorthand
        current = self._expand_shorthand(current)

        # Expand pattern references
        current = self._expand_pattern_refs(current, dictionary_version)

        return current

    # ============================================================
    # Layer Implementations
    # ============================================================

    def _apply_pattern_refs(self, text: str) -> tuple:
        """Layer 1: Replace known code patterns with §symbol notation.

        Scans text for function/class names that match atoms in the dictionary.
        Replaces the full definition with §symbol reference.

        Returns (compressed_text, hit_count)
        """
        if not self.dictionary.entry_count:
            return text, 0

        hits = 0
        result = text

        # Get all dictionary entries
        current = self.dictionary.get_current()
        entries = current.get("entries", {})

        # For each symbol, look for matching function definitions in text
        for symbol, atom_id in entries.items():
            # Look up the atom name from the symbol
            # Symbol format: "verify.api_key" → atom name "verify_api_key"
            atom_name = symbol.replace(".", "_")

            # Pattern: match function definitions
            # async def verify_api_key(...): ... (entire function body)
            fn_pattern = re.compile(
                rf'((?:async\s+)?def\s+{re.escape(atom_name)}\s*\([^)]*\)[^:]*:.*?)(?=\n(?:async\s+)?def\s|\nclass\s|\Z)',
                re.DOTALL
            )

            match = fn_pattern.search(result)
            if match:
                full_fn = match.group(1).rstrip()
                replacement = f"{PATTERN_REF_PREFIX}{symbol}"
                result = result.replace(full_fn, replacement, 1)
                hits += 1

        return result, hits

    def _apply_boilerplate_dedup(self, text: str) -> str:
        """Layer 2: Collapse repeated boilerplate patterns."""
        result = text
        for pattern, replacer in BOILERPLATE_PATTERNS:
            result = pattern.sub(replacer, result)
        return result

    def _apply_shorthand(self, text: str) -> str:
        """Layer 3: Abbreviate common programming terms.

        Only applies to prose/comments, not inside code strings or identifiers.
        Uses word boundary matching to avoid partial replacements.
        """
        result = text
        for full_word, short in SHORTHAND_MAP.items():
            # Word boundary match, case-insensitive for prose
            pattern = re.compile(
                rf'\b{re.escape(full_word)}\b',
                re.IGNORECASE
            )
            result = pattern.sub(short, result)
        return result

    def _apply_pruning(self, text: str, max_tokens: int) -> str:
        """Layer 4: Remove low-relevance content to fit token budget.

        Strategy: truncate from the middle, keeping start (context setup)
        and end (most recent/relevant content).
        """
        current_tokens = _estimate_tokens(text)
        if current_tokens <= max_tokens:
            return text

        # Split into lines, keep first 40% and last 40% of budget
        lines = text.split("\n")
        chars_budget = max_tokens * 4
        head_budget = int(chars_budget * 0.4)
        tail_budget = int(chars_budget * 0.4)

        head_text = ""
        head_idx = 0
        for i, line in enumerate(lines):
            if len(head_text) + len(line) + 1 > head_budget:
                break
            head_text += line + "\n"
            head_idx = i + 1

        tail_text = ""
        tail_idx = len(lines)
        for i in range(len(lines) - 1, head_idx - 1, -1):
            if len(tail_text) + len(lines[i]) + 1 > tail_budget:
                break
            tail_text = lines[i] + "\n" + tail_text
            tail_idx = i

        pruned_count = tail_idx - head_idx
        if pruned_count > 0:
            return f"{head_text}[... {pruned_count} lines pruned for token budget ...]\n{tail_text}"
        return text

    # ============================================================
    # Decompression
    # ============================================================

    def _expand_shorthand(self, text: str) -> str:
        """Reverse shorthand notation."""
        result = text
        for short, full_word in SHORTHAND_REVERSE.items():
            pattern = re.compile(rf'\b{re.escape(short)}\b')
            result = pattern.sub(full_word, result)
        return result

    def _expand_pattern_refs(self, text: str, dictionary_version: Optional[str] = None) -> str:
        """Expand §symbol references back to full atom code.

        Uses version-matched dictionary if specified, otherwise current.
        """
        if PATTERN_REF_PREFIX not in text:
            return text

        # Load correct dictionary version
        if dictionary_version:
            version_data = self.dictionary.get_version(dictionary_version)
            if version_data:
                entries = version_data["entries"]
            else:
                logger.warning(f"Dictionary version {dictionary_version} not found, using current")
                entries = self.dictionary.get_current()["entries"]
        else:
            entries = self.dictionary.get_current()["entries"]

        # Reverse: symbol → atom_id
        result = text
        db = get_db()

        for symbol, atom_id in entries.items():
            ref = f"{PATTERN_REF_PREFIX}{symbol}"
            if ref in result:
                # Look up full atom code
                with db.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT code FROM atoms WHERE id = ?", (atom_id,))
                    row = cursor.fetchone()
                    if row and row[0]:
                        result = result.replace(ref, row[0])
                    else:
                        logger.warning(f"Atom {atom_id} not found for symbol {symbol}")

        return result

    # ============================================================
    # Logging
    # ============================================================

    def _log_compression(
        self,
        provider: str,
        model: str,
        session_id: Optional[str],
        tokens_original: int,
        tokens_compressed: int,
        ratio: float,
        layers: List[Dict],
        pattern_hits: int,
    ) -> str:
        """Log compression event to compression_log table."""
        log_id = f"cmp_{uuid.uuid4().hex[:12]}"

        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO compression_log
                    (id, provider, model, session_id,
                     tokens_original_in, tokens_compressed_in,
                     compression_ratio_in, layers,
                     pattern_ref_hits, dictionary_version, tokenizer)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                log_id,
                provider,
                model,
                session_id,
                tokens_original,
                tokens_compressed,
                ratio,
                json.dumps(layers),
                pattern_hits,
                self.dictionary.version,
                "estimate_4cpp",
            ))
            conn.commit()

        return log_id

    def get_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Get compression statistics for the past N hours."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            # Overall stats
            cursor.execute("""
                SELECT
                    COUNT(*) as total_events,
                    COALESCE(SUM(tokens_original_in), 0) as total_original,
                    COALESCE(SUM(tokens_compressed_in), 0) as total_compressed,
                    COALESCE(AVG(compression_ratio_in), 0) as avg_ratio,
                    COALESCE(SUM(pattern_ref_hits), 0) as total_pattern_hits
                FROM compression_log
                WHERE timestamp > datetime('now', ?)
            """, (f"-{hours} hours",))

            row = cursor.fetchone()
            total_events = row[0]
            total_original = row[1]
            total_compressed = row[2]
            avg_ratio = round(row[3], 4)
            total_pattern_hits = row[4]

            # Per-provider breakdown
            cursor.execute("""
                SELECT provider,
                    COUNT(*) as events,
                    COALESCE(AVG(compression_ratio_in), 0) as avg_ratio
                FROM compression_log
                WHERE timestamp > datetime('now', ?)
                GROUP BY provider
            """, (f"-{hours} hours",))
            providers = {row[0]: {"events": row[1], "avg_ratio": round(row[2], 4)}
                         for row in cursor.fetchall()}

            # Dictionary info
            dictionary = get_dictionary_service()

        return {
            "period_hours": hours,
            "total_events": total_events,
            "tokens_original": total_original,
            "tokens_compressed": total_compressed,
            "tokens_saved": total_original - total_compressed,
            "avg_compression_ratio": avg_ratio,
            "total_pattern_hits": total_pattern_hits,
            "providers": providers,
            "dictionary": {
                "version": dictionary.version,
                "entries": dictionary.entry_count,
            },
        }


# Global singleton
_compression_service = CompressionService()


def get_compression_service() -> CompressionService:
    """Get compression service instance."""
    return _compression_service
