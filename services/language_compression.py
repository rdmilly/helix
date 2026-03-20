"""Language Compression Service — Universal Token Savings

Programmatic compression/expansion of natural language.
No meaning changes. Only removes/restores linguistic packaging
(articles, filler phrases, hedging, ceremonial language).

Three layers:
  1. Universal dictionary — phrases/abbreviations that work for everyone
  2. Personal frequency model — learned from user's transcript history
  3. LLM injection spec — teaches the model to output compressed

The user never sees compressed text. Expansion happens before delivery,
either client-side (browser extension) or server-side (Cortex fallback).
"""
import json
import logging
import re
import sqlite3
import os
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

log = logging.getLogger("helix.language_compression")

DB_PATH = os.environ.get("DB_PATH", "/app/data/cortex.db")
FTS_DB_PATH = os.environ.get("FTS_DB_PATH", "/app/data/conversations_fts.db")


# ============================================================
# UNIVERSAL DICTIONARY — works across all domains/users
# ============================================================

# TIER 1: Phrase compressions (biggest savings — 3-15 tokens each)
# Sorted longest-first for greedy matching
PHRASE_COMPRESSIONS: List[Tuple[str, str]] = [
    # Long ceremonial phrases → nothing or minimal
    ("let me know if you have any questions", "?q"),
    ("feel free to ask if you need", "?q"),
    ("is there anything else you'd like", "?m"),
    ("would you like me to", "shall I"),
    ("it's important to note that", "nb:"),
    ("it is important to note that", "nb:"),
    ("it's worth noting that", "nb:"),
    ("it is worth noting that", "nb:"),
    ("please note that", "nb:"),
    ("keep in mind that", "nb:"),
    ("I would recommend", "rec:"),
    ("I'd recommend", "rec:"),
    ("I'd suggest", "rec:"),
    ("you might want to consider", "consider"),
    ("it would be beneficial to", "helps to"),
    ("here's what's happening", "status:"),
    ("the reason for this is", "because"),
    ("this is because", "because"),
    ("what this means is", "means:"),
    ("I've successfully", "done:"),
    ("has been successfully", "done"),
    ("has been updated", "updated"),
    ("has been created", "created"),
    ("has been completed", "done"),
    ("in addition to", "+"),
    ("as well as", "+"),
    ("in order to", "to"),
    ("on the other hand", "but"),
    ("with respect to", "re:"),
    ("in the context of", "re:"),
    ("for example", "eg:"),
    ("for instance", "eg:"),
    ("such as", "eg:"),
    ("in other words", "ie:"),
    ("as a result", "∴"),
    ("according to", "per"),
    ("based on", "per"),
    ("is now available", "ready"),
    ("is currently", "now"),
    ("at this point", "now"),
    ("at the moment", "now"),
    ("going forward", "next"),
    ("additionally", "+"),
    ("furthermore", "+"),
    ("hope this helps", ""),
    ("I'd be happy to", "I can"),
    ("feel free to", ""),
]

# TIER 2: Filler words (pure waste — zero semantic content)
FILLER_WORDS = ["essentially", "basically", "actually", "honestly", "really"]

# TIER 3: Universal word abbreviations (save ≥2 tokens each)
WORD_ABBREVIATIONS: List[Tuple[str, str]] = [
    ("implementation", "impl"),
    ("infrastructure", "infra"),
    ("configuration", "config"),
    ("environment", "env"),
    ("authentication", "auth"),
    ("documentation", "docs"),
    ("requirements", "reqs"),
    ("dependencies", "deps"),
    ("performance", "perf"),
    ("communication", "comms"),
    ("organization", "org"),
    ("information", "info"),
    ("application", "app"),
    ("automatically", "auto"),
    ("approximately", "~"),
    ("function", "fn"),
    ("service", "svc"),
    ("parameter", "param"),
]

# The spec injected into system prompts (~138 tokens)
INJECTION_SPEC = """--- CMPRS ---
Compressed output mode. Downstream expander restores readability.

PHR: "in order to"→to, "as well as"/additionally/furthermore→+, "as a result"/therefore→∴, "for example"/"such as"→eg:, "in other words"→ie:, "note that"/"keep in mind"→nb:, "I suggest"/"recommend"→rec:, "regarding"/"about"→re:, "questions?"→?q, "anything else?"→?m
DROP: essentially, basically, actually, honestly, "hope this helps", "it's worth noting", "I'd be happy to", "feel free to"
ABR: info config app env docs impl reqs deps perf auth auto comms org fn svc param
SYM: →then/produces ←from ∴therefore ✓yes/done ✗no +and/also ~approx
RULE: skip articles when clear. terse. no filler. code unchanged.
--- END ---"""


# ============================================================
# COMPRESSOR — reduces text to compressed form
# ============================================================

class Compressor:
    """Compress natural language text. Deterministic, no LLM needed."""

    def compress(self, text: str, personal_phrases: Optional[List[Tuple[str, str]]] = None) -> Dict[str, Any]:
        """Compress text through all layers.

        Returns dict with compressed text and metrics.
        """
        original = text
        result = text
        layers_applied = []

        # Layer 1: Personal phrases (if available, applied first for longest matches)
        if personal_phrases:
            for phrase, replacement in personal_phrases:
                before = len(result)
                result = re.sub(re.escape(phrase), replacement, result, flags=re.IGNORECASE)
                if len(result) != before:
                    layers_applied.append(f"personal: '{phrase}' → '{replacement}'")

        # Layer 2: Universal phrase compression
        for phrase, replacement in PHRASE_COMPRESSIONS:
            result = re.sub(re.escape(phrase), replacement, result, flags=re.IGNORECASE)

        # Layer 3: Filler word removal
        for filler in FILLER_WORDS:
            result = re.sub(r'\b' + filler + r'\b\s*,?\s*', '', result, flags=re.IGNORECASE)

        # Layer 4: Word abbreviation
        for full_word, abbreviation in WORD_ABBREVIATIONS:
            result = re.sub(r'\b' + re.escape(full_word) + r'\b', abbreviation, result, flags=re.IGNORECASE)

        # Clean artifacts
        result = re.sub(r'\s+', ' ', result).strip()
        result = re.sub(r' +([.,;:])', r'\1', result)
        result = re.sub(r'\.\s*\.', '.', result)

        # Metrics
        orig_tokens = _estimate_tokens(original)
        comp_tokens = _estimate_tokens(result)
        savings = orig_tokens - comp_tokens

        return {
            "compressed": result,
            "original_tokens": orig_tokens,
            "compressed_tokens": comp_tokens,
            "tokens_saved": savings,
            "compression_ratio": round(savings / orig_tokens * 100, 1) if orig_tokens > 0 else 0,
            "layers_applied": layers_applied,
        }


# ============================================================
# EXPANDER — restores compressed text to natural English
# ============================================================

class Expander:
    """Expand compressed notation back to natural readable English.

    This is deterministic string transformation, not prediction.
    It adds back articles, connectors, and grammatical completeness
    that were removed during compression. Meaning is preserved
    because the compressed form contains all semantic content.
    """

    # Prefix expansions (start of sentence or after punctuation)
    PREFIXES = [
        ('rec:', "I'd recommend"),
        ('nb:', 'Note that'),
        ('eg:', 'for example,'),
        ('ie:', 'in other words,'),
        ('re:', 'regarding'),
        ('status:', "Here's the status:"),
        ('key:', 'The key point:'),
        ('done:', 'Done \u2014'),
    ]

    # Terminal markers (end of message)
    TERMINALS = [
        ('?q', 'Let me know if you have any questions.'),
        ('?m', 'Is there anything else you\'d like help with?'),
    ]

    # Symbols
    SYMBOLS = [
        ('∴', 'therefore'),
        ('✓', '(confirmed)'),
        ('✗', '(no)'),
        ('↔', 'bidirectional'),
    ]

    # Word abbreviations (reverse of compressor)
    ABBREVIATIONS = {
        'impl': 'implementation',
        'infra': 'infrastructure',
        'config': 'configuration',
        'env': 'environment',
        'auth': 'authentication',
        'docs': 'documentation',
        'reqs': 'requirements',
        'deps': 'dependencies',
        'perf': 'performance',
        'comms': 'communication',
        'org': 'organization',
        'fn': 'function',
        'svc': 'service',
        'param': 'parameter',
        'info': 'information',
        'auto': 'automatically',
    }

    def expand(self, text: str, personal_expansions: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Expand compressed notation to natural English.

        Returns dict with expanded text and metrics.
        """
        original = text
        result = text

        # Step 1: Terminal markers (handle both end-of-text and inline)
        for marker, expansion in self.TERMINALS:
            if result.rstrip().endswith(marker) or result.rstrip().endswith(marker + '.'):
                # At end of message — replace with full sentence
                result = re.sub(re.escape(marker) + r'\s*\.?\s*$', expansion, result.rstrip())
                if not result.rstrip().endswith('.'):
                    result = result.rstrip() + '.'
            else:
                # Inline — just expand
                result = result.replace(marker, expansion)

        # Step 2: Prefixes at sentence boundaries
        for prefix, expansion in self.PREFIXES:
            if result.startswith(prefix):
                result = expansion + ' ' + result[len(prefix):].lstrip()
            result = re.sub(
                r'([.!?]\s+)' + re.escape(prefix) + r'\s*',
                r'\1' + expansion + ' ', result
            )
            result = re.sub(
                r'(\n\s*)' + re.escape(prefix) + r'\s*',
                r'\1' + expansion + ' ', result
            )

        # Step 3: Symbols
        for symbol, expansion in self.SYMBOLS:
            result = result.replace(symbol, expansion)

        # Step 4: + connector (context-sensitive)
        result = re.sub(r'\.\s*\+\s+', '. Additionally, ', result)
        result = re.sub(r',\s*\+\s+', ', as well as ', result)
        result = re.sub(r'\s+\+\s+', ' and ', result)

        # Step 5: Arrows
        result = re.sub(r'\s*→\s*', ' leads to ', result)
        result = re.sub(r'\s*←\s*', ' from ', result)

        # Step 6: ~ before numbers
        result = re.sub(r'~(\d)', r'approximately \1', result)

        # Step 7: Word abbreviations (longest first, word-boundary aware)
        all_abbrevs = dict(self.ABBREVIATIONS)
        if personal_expansions:
            all_abbrevs.update(personal_expansions)

        for abbrev, full in sorted(all_abbrevs.items(), key=lambda x: -len(x[0])):
            result = re.sub(
                r'(?<![a-zA-Z_/\-])'+ re.escape(abbrev) + r'(?![a-zA-Z_/\-])',
                full, result
            )

        # Step 8: Clean up
        result = re.sub(r'  +', ' ', result)
        result = re.sub(r' +([.,;:!?])', r'\1', result)
        result = re.sub(r'([.!?]\s+)([a-z])', lambda m: m.group(1) + m.group(2).upper(), result)
        if result and result[0].islower():
            result = result[0].upper() + result[1:]

        orig_tokens = _estimate_tokens(original)
        expanded_tokens = _estimate_tokens(result)

        return {
            "expanded": result.strip(),
            "compressed_tokens": orig_tokens,
            "expanded_tokens": expanded_tokens,
            "tokens_restored": expanded_tokens - orig_tokens,
        }


# ============================================================
# PERSONAL FREQUENCY ANALYZER
# ============================================================

class FrequencyAnalyzer:
    """Analyzes transcript data to build personal compression profiles.

    Extracts high-frequency phrases from both human and assistant messages.
    These become the personal compression layer that sits on top of
    the universal dictionary.
    """

    def analyze_transcripts(self, min_frequency: int = 5, top_n: int = 50) -> Dict[str, Any]:
        """Analyze all conversation transcripts for compression targets.

        Returns frequency tables for both sides of the conversation.
        """
        try:
            conn = sqlite3.connect(str(FTS_DB_PATH))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT c5 FROM conversation_fts_content WHERE length(c5) > 20"
            ).fetchall()
            conn.close()
        except Exception as e:
            log.error(f"Failed to read transcripts: {e}")
            return {"error": str(e)}

        human_msgs = []
        assistant_msgs = []

        for row in rows:
            content = row[0] or ""
            parts = re.split(r'\n(?=Human:|Assistant:)', content)
            for part in parts:
                part = part.strip()
                if part.startswith('Human:'):
                    msg = part[6:].strip()
                    if len(msg) > 5:
                        human_msgs.append(msg)
                elif part.startswith('Assistant:'):
                    msg = part[10:].strip()
                    if len(msg) > 20:
                        assistant_msgs.append(msg)

        # Analyze both sides
        human_profile = self._build_profile(human_msgs, "human", min_frequency, top_n)
        assistant_profile = self._build_profile(assistant_msgs, "assistant", min_frequency, top_n)

        return {
            "human_messages": len(human_msgs),
            "assistant_messages": len(assistant_msgs),
            "human_profile": human_profile,
            "assistant_profile": assistant_profile,
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _build_profile(self, messages: List[str], role: str, min_freq: int, top_n: int) -> Dict[str, Any]:
        """Build frequency profile for one side of the conversation."""
        bigrams = Counter()
        trigrams = Counter()
        starters = Counter()
        filler_count = 0

        for msg in messages:
            words = re.findall(r'\b[a-z]+\b', msg.lower())

            for i in range(len(words) - 1):
                bigrams[(words[i], words[i + 1])] += 1
            for i in range(len(words) - 2):
                trigrams[(words[i], words[i + 1], words[i + 2])] += 1

            start = msg.strip().split()[:4]
            if len(start) >= 2:
                starters[' '.join(start).lower()] += 1

            for filler in FILLER_WORDS:
                filler_count += msg.lower().count(filler)

        # Build compression suggestions
        compressible_phrases = []

        # Trigrams that appear frequently and have compressible forms
        for trigram, count in trigrams.most_common(100):
            if count >= min_freq:
                phrase = ' '.join(trigram)
                # Check if this is already handled by universal dict
                already_handled = any(
                    phrase in p[0].lower() for p in PHRASE_COMPRESSIONS
                )
                if not already_handled:
                    compressible_phrases.append({
                        "phrase": phrase,
                        "frequency": count,
                        "type": "trigram",
                        "estimated_savings_per_use": 2,  # ~2 tokens saved per occurrence
                        "total_potential_savings": count * 2,
                    })

        # Frequent starters
        starter_patterns = []
        for starter, count in starters.most_common(50):
            if count >= min_freq:
                starter_patterns.append({
                    "pattern": starter,
                    "frequency": count,
                })

        compressible_phrases.sort(key=lambda x: -x["total_potential_savings"])

        return {
            "role": role,
            "total_messages": len(messages),
            "filler_word_count": filler_count,
            "top_bigrams": [
                {"phrase": f"{b[0]} {b[1]}", "count": c}
                for b, c in bigrams.most_common(top_n)
                if c >= min_freq
            ],
            "compressible_phrases": compressible_phrases[:top_n],
            "frequent_starters": starter_patterns[:30],
        }

    def get_personal_compressions(self, role: str = "assistant", min_freq: int = 10) -> List[Tuple[str, str]]:
        """Get personal phrase→compressed pairs for use in compression.

        These supplement the universal dictionary with user-specific patterns.
        Only returns patterns with clear, safe compressions.
        """
        analysis = self.analyze_transcripts(min_frequency=min_freq)
        if "error" in analysis:
            return []

        profile = analysis.get(f"{role}_profile", {})
        personal = []

        # Auto-generate compressions for high-frequency starters
        starter_map = {
            "let me check the": "",     # Just do it, don't narrate
            "let me check what": "",
            "now let me": "",
            "let me verify": "",
            "let me search for": "",
            "let me test": "",
            "let me create": "",
            "let me update": "",
            "let me fix": "",
            "let me try": "",
            "you're right, let me": "",
            "you're absolutely right": "",
            "good question —": "",
            "good question.": "",
            "great question.": "",
            "perfect,": "",
            "great,": "",
            "got it!": "",
        }

        for starter in profile.get("frequent_starters", []):
            pattern = starter["pattern"]
            for prefix, replacement in starter_map.items():
                if pattern.startswith(prefix) and starter["frequency"] >= min_freq:
                    personal.append((prefix, replacement))
                    break

        return personal


# ============================================================
# UNIFIED SERVICE
# ============================================================

class LanguageCompressionService:
    """Unified language compression service.

    Provides compress, expand, analyze, and dictionary endpoints.
    """

    def __init__(self):
        self.compressor = Compressor()
        self.expander = Expander()
        self.analyzer = FrequencyAnalyzer()
        self._personal_cache = None
        self._personal_cache_time = None

    def compress(self, text: str, use_personal: bool = True) -> Dict[str, Any]:
        """Compress text using universal + personal dictionary."""
        personal = None
        if use_personal:
            personal = self._get_personal_phrases()
        return self.compressor.compress(text, personal_phrases=personal)

    def expand(self, text: str) -> Dict[str, Any]:
        """Expand compressed text to natural English."""
        return self.expander.expand(text)

    def get_spec(self) -> Dict[str, Any]:
        """Get the injection spec for system prompts."""
        return {
            "spec": INJECTION_SPEC,
            "spec_tokens": _estimate_tokens(INJECTION_SPEC),
            "phrase_count": len(PHRASE_COMPRESSIONS),
            "abbreviation_count": len(WORD_ABBREVIATIONS),
            "filler_count": len(FILLER_WORDS),
            "version": "2.0",
        }

    def analyze(self, min_frequency: int = 5) -> Dict[str, Any]:
        """Analyze transcripts for compression opportunities."""
        return self.analyzer.analyze_transcripts(min_frequency=min_frequency)

    def test_roundtrip(self, text: str) -> Dict[str, Any]:
        """Test compress → expand roundtrip on sample text."""
        compressed = self.compress(text)
        expanded = self.expand(compressed["compressed"])
        return {
            "original": text,
            "compressed": compressed["compressed"],
            "expanded": expanded["expanded"],
            "tokens_saved": compressed["tokens_saved"],
            "compression_ratio": compressed["compression_ratio"],
            "original_tokens": compressed["original_tokens"],
            "roundtrip_tokens": expanded["expanded_tokens"],
        }

    def _get_personal_phrases(self) -> Optional[List[Tuple[str, str]]]:
        """Get cached personal phrases from profile service (refresh every 10 min)."""
        import time
        now = time.time()
        if self._personal_cache is not None and self._personal_cache_time:
            if now - self._personal_cache_time < 600:  # 10 min cache
                return self._personal_cache
        try:
            # Try profile service first (learned, staged patterns)
            from services.compression_profiles import get_profile_service
            profile_svc = get_profile_service()
            active = profile_svc.get_active_compressions(role="assistant")
            if active:
                self._personal_cache = active
                self._personal_cache_time = now
                return self._personal_cache
        except Exception as e:
            log.warning(f"Profile service unavailable: {e}")
        try:
            # Fallback to direct analysis
            self._personal_cache = self.analyzer.get_personal_compressions()
            self._personal_cache_time = now
            return self._personal_cache
        except Exception as e:
            log.warning(f"Personal phrases failed: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Get compression service statistics."""
        conn = sqlite3.connect(str(DB_PATH))
        try:
            # Check if stats table exists
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            if 'compression_stats' not in tables:
                return {"status": "no_stats_table", "message": "Run analyze first"}

            row = conn.execute(
                "SELECT COUNT(*), AVG(compression_ratio), SUM(tokens_saved) FROM compression_stats"
            ).fetchone()
            return {
                "total_compressions": row[0],
                "avg_compression_ratio": round(row[1] or 0, 1),
                "total_tokens_saved": row[2] or 0,
            }
        except Exception as e:
            return {"error": str(e)}
        finally:
            conn.close()


# ============================================================
# HELPERS
# ============================================================

def _estimate_tokens(text: str) -> int:
    """Rough token estimate (cl100k_base approximation)."""
    words = re.findall(r'\S+', text)
    total = 0.0
    for w in words:
        if len(w) <= 3:
            total += 1
        elif len(w) <= 7:
            total += 1.3
        else:
            total += 1.5 + (len(w) - 8) * 0.15
    return int(total)


# ============================================================
# SINGLETON
# ============================================================

_instance: Optional[LanguageCompressionService] = None

def get_language_compression() -> LanguageCompressionService:
    global _instance
    if _instance is None:
        _instance = LanguageCompressionService()
    return _instance
