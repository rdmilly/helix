"""
content_detector.py — Phase 1b: Content Type Classifier

Classifies text content into types so the pipeline routes correctly:
  code      — Python, JS, TS, shell, SQL, etc.
  prose     — natural language, documentation, chat
  config    — YAML, JSON, TOML, INI, env files
  data      — CSV, tabular, structured data
  markdown  — mixed prose+code with headings
  mixed     — significant code embedded in prose
  unknown   — insufficient signal

Used by:
  - scanner.py: skip non-code content
  - synapse.py: route to correct compression layers
  - ext_ingest.py: tag turns for intelligence pipeline
"""
import re
from typing import Tuple

# Language markers for code detection
_CODE_PATTERNS = [
    # Strong code signals
    (r"^\s*(def |class |import |from .+ import |async def )", 6),
    (r"^\s*(function |const |let |var |=>|export |import \{)", 6),
    (r"^\s*(SELECT |INSERT |UPDATE |DELETE |CREATE TABLE)", 5),
    (r"^\s*(if __name__|@\w+\(|#!\s*/usr|#!/)", 5),
    (r"\bfor .+ in .+:|\bwhile .+:|\bif .+:", 4),
    (r"\breturn\b.+|\braise\b.+|\bawait\b.+", 3),
    (r"[{\[].+[}\]]\s*$", 2),  # JSON-like
    (r"\w+\(\w*\)", 1),        # function call
]

_CONFIG_PATTERNS = [
    (r"^[a-z_]+:\s+.+$", 3),           # YAML key: value
    (r'^"[^"]+":\s+', 3),  # JSON key
    (r"^[A-Z_]+=.+$", 3),              # ENV var
    (r"^\[\w+\]$", 3),                 # INI section
    (r"^version:\s+['\"]?[\d.]+", 4),  # YAML version
]

_MARKDOWN_PATTERNS = [
    (r"^#{1,6}\s+.+$", 4),     # headings
    (r"^```", 3),              # code fence
    (r"^\*\*[^*]+\*\*", 2),   # bold
    (r"^\*\s+.+$", 2),        # bullet
    (r"^\d+\.\s+.+$", 1),    # numbered list
]

_DATA_PATTERNS = [
    (r"^[\w ,;|]+,[\w ,;|]+,[\w ,;|]+$", 3),  # CSV-like
    (r"^\d+[,;\t]\d+", 3),                      # numeric data
]


def _score_lines(lines: list, patterns: list) -> float:
    score = 0
    for line in lines:
        for pat, weight in patterns:
            if re.search(pat, line, re.MULTILINE | re.IGNORECASE):
                score += weight
                break
    return score / max(len(lines), 1)


def detect(text: str) -> Tuple[str, float]:
    """
    Returns (content_type, confidence) where confidence is 0.0-1.0.

    content_type: 'code' | 'prose' | 'config' | 'data' | 'markdown' | 'mixed' | 'unknown'
    """
    if not text or len(text.strip()) < 10:
        return "unknown", 0.0

    lines = [l for l in text.splitlines() if l.strip()][:100]  # cap at 100 lines
    if not lines:
        return "unknown", 0.0

    code_score    = _score_lines(lines, _CODE_PATTERNS)
    config_score  = _score_lines(lines, _CONFIG_PATTERNS)
    md_score      = _score_lines(lines, _MARKDOWN_PATTERNS)
    data_score    = _score_lines(lines, _DATA_PATTERNS)

    # Code fence blocks inside markdown = mixed
    has_fence = bool(re.search(r"^```", text, re.MULTILINE))
    if has_fence and md_score > 1.0:
        # Check code ratio inside fences
        fenced = re.findall(r"```[\w]*\n([\s\S]*?)```", text)
        fenced_chars = sum(len(f) for f in fenced)
        if fenced_chars / max(len(text), 1) > 0.25:
            return "mixed", min(md_score / 3, 1.0)
        return "markdown", min(md_score / 4, 1.0)

    if md_score > 1.5:
        return "markdown", min(md_score / 4, 1.0)

    if data_score > 2.0 and code_score < 1.0:
        return "data", min(data_score / 4, 1.0)

    if config_score > code_score and config_score > 0.8:
        return "config", min(config_score / 5, 1.0)

    if code_score > 2.0:
        confidence = min(code_score / 8, 1.0)
        return "code", confidence

    if code_score > 0.5:
        return "mixed", min(code_score / 5, 0.6)

    return "prose", 0.8


def is_code(text: str, threshold: float = 1.5) -> bool:
    """Quick check: is this primarily code?"""
    ctype, conf = detect(text)
    return ctype in ("code", "mixed") and conf >= threshold / 10


def should_scan(text: str, filepath: str = "") -> bool:
    """
    Should this content be sent to the AST scanner?
    True only for code/mixed with Python signals, or .py filepath.
    """
    if filepath.endswith(".py"):
        return True
    if any(filepath.endswith(ext) for ext in (".js",".ts",".jsx",".tsx")):
        return False  # future: JS scanner
    ctype, conf = detect(text)
    if ctype not in ("code", "mixed"):
        return False
    # Check for Python-specific patterns
    return bool(re.search(
        r"def \w+\(|class \w+[:(]|import \w+|from \w+ import",
        text
    ))
