"""Smart conversation chunker for Helix Cortex RAG (ported from Memory v0.5).

Two-strategy chunking:
1. Turn-boundary segmentation — keeps Q&A pairs together
2. Token-window fallback — for long monotopic segments

Chunks include overlap for context continuity.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional

from config import CHUNK_TARGET_TOKENS, CHUNK_MAX_TOKENS, CHUNK_OVERLAP_TOKENS
import logging
logger = logging.getLogger(__name__)

CHARS_PER_TOKEN = 4  # rough estimate for English


@dataclass
class ConversationChunk:
    """A single chunk of conversation for embedding."""
    text: str
    chunk_index: int
    session_id: str
    source: str = ""
    timestamp: str = ""
    topic_hint: str = ""
    char_count: int = 0
    token_estimate: int = 0
    has_decision: bool = False
    has_failure: bool = False
    has_code: bool = False

    def __post_init__(self):
        self.char_count = len(self.text)
        self.token_estimate = self.char_count // CHARS_PER_TOKEN


@dataclass
class ChunkResult:
    """Result of chunking a conversation."""
    chunks: List[ConversationChunk]
    session_id: str
    total_chars: int
    strategy: str


# Patterns for turn boundaries
TURN_PATTERNS = [
    r"(?:^|\n)(?:Human|User|H|U):\s",
    r"(?:^|\n)(?:Assistant|Claude|A):\s",
    r"(?:^|\n)\[(?:human|user|assistant|claude)\]",
]

# Decision/failure indicators
DECISION_WORDS = {"decided", "decision", "chose", "choosing", "went with", "picked", "selected", "agreed"}
FAILURE_WORDS = {"broke", "failed", "error", "bug", "crash", "fix", "broken", "issue", "wrong", "revert"}
CODE_PATTERNS = re.compile(r"```|\$ |import |def |class |function |const |docker |curl |pip |npm ")


def _detect_features(text: str) -> dict:
    """Detect chunk features for metadata."""
    lower = text.lower()
    return {
        "has_decision": any(w in lower for w in DECISION_WORDS),
        "has_failure": any(w in lower for w in FAILURE_WORDS),
        "has_code": bool(CODE_PATTERNS.search(text)),
    }


def _extract_topic_hint(text: str) -> str:
    """Extract a rough topic hint from chunk content."""
    # Look for headers
    header = re.search(r"^#+\s+(.+)$", text, re.MULTILINE)
    if header:
        return header.group(1).strip()[:80]
    # First meaningful line
    for line in text.split("\n"):
        line = line.strip()
        if len(line) > 20 and not line.startswith(("```", "#", "---", "|")):
            return line[:80]
    return ""


def _split_into_turns(text: str) -> List[str]:
    """Split conversation text into turns (Human/Assistant pairs)."""
    pattern = re.compile(r"(?:^|\n)(?:Human|User|Assistant|Claude|H|A):\s", re.IGNORECASE)
    positions = [m.start() for m in pattern.finditer(text)]

    if not positions:
        # No turn markers — treat as single block
        return [text]

    turns = []
    for i, pos in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(text)
        turn = text[pos:end].strip()
        if turn:
            turns.append(turn)
    return turns


def _group_turns_into_chunks(
    turns: List[str],
    target_chars: int,
    max_chars: int,
) -> List[str]:
    """Group turns into chunks respecting token budget.

    Keeps Q&A pairs together when possible.
    """
    chunks = []
    current = ""

    for turn in turns:
        candidate = (current + "\n\n" + turn).strip() if current else turn

        if len(candidate) <= target_chars:
            current = candidate
        elif len(candidate) <= max_chars and current:
            # Over target but under max — include if it keeps a pair together
            current = candidate
        else:
            # Flush current chunk
            if current:
                chunks.append(current)
            # Start new chunk with this turn
            if len(turn) > max_chars:
                # Single turn exceeds max — split by paragraphs
                sub_chunks = _split_long_text(turn, target_chars, max_chars)
                chunks.extend(sub_chunks)
                current = ""
            else:
                current = turn

    if current:
        chunks.append(current)

    return chunks


def _split_long_text(text: str, target_chars: int, max_chars: int) -> List[str]:
    """Split long text by paragraphs or sentences."""
    paragraphs = text.split("\n\n")
    chunks = []
    current = ""

    for para in paragraphs:
        candidate = (current + "\n\n" + para).strip() if current else para
        if len(candidate) <= target_chars:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(para) > max_chars:
                # Split by sentences
                sentences = re.split(r"(?<=[.!?])\s+", para)
                sub = ""
                for s in sentences:
                    cand = (sub + " " + s).strip() if sub else s
                    if len(cand) <= target_chars:
                        sub = cand
                    else:
                        if sub:
                            chunks.append(sub)
                        sub = s
                if sub:
                    chunks.append(sub)
                current = ""
            else:
                current = para

    if current:
        chunks.append(current)
    return chunks


def _add_overlap(chunks: List[str], overlap_chars: int) -> List[str]:
    """Add overlap from end of previous chunk to start of next."""
    if overlap_chars <= 0 or len(chunks) <= 1:
        return chunks

    result = [chunks[0]]
    for i in range(1, len(chunks)):
        prev = chunks[i - 1]
        # Take last N chars of previous as overlap prefix
        overlap = prev[-overlap_chars:] if len(prev) > overlap_chars else prev
        # Find a clean break point (sentence or line boundary)
        clean = overlap.find(". ")
        if clean > 0:
            overlap = overlap[clean + 2:]
        elif "\n" in overlap:
            overlap = overlap[overlap.index("\n") + 1:]
        result.append(f"[...] {overlap}\n\n{chunks[i]}")

    return result


def chunk_conversation(
    text: str,
    session_id: str,
    source: str = "",
    timestamp: str = "",
) -> ChunkResult:
    """Chunk a conversation into retrieval-optimized segments.

    Strategy:
    1. Split into turns (Human/Assistant markers)
    2. Group turns into chunks respecting token budget
    3. Add overlap for context continuity
    4. Detect features (decisions, failures, code) per chunk
    """
    target_chars = CHUNK_TARGET_TOKENS * CHARS_PER_TOKEN
    max_chars = CHUNK_MAX_TOKENS * CHARS_PER_TOKEN
    overlap_chars = CHUNK_OVERLAP_TOKENS * CHARS_PER_TOKEN

    # Step 1: split into turns
    turns = _split_into_turns(text)
    strategy = "turn_boundary" if len(turns) > 1 else "token_window"

    # Step 2: group into chunks
    if len(turns) > 1:
        raw_chunks = _group_turns_into_chunks(turns, target_chars, max_chars)
    else:
        raw_chunks = _split_long_text(text, target_chars, max_chars)

    # Step 3: add overlap
    overlapped = _add_overlap(raw_chunks, overlap_chars)

    # Step 4: create ConversationChunk objects
    chunks = []
    for i, chunk_text in enumerate(overlapped):
        features = _detect_features(chunk_text)
        topic = _extract_topic_hint(chunk_text)
        chunks.append(ConversationChunk(
            text=chunk_text,
            chunk_index=i,
            session_id=session_id,
            source=source,
            timestamp=timestamp,
            topic_hint=topic,
            **features,
        ))

    logger.info(
        f"Chunked {session_id}: {len(text)} chars -> {len(chunks)} chunks "
        f"(strategy={strategy}, avg={len(text)//max(len(chunks),1)} chars/chunk)"
    )

    return ChunkResult(
        chunks=chunks,
        session_id=session_id,
        total_chars=len(text),
        strategy=strategy,
    )


def chunk_extract(extract: dict) -> ChunkResult:
    """Chunk a MillyExt Haiku extract (pre-processed summary).

    Extracts are typically short enough for a single chunk,
    but we still structure them for consistency.
    """
    session_id = extract.get("conversation_id", "unknown")
    source = "millyext"
    timestamp = extract.get("created_at", "")

    # Build searchable text from extract fields
    parts = []
    name = extract.get("name", "")
    if name:
        parts.append(f"# {name}")

    summary = extract.get("summary", "")
    if summary:
        parts.append(summary)

    topics = extract.get("topics", [])
    if topics:
        parts.append(f"Topics: {', '.join(topics)}")

    decisions = extract.get("decisions", [])
    if decisions:
        parts.append("Decisions: " + "; ".join(decisions))

    entities = extract.get("entities", {})
    for etype, vals in entities.items():
        if vals:
            parts.append(f"{etype}: {', '.join(vals)}")

    tools = extract.get("tools_used", [])
    if tools:
        parts.append(f"Tools: {', '.join(tools)}")

    text = "\n\n".join(parts)

    return chunk_conversation(
        text=text,
        session_id=session_id,
        source=source,
        timestamp=timestamp,
    )
