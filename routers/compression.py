"""Compression Router — Phase 4 API Endpoints

Replaces stub endpoints with live compression functionality.

Endpoints:
  POST /api/v1/compression/compress — Compress text through multi-layer pipeline
  POST /api/v1/compression/decompress — Expand compressed text back to full form
  GET  /api/v1/compression/dictionary — Get current dictionary state
  POST /api/v1/compression/dictionary/build — Build dictionary from atoms
  GET  /api/v1/compression/dictionary/history — Dictionary version history
  GET  /api/v1/compression/stats — Compression statistics
"""
import logging
from typing import Optional, List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.compression import get_compression_service
from services.dictionary import get_dictionary_service

logger = logging.getLogger(__name__)

compression_router = APIRouter(prefix="/api/v1/compression")


# ============================================================
# Request/Response Models
# ============================================================

class CompressRequest(BaseModel):
    """Request to compress text."""
    text: str
    provider: str = "unknown"
    model: str = "unknown"
    session_id: Optional[str] = None
    max_tokens: int = Field(default=0, description="Token budget (0 = no limit)")
    layers: Optional[List[str]] = Field(
        default=None,
        description="Specific layers to apply. Options: pattern_ref, boilerplate, shorthand, pruning"
    )


class DecompressRequest(BaseModel):
    """Request to decompress text."""
    text: str
    dictionary_version: Optional[str] = None


class DictionaryAddRequest(BaseModel):
    """Request to add dictionary entries."""
    entries: dict  # symbol → atom_id


# ============================================================
# Compression Endpoints
# ============================================================

@compression_router.post("/compress")
async def compress_content(request: CompressRequest):
    """Compress text through the multi-layer compression pipeline.

    Applies up to 4 compression layers:
    1. Pattern Reference — replace known code patterns with dictionary symbols
    2. Boilerplate Dedup — collapse repeated imports, error handlers
    3. Shorthand Notation — abbreviate common programming terms
    4. Context Pruning — remove low-relevance content for token budget

    Returns compressed text with per-layer metrics.
    """
    if not request.text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    compressor = get_compression_service()
    result = compressor.compress(
        text=request.text,
        provider=request.provider,
        model=request.model,
        session_id=request.session_id,
        max_tokens=request.max_tokens,
        layers=request.layers,
    )

    return result


@compression_router.post("/decompress")
async def decompress_content(request: DecompressRequest):
    """Expand compressed text back to full form.

    Uses the dictionary version specified (or current version)
    to correctly expand shorthand symbols and pattern references.
    """
    if not request.text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    compressor = get_compression_service()
    expanded = compressor.decompress(
        text=request.text,
        dictionary_version=request.dictionary_version,
    )

    return {
        "original": request.text,
        "expanded": expanded,
        "dictionary_version": request.dictionary_version or get_dictionary_service().version,
    }


# ============================================================
# Dictionary Endpoints
# ============================================================

@compression_router.get("/dictionary")
async def get_dictionary():
    """Get current compression dictionary.

    Returns all symbol → atom_id mappings and version info.
    """
    dictionary = get_dictionary_service()
    return dictionary.get_current()


@compression_router.post("/dictionary/build")
async def build_dictionary():
    """Build/update dictionary from all atoms in the database.

    Scans the atoms table and generates shorthand symbols for any
    atoms not yet in the dictionary. Append-only — existing symbols
    are never changed.
    """
    dictionary = get_dictionary_service()
    result = dictionary.build_from_atoms()
    return result


@compression_router.post("/dictionary/add")
async def add_dictionary_entries(request: DictionaryAddRequest):
    """Add manual entries to the dictionary.

    Symbol → atom_id mappings. Symbols are immutable once assigned.
    Attempting to reassign an existing symbol returns 409 Conflict.
    """
    dictionary = get_dictionary_service()
    try:
        result = dictionary.add_entries(request.entries)
        return result
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@compression_router.get("/dictionary/history")
async def get_dictionary_history(limit: int = 20):
    """Get dictionary version history.

    Shows version progression with delta counts.
    """
    dictionary = get_dictionary_service()
    return {
        "current_version": dictionary.version,
        "history": dictionary.get_version_history(limit=limit),
    }


@compression_router.get("/dictionary/{version}")
async def get_dictionary_version(version: str):
    """Get a specific dictionary version."""
    dictionary = get_dictionary_service()
    result = dictionary.get_version(version)
    if not result:
        raise HTTPException(status_code=404, detail=f"Version {version} not found")
    return result


# ============================================================
# Stats Endpoint
# ============================================================

@compression_router.get("/stats")
async def get_compression_stats(hours: int = 24):
    """Get compression statistics.

    Returns aggregate metrics including per-layer token savings,
    compression ratios, and dictionary usage.
    """
    compressor = get_compression_service()
    return compressor.get_stats(hours=hours)


# ============================================================
# LANGUAGE COMPRESSION ENDPOINTS (v2 — universal)
# ============================================================

from services.language_compression import get_language_compression


class LangCompressRequest(BaseModel):
    text: str
    use_personal: bool = True


class LangExpandRequest(BaseModel):
    text: str


class LangTestRequest(BaseModel):
    text: str


@compression_router.post("/language/compress")
async def language_compress(request: LangCompressRequest):
    """Compress natural language text.

    Removes linguistic packaging (filler, hedging, articles, ceremony)
    while preserving all semantic content. Deterministic, no LLM needed.
    Uses universal dictionary + optional personal frequency profile.
    """
    if not request.text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    svc = get_language_compression()
    return svc.compress(request.text, use_personal=request.use_personal)


@compression_router.post("/language/expand")
async def language_expand(request: LangExpandRequest):
    """Expand compressed text back to natural readable English.

    Deterministic restoration of articles, connectors, and grammar.
    No prediction involved — only adds back known-redundant packaging.
    Used by browser extension (client-side) or Cortex (server-side fallback).
    """
    if not request.text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    svc = get_language_compression()
    return svc.expand(request.text)


@compression_router.get("/language/spec")
async def language_spec():
    """Get the compression spec for system prompt injection.

    Returns the ~138 token notation spec that teaches an LLM
    to output compressed. Inject this into system prompts.
    The extension or Cortex handles expansion before the user sees output.
    """
    svc = get_language_compression()
    return svc.get_spec()


@compression_router.post("/language/test")
async def language_test(request: LangTestRequest):
    """Test compress → expand roundtrip on sample text.

    Shows original, compressed, and expanded forms side by side
    with token counts at each stage. Use this to verify the
    expansion produces natural readable output.
    """
    if not request.text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    svc = get_language_compression()
    return svc.test_roundtrip(request.text)


@compression_router.get("/language/analyze")
async def language_analyze(min_frequency: int = 5):
    """Analyze conversation transcripts for compression opportunities.

    Runs frequency analysis on all stored transcripts.
    Returns per-role (human/assistant) profiles with:
    - Top bigrams and trigrams
    - Compressible phrases not in universal dictionary
    - Frequent sentence starters
    - Filler word counts
    """
    svc = get_language_compression()
    return svc.analyze(min_frequency=min_frequency)


@compression_router.get("/language/stats")
async def language_stats():
    """Get language compression statistics."""
    svc = get_language_compression()
    return svc.get_stats()


# ============================================================
# COMPRESSION PROFILE ENDPOINTS (auto-learning)
# ============================================================

from services.compression_profiles import get_profile_service


@compression_router.post("/profiles/build")
async def build_profiles(rebuild: bool = False):
    """Build or update compression profiles from transcript data.

    Analyzes all conversation transcripts, discovers compressible phrases,
    tracks frequency across sessions, and promotes patterns that cross
    thresholds. Call periodically (daily) or on-demand.

    Args:
        rebuild: Wipe and rebuild all profiles from scratch
    """
    svc = get_profile_service()
    return svc.build_profiles(rebuild=rebuild)


@compression_router.get("/profiles/summary")
async def profile_summary():
    """Get summary of all compression profiles.

    Shows pattern counts by stage (candidate/active/proven/decayed),
    total tokens saved, and top patterns per role.
    """
    svc = get_profile_service()
    return svc.get_profile_summary()


@compression_router.get("/profiles/history")
async def profile_history():
    """Track how compression improves over time.

    Returns growth curve: when patterns were discovered,
    how many are active, and cumulative savings potential.
    """
    svc = get_profile_service()
    return svc.get_compression_history()


@compression_router.get("/profiles/active")
async def active_profiles(role: str = "assistant"):
    """Get active compression pairs being used right now.

    These are the patterns that have crossed the frequency threshold
    and are actively being applied during compression.
    """
    svc = get_profile_service()
    pairs = svc.get_active_compressions(role=role)
    return {"role": role, "count": len(pairs), "compressions": pairs}


# ============================================================
# SCHEDULER ENDPOINTS
# ============================================================

from services.scheduler import get_scheduler


@compression_router.get("/scheduler/status")
async def scheduler_status():
    """Get status of all scheduled jobs.

    Shows enabled/disabled state, last run time, run count,
    errors, and next scheduled run for each job.
    """
    return get_scheduler().get_status()


@compression_router.post("/scheduler/run/{job_name}")
async def scheduler_run_job(job_name: str):
    """Manually trigger a scheduled job immediately.

    Jobs: compression_profiles, db_backup, pattern_decay
    """
    return await get_scheduler().run_now(job_name)
