"""Synapse Models - Request/Response schemas for Phase 3.

Session lifecycle, search, and context injection models.
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# === SESSION LIFECYCLE ===

class SessionStartRequest(BaseModel):
    """Start a new Helix session."""
    session_id: str = Field(..., description="External session identifier")
    provider: str = Field(default="anthropic", description="LLM provider")
    model: str = Field(default="unknown", description="Model used")
    context_query: Optional[str] = Field(default=None, description="Query for context injection")
    tags: Optional[List[str]] = Field(default=None, description="Session tags")
    meta: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")


class SessionEndRequest(BaseModel):
    """End a Helix session."""
    session_id: str = Field(..., description="Session to close")
    summary: Optional[str] = Field(default=None, description="Final session summary")
    outcome: Optional[str] = Field(default=None, description="Session outcome")


class SessionResponse(BaseModel):
    """Session details with enriched meta."""
    session_id: str
    provider: str
    model: str
    significance: int = 0
    meta: Dict[str, Any] = {}
    created_at: Optional[str] = None
    closed_at: Optional[str] = None
    context_injected: Optional[Dict[str, Any]] = None


# === SEARCH ===

class AtomSearchRequest(BaseModel):
    """Search atoms in the DNA library."""
    query: Optional[str] = Field(default=None, description="Text query for semantic search")
    name: Optional[str] = Field(default=None, description="Filter by atom name")
    category: Optional[str] = Field(default=None, description="Filter by category")
    language: Optional[str] = Field(default=None, description="Filter by language")
    min_significance: int = Field(default=0, description="Minimum significance score")
    limit: int = Field(default=20, ge=1, le=100, description="Max results")


class SemanticSearchRequest(BaseModel):
    """Semantic search across all collections."""
    query: str = Field(..., description="Natural language query")
    collections: Optional[List[str]] = Field(
        default=None,
        description="Collections to search: atoms, sessions, entities. Defaults to all."
    )
    limit: int = Field(default=10, ge=1, le=50, description="Max results per collection")


class SearchResult(BaseModel):
    """Individual search result."""
    id: str
    collection: str
    text: str
    score: float
    metadata: Dict[str, Any] = {}


class SearchResponse(BaseModel):
    """Search results container."""
    query: str
    results: List[SearchResult]
    total: int
    collections_searched: List[str]


# === CONTEXT INJECTION ===

class ContextInjectRequest(BaseModel):
    """Request context injection for a session."""
    session_id: Optional[str] = Field(default=None, description="Session to inject context into")
    query: str = Field(..., description="What context is needed")
    max_atoms: int = Field(default=10, ge=1, le=50, description="Max atoms to include")
    max_decisions: int = Field(default=5, ge=1, le=20, description="Max decisions to include")
    max_sessions: int = Field(default=5, ge=1, le=20, description="Max related sessions")
    include_entities: bool = Field(default=True, description="Include entity context")
    since_session_id: Optional[str] = Field(default=None, description="Delta shards since this session ended")


class ContextInjectResponse(BaseModel):
    """Assembled context for injection."""
    session_id: Optional[str] = None
    query: str
    atoms: List[Dict[str, Any]] = []
    decisions: List[Dict[str, Any]] = []
    related_sessions: List[Dict[str, Any]] = []
    entities: Dict[str, List[str]] = {}
    injection_text: str = ""
    stats: Dict[str, int] = {}


# === TIER 1 ON-DEMAND CONTEXT (Phase 2.1 + 2.2) ===

class Tier1Request(BaseModel):
    """Tier 1 on-demand context enrichment request.

    Tier 0 (150 tokens) is always injected by the extension as a static header.
    Tier 1 (500-2000 tokens) is on-demand enrichment triggered before each send.

    Phase 2.1: 4-store parallel query with token-budget packing.
    Phase 2.2: Server-side shorthand compression of assembled body (~15-25% reduction).
    """
    session_id: str = Field(..., description="Active Claude session ID")
    query: str = Field(..., description="The user's current query / topic")
    budget: int = Field(
        default=1000,
        ge=500,
        le=2000,
        description="Token budget for Tier 1 context (500-2000)",
    )
    compress: bool = Field(
        default=True,
        description="Apply server-side shorthand compression (Phase 2.2). Set false to debug raw output.",
    )


class Tier1Response(BaseModel):
    """Assembled Tier 1 context ready for sandwich injection."""
    session_id: str
    query: str
    budget: int
    tokens_used: int
    injection_text: str
    sources: Dict[str, int] = Field(
        default_factory=dict,
        description="Count of items drawn from each store",
    )
    stats: Dict[str, Any] = Field(default_factory=dict)
