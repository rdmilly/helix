"""Pydantic Models for Intake Payloads

Defines schemas for different intake types.
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


# === UNIVERSAL INTAKE WRAPPER ===

class IntakePayload(BaseModel):
    """Universal intake envelope for registry-based routing.
    
    All intake goes through this wrapper. The `intake_type` field
    routes through type_registry; `payload` carries the actual data.
    """
    intake_type: str  # Matched against type_registry
    payload: Dict[str, Any]  # The actual data (schema varies by type)
    content_type: Optional[str] = None  # Auto-classified if not provided
    priority: int = Field(default=5, ge=1, le=10)  # 1=highest, 10=lowest
    source: Optional[str] = None  # membrane, synapse, webhook, import
    session_id: Optional[str] = None  # Originating session


# === INTAKE TYPE SCHEMAS ===

class ExchangePayload(BaseModel):
    """Full conversation exchange with tools"""
    provider: str
    model: str
    messages: List[Dict[str, Any]]
    tool_calls: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SummaryPayload(BaseModel):
    """Summarized session data"""
    session_id: str
    provider: str
    model: str
    summary: str
    significance: float = 0.0
    tags: List[str] = Field(default_factory=list)
    entities_mentioned: List[str] = Field(default_factory=list)
    decisions_made: List[str] = Field(default_factory=list)


class ToolUsePayload(BaseModel):
    """Extracted tool usage"""
    tool_name: str
    arguments: Dict[str, Any]
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class ImportPayload(BaseModel):
    """Bulk import from external source"""
    source: str  # forge_migration, memory_migration, manual_upload
    records: List[Dict[str, Any]]
    batch_id: str
    total_count: int


class WebhookPayload(BaseModel):
    """Generic webhook intake"""
    source: str  # github, gitlab, external_service
    event_type: str
    payload: Dict[str, Any]
    headers: Dict[str, str] = Field(default_factory=dict)


# === DNA RECORD SCHEMAS ===

class AtomCreate(BaseModel):
    """Create new atom"""
    name: str
    full_name: Optional[str] = None
    code: str
    template: Optional[str] = None
    parameters_json: Optional[str] = None
    structural_fp: Optional[str] = None
    semantic_fp: Optional[str] = None


class MoleculeCreate(BaseModel):
    """Create new molecule"""
    name: str
    description: Optional[str] = None
    atom_ids: List[str]
    atom_names: List[str]
    template: Optional[str] = None


class OrganismCreate(BaseModel):
    """Create new organism"""
    name: str
    description: Optional[str] = None
    molecule_ids: List[str]
    template: Optional[str] = None


class SessionCreate(BaseModel):
    """Create new session record"""
    provider: str
    model: str
    summary: Optional[str] = None
    significance: float = 0.0
    tags: List[str] = Field(default_factory=list)


# === COMPRESSION LOG ===

class CompressionEntry(BaseModel):
    """Compression metrics entry"""
    provider: str
    model: str
    conversation_id: Optional[str] = None
    session_id: Optional[str] = None
    tokens_original_in: int
    tokens_compressed_in: int
    tokens_original_out: int
    tokens_compressed_out: int
    compression_ratio_in: float
    compression_ratio_out: float
    layers: List[Dict[str, Any]] = Field(default_factory=list)
    pattern_ref_hits: int = 0
    dictionary_version: str
    tokenizer: str


# === ANOMALY & NUDGE ===

class AnomalyCreate(BaseModel):
    """Create anomaly"""
    type: str
    description: str
    evidence: Optional[str] = None
    severity: str = "MEDIUM"
    session_id: Optional[str] = None


class NudgeCreate(BaseModel):
    """Create nudge"""
    description: str
    category: Optional[str] = None
    priority: str = "MEDIUM"
    session_id: Optional[str] = None


# === DECISION TRACKING ===

class DecisionCreate(BaseModel):
    """Create decision record"""
    session_id: str
    decision: str
    rationale: Optional[str] = None
    project: Optional[str] = None