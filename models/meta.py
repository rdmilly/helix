"""Pydantic Models for Meta Namespaces

Defines the structure of epigenetic metadata enrichments.
"""
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any


# === META NAMESPACE MODELS ===

class StructuralMeta(BaseModel):
    """Structural metadata from AST analysis"""
    language: str
    lines: int
    is_async: bool = False
    complexity: Optional[int] = None
    template_format: str = "jinja2"
    template_version: str = "3.1"
    ast_hash: Optional[str] = None
    imports: List[str] = Field(default_factory=list)
    decorators: List[str] = Field(default_factory=list)


class DomainMeta(BaseModel):
    """Domain classification metadata"""
    categories: List[str] = Field(default_factory=list)
    confidence: float
    auto_cluster: Optional[str] = None
    inferred_project: Optional[str] = None


class SemanticMeta(BaseModel):
    """Semantic tags and similarity markers"""
    tags: List[str] = Field(default_factory=list)
    similar_to: List[str] = Field(default_factory=list)
    context_summary: Optional[str] = None


class CoOccurrenceMeta(BaseModel):
    """Co-occurrence patterns from predictor"""
    always_with: List[str] = Field(default_factory=list)
    confidence: float
    observations: int


class QualityMeta(BaseModel):
    """Quality metrics from deployment verifier"""
    deploy_success_rate: float
    contexts_verified: List[str] = Field(default_factory=list)
    corrections: int = 0
    last_verified: Optional[str] = None


class CompressionMeta(BaseModel):
    """Compression optimizer metadata"""
    shorthand: Optional[str] = None
    diff_baseline: Optional[str] = None
    delta_size: Optional[int] = None
    layer_savings: Dict[str, float] = Field(default_factory=dict)


# === TYPE REGISTRY MODELS ===

class ContentType(BaseModel):
    """Content type classification"""
    type_name: str
    confidence: float = 1.0
    detected_by: str = "registry"


class IntakePayload(BaseModel):
    """Base intake payload structure"""
    intake_type: str  # exchange, summary, tool_use, import, webhook
    content_type: Optional[str] = None  # CODE, ACTIONS, TEXT, CHANGES
    payload: Dict[str, Any]
    priority: int = 0
    idempotency_key: Optional[str] = None


class NamespaceRegistration(BaseModel):
    """Namespace registration request"""
    namespace: str
    registered_by: str
    description: str
    fields_schema: Dict[str, Any]  # JSON schema for validation
    applies_to: List[str] = Field(default_factory=lambda: ["atoms"])
    version: str = "1.0"


class TypeRegistration(BaseModel):
    """Type registry entry"""
    type_name: str
    category: str  # content_type, intake_type, embedding_model, domain_type, etc
    handler: str  # Python module path
    registered_by: str
    config: Dict[str, Any] = Field(default_factory=dict)
    active: bool = True
