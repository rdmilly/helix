"""Agent Events Router — called by helix-mcp and assembler.

Endpoints for recording suggestion outcomes, failures, and quality events.
These write directly to agent_preference, failure, and quality namespaces.
"""
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/api/v1/agent", tags=["Agent Events"])


class SuggestionEvent(BaseModel):
    atom_id: str
    action: str  # accepted | rejected | modified
    session_id: Optional[str] = None
    original_code: Optional[str] = None
    replacement_code: Optional[str] = None
    context: Optional[str] = None


class FailureEvent(BaseModel):
    atom_id: str
    error_type: str
    error_message: str
    project: Optional[str] = None
    is_rollback: bool = False


class QualityEvent(BaseModel):
    atom_id: str
    context: str
    success: bool = True


@router.post("/suggestion")
async def record_suggestion(event: SuggestionEvent):
    """Record an AI suggestion outcome. replacement_code is the highest-value field."""
    from services.agent_preference_tracker import record_suggestion_outcome
    await record_suggestion_outcome(
        atom_id=event.atom_id, action=event.action,
        session_id=event.session_id, original_code=event.original_code,
        replacement_code=event.replacement_code, context=event.context,
    )
    return {"status": "ok", "atom_id": event.atom_id, "action": event.action}


@router.post("/failure")
async def record_failure(event: FailureEvent):
    """Record a runtime failure or rollback for an atom."""
    from services.failure_tracker import record_atom_failure
    await record_atom_failure(
        atom_id=event.atom_id, error_type=event.error_type,
        error_message=event.error_message, project=event.project,
        is_rollback=event.is_rollback,
    )
    return {"status": "ok", "atom_id": event.atom_id}


@router.post("/quality")
async def record_quality(event: QualityEvent):
    """Record that an atom was verified working in a given context."""
    from services.quality_tracker import record_atom_verified
    await record_atom_verified(
        atom_id=event.atom_id, context=event.context, success=event.success,
    )
    return {"status": "ok", "atom_id": event.atom_id, "context": event.context}
