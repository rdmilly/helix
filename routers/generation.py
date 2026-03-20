"""Generation Router — Editor v1 E2+E3+E4 Endpoints

POST /api/v1/generation/classify   - E2: intent classification only (fast, no DB)
POST /api/v1/generation/plan       - E2+E3+E4: full plan with mode selection
POST /api/v1/generation/execute    - E4+: execute a plan (Mode 1 direct assembly)
"""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.generation import (
    get_generation_service,
    MODE_1, MODE_2, MODE_3, MODE_S,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/generation", tags=["Generation - Editor v1"])


# === Request models ===

class ClassifyRequest(BaseModel):
    query: str = Field(..., description="Natural language generation request")


class PlanRequest(BaseModel):
    query: str = Field(..., description="Natural language generation request")
    session_id: Optional[str] = Field(default=None, description="Active session ID")
    mode_override: Optional[str] = Field(
        default=None,
        description=f"Force mode: {MODE_1}|{MODE_2}|{MODE_3}|{MODE_S}",
    )


class ExecuteRequest(BaseModel):
    """Execute a generation plan.

    For Mode 1: directly assembles top candidate atoms.
    For Mode 2/3: returns context + scaffold hint for LLM use.
    """
    query: str = Field(..., description="Natural language generation request")
    session_id: Optional[str] = Field(default=None)
    mode_override: Optional[str] = Field(default=None)
    target_framework: str = Field(
        default="python-fastapi",
        description="Target framework for printer assembly",
    )
    synthesize: bool = Field(
        default=False,
        description="Allow Haiku synthesis for missing expressions (~$0.003)",
    )


# === Endpoints ===

@router.post("/classify")
async def classify_intent(req: ClassifyRequest):
    """E2: Classify user intent without touching the database.

    Fast endpoint (< 5ms) that returns:
    - domain + confidence
    - complexity estimate
    - cleaned query for atom search
    - keywords

    Useful for UI hints and pre-flight checks.
    """
    svc = get_generation_service()
    return svc.classify_intent(req.query)


@router.post("/plan")
async def plan_generation(req: PlanRequest):
    """E2+E3+E4: Full generation plan with mode selection.

    Runs the complete classification + atom pre-retrieval + mode selection
    pipeline. Returns:
    - intent classification
    - candidate atoms with coverage estimate
    - selected mode + reason
    - assembly suggestion (Mode 1 only)
    - plan steps + estimated token cost

    This is the recommended first call before any generation. Use the
    returned mode and context to route the actual generation.
    """
    svc = get_generation_service()
    result = await svc.plan_generation(
        query=req.query,
        session_id=req.session_id,
        mode_override=req.mode_override,
    )
    return result


@router.post("/execute")
async def execute_generation(req: ExecuteRequest):
    """Execute generation from a plan.

    Mode 1: If coverage is high enough, directly assembles the top candidate
    atoms using the printer (zero/minimal LLM tokens).

    Mode 2/3: Returns the plan context + scaffold hint ready for LLM use.
    The caller should inject this into the LLM context via Tier 1.

    Mode S: Not yet implemented (C1 compound registry required).
    """
    svc = get_generation_service()

    # Get the plan first
    plan = await svc.plan_generation(
        query=req.query,
        session_id=req.session_id,
        mode_override=req.mode_override,
    )

    mode = plan["mode"]
    context = plan["context"]
    suggestion = context.get("assembly_suggestion")

    # Mode 1 with a direct assembly suggestion: try printer
    if mode == MODE_1 and suggestion and suggestion.get("atom_ids"):
        try:
            from services.printer import get_printer
            printer = get_printer()
            assembly_result = await printer.print_assembly(
                atom_ids=suggestion["atom_ids"],
                target_framework=req.target_framework,
                param_overrides=None,
                synthesize=req.synthesize,
            )
            return {
                "mode": MODE_1,
                "executed": True,
                "method": "direct_assembly",
                "result": assembly_result,
                "plan": plan,
            }
        except Exception as e:
            logger.warning(f"Mode 1 direct assembly failed, falling back: {e}")
            # Fall through to context return

    # Mode 2/3/S or Mode 1 fallback: return context for LLM use
    return {
        "mode": mode,
        "executed": False,
        "method": "context_for_llm",
        "scaffold_hint": context.get("scaffold_hint", ""),
        "top_atoms": context.get("top_atoms", [])[:8],
        "plan": plan,
        "note": (
            f"Mode {mode}: inject scaffold_hint + top_atoms into your LLM context. "
            f"Coverage: {plan['candidates']['coverage_estimate']:.0%}"
        ),
    }


@router.get("/coverage")
async def check_coverage(query: str):
    """Quick coverage check: how well does the atom library cover this request?

    Returns coverage estimate + top matching atoms. Useful for testing
    library coverage before running a full plan.
    """
    svc = get_generation_service()
    intent = svc.classify_intent(query)
    candidates = await svc.retrieve_candidate_atoms(
        query=intent["query_cleaned"],
        domain=intent["domain"],
        limit=15,
    )
    return {
        "query": query,
        "domain": intent["domain"],
        "coverage": candidates["coverage_estimate"],
        "strong_matches": candidates["strong_matches"],
        "partial_matches": candidates["partial_matches"],
        "top_atoms": candidates["atoms"][:5],
    }
