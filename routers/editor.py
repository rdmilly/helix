"""Editor Router - Phase 5.1: Semantic Concepts + Expression Assembly

Two layers:
  /api/v1/editor/concepts/*   — Semantic concept extraction and queries (NEW)
  /api/v1/editor/assemble/*   — Concept-based assembly engine (NEW)
  /api/v1/editor/*            — Legacy template endpoints (backward compat)
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from services.concepts import get_concept_service
from services.assembler import get_assembler
from services.editor import get_editor_service

router = APIRouter(prefix="/api/v1/editor", tags=["Editor - Phase 5"])


# ========== REQUEST MODELS ==========

class ConceptExtractRequest(BaseModel):
    atom_id: str = Field(..., description="Atom ID to extract concept from")
    context: str = Field("", description="What was being built/configured when this was captured")
    force: bool = Field(False, description="Re-extract even if already enriched")

class AssembleRequest(BaseModel):
    atom_ids: List[str] = Field(..., description="Atom IDs to assemble")
    mode: str = Field("code", description="Output mode: code, documentation, manifest, compressed")
    params: Optional[Dict[str, Dict[str, Any]]] = Field(None, description="Per-atom parameter overrides")
    title: Optional[str] = Field(None, description="Optional assembly name")

class AssembleByArchetypeRequest(BaseModel):
    archetypes: List[str] = Field(..., description="Archetypes to assemble")
    mode: str = Field("code", description="Output mode")
    params: Optional[Dict[str, Dict[str, Any]]] = Field(None)

class ContextEnrichRequest(BaseModel):
    atom_id: str = Field(..., description="Atom ID to enrich")
    captured_during: Optional[str] = Field(None, description="What was being built")
    tools_observed: Optional[List[str]] = Field(None, description="Tools used during capture")
    project_type: Optional[str] = Field(None, description="Type of project")

# Legacy models
class ExtractRequest(BaseModel):
    atom_id: str
    force: bool = False

class RenderRequest(BaseModel):
    atom_id: str
    parameters: Optional[Dict[str, Any]] = None

class AssembleMoleculeRequest(BaseModel):
    molecule_id: str
    parameters: Optional[Dict[str, Dict[str, Any]]] = None

class AssembleOrganismRequest(BaseModel):
    organism_id: str
    parameters: Optional[Dict[str, Dict[str, Any]]] = None


# ============================================================
# CONCEPT ENDPOINTS (Semantic Concept Layer)
# ============================================================

@router.post("/concepts/extract")
async def extract_concept(req: ConceptExtractRequest):
    """Extract semantic concept from an atom using Haiku analysis.
    
    Understands WHAT the code represents as a reusable concept.
    Writes to EDA namespaces: concept, relationships, composition.
    """
    svc = get_concept_service()
    result = await svc.extract_concept(req.atom_id, context=req.context, force=req.force)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.post("/concepts/extract/all")
async def extract_all_concepts(force: bool = Query(False), context: str = Query("")):
    """Extract semantic concepts for all un-enriched atoms."""
    svc = get_concept_service()
    return await svc.extract_all_concepts(force=force, context=context)

@router.get("/concepts/{atom_id}")
async def get_concept(atom_id: str):
    """Get full semantic concept for an atom (all EDA namespaces)."""
    svc = get_concept_service()
    result = svc.get_concept(atom_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

@router.get("/concepts/archetype/{archetype}")
async def find_by_archetype(archetype: str):
    """Find all atoms matching a semantic archetype."""
    svc = get_concept_service()
    return {"archetype": archetype, "atoms": svc.find_by_archetype(archetype)}

@router.get("/concepts/{atom_id}/composable")
async def find_composable(atom_id: str):
    """Find atoms that can compose with a given atom based on relationships."""
    svc = get_concept_service()
    result = svc.find_composable(atom_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

@router.post("/concepts/context")
async def enrich_context(req: ContextEnrichRequest):
    """Add capture context to an atom. Context accumulates over time."""
    svc = get_concept_service()
    context_data = {}
    if req.captured_during:
        context_data["captured_during"] = req.captured_during
    if req.tools_observed:
        context_data["tools_observed"] = req.tools_observed
    if req.project_type:
        context_data["project_type"] = req.project_type
    result = await svc.enrich_context(req.atom_id, context_data)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


# ============================================================
# ASSEMBLY ENDPOINTS (Concept Composition)
# ============================================================

@router.post("/assemble")
async def assemble_concepts(req: AssembleRequest):
    """Assemble atoms into composed output through section merging.
    
    Modes: code, documentation, manifest, compressed
    """
    asm = get_assembler()
    result = asm.assemble(req.atom_ids, mode=req.mode, params=req.params, title=req.title)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.post("/assemble/archetype")
async def assemble_by_archetype(req: AssembleByArchetypeRequest):
    """Assemble all atoms matching given archetypes."""
    asm = get_assembler()
    result = asm.assemble_by_archetype(req.archetypes, mode=req.mode, params=req.params)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.post("/assemble/molecule/{molecule_id}")
async def assemble_molecule_concepts(molecule_id: str, mode: str = Query("code")):
    """Assemble a molecule using concept-aware section merging."""
    asm = get_assembler()
    result = asm.assemble_molecule(molecule_id, mode=mode)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


# ============================================================
# LEGACY TEMPLATE ENDPOINTS (Backward Compatibility)
# ============================================================

@router.post("/extract")
async def extract_template(req: ExtractRequest):
    """[Legacy] Extract Jinja2 template from atom code."""
    editor = get_editor_service()
    result = await editor.extract_template(req.atom_id, force=req.force)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.post("/extract/all")
async def extract_all_templates(force: bool = Query(False)):
    """[Legacy] Extract templates for all un-templatized atoms."""
    editor = get_editor_service()
    return await editor.extract_all(force=force)

@router.get("/templates")
async def list_templates(templatized_only: bool = Query(False)):
    """List all atoms with template AND concept status."""
    editor = get_editor_service()
    result = editor.list_templates(templatized_only=templatized_only)
    concept_svc = get_concept_service()
    for tmpl in result.get("templates", []):
        concept_data = concept_svc.get_concept(tmpl["atom_id"])
        tmpl["has_concept"] = concept_data.get("has_concept", False)
        if concept_data.get("concept"):
            tmpl["essence"] = concept_data["concept"].get("essence", "")
            tmpl["archetype"] = concept_data["concept"].get("archetype", "")
    return result

@router.get("/templates/{atom_id}")
async def get_template(atom_id: str):
    """[Legacy] Get full template details for a single atom."""
    editor = get_editor_service()
    result = editor.get_template(atom_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

@router.get("/templates/{atom_id}/parameters")
async def get_parameters(atom_id: str):
    """[Legacy] Discover parameters from atom's Jinja2 template."""
    editor = get_editor_service()
    result = editor.discover_parameters(atom_id)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result

@router.post("/generate")
async def generate_expression(req: RenderRequest):
    """[Legacy] Render an atom template with provided parameters."""
    editor = get_editor_service()
    result = editor.render_expression(req.atom_id, req.parameters)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.post("/assemble/legacy/molecule")
async def assemble_molecule_legacy(req: AssembleMoleculeRequest):
    """[Legacy] Assemble molecule using Jinja2 template rendering."""
    editor = get_editor_service()
    result = editor.assemble_molecule(req.molecule_id, req.parameters)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.post("/assemble/legacy/organism")
async def assemble_organism_legacy(req: AssembleOrganismRequest):
    """[Legacy] Assemble organism using Jinja2 template rendering."""
    editor = get_editor_service()
    result = editor.assemble_organism(req.organism_id, req.parameters)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


# ============================================================
# PRINTER ENDPOINTS v2 (Mechanical Expression Engine)
# ============================================================

from services.printer import get_printer, get_expression_table

class PrintRequest(BaseModel):
    atom_id: str = Field(..., description="Atom to print")
    target_framework: str = Field(..., description="Target framework (e.g., python-fastapi, go-stdlib)")
    param_overrides: Optional[Dict[str, str]] = Field(None, description="Parameter value overrides")
    synthesize: bool = Field(False, description="If True, generate skeleton via Haiku when none exists (~$0.003)")

class PrintAssemblyRequest(BaseModel):
    atom_ids: List[str] = Field(..., description="Atoms to assemble and print")
    target_framework: str = Field(..., description="Target framework")
    param_overrides: Optional[Dict[str, Dict[str, str]]] = Field(None)
    synthesize: bool = Field(False, description="Generate missing skeletons via Haiku")

class PolishRequest(BaseModel):
    code: str = Field(..., description="Code to polish")
    target_framework: str = Field(..., description="Target framework for idiom correction")
    instructions: Optional[str] = Field(None, description="Additional cleanup instructions")

@router.post("/printer/print")
async def print_concept(req: PrintRequest):
    """Print a concept as code in target framework. Zero LLM tokens (unless synthesize=true for first-time framework)."""
    printer = get_printer()
    result = await printer.print_concept(req.atom_id, req.target_framework, req.param_overrides, synthesize=req.synthesize)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.post("/printer/assembly")
async def print_assembly(req: PrintAssemblyRequest):
    """Print and assemble multiple concepts. Zero LLM tokens."""
    printer = get_printer()
    return await printer.print_assembly(req.atom_ids, req.target_framework, req.param_overrides, synthesize=req.synthesize)

@router.post("/printer/polish")
async def polish_output(req: PolishRequest):
    """Cheap LLM cleanup pass on mechanical output. ~100-300 tokens."""
    printer = get_printer()
    return await printer.polish(req.code, req.target_framework, req.instructions)

@router.post("/printer/learn/all")
async def learn_all_expressions():
    """Re-learn expression patterns from all enriched atoms (clears and rebuilds table)."""
    printer = get_printer()
    return printer.learn_all()

@router.post("/printer/learn/{atom_id}")
async def learn_expression(atom_id: str):
    """Learn expression pattern from a single enriched atom."""
    printer = get_printer()
    result = printer.learn_expression(atom_id)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result

@router.get("/printer/coverage")
async def expression_coverage():
    """Report on expression table coverage (archetype x framework matrix)."""
    table = get_expression_table()
    return table.coverage_report()

@router.get("/printer/frameworks")
async def list_frameworks(archetype: Optional[str] = Query(None)):
    """List known frameworks in the expression table."""
    table = get_expression_table()
    return {"frameworks": table.list_frameworks(archetype)}
