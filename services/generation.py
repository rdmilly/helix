"""Generation Service — Editor v1 Orchestrator (E2 + E3 + E4)

The intent classifier, coverage estimator, and mode selector that drives
Editor v1 generation. Takes a natural language request and routes it through
the correct generation mode using the existing atom/molecule/printer pipeline.

Modes (from build order):
  Mode 1 (coverage >= 0.90): Scaffold + fill. ~80% pre-built. LLM fills slots.
  Mode 2 (coverage 0.60-0.90): Constrained generation. ~50% pre-built.
  Mode 3 (coverage < 0.60): Reference-only. Free gen + register output.
  Mode S (compound available, high coverage): Autocatalytic synthesis.

E2 - Intent classifier: domain detection + complexity estimate
E3 - Pre-retrieval query: Synapse atom search for candidate atoms
E4 - Mode selection + context assembly
"""
import asyncio
import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Mode constants
MODE_1 = "mode_1"  # Scaffold fill (>= 0.90 coverage)
MODE_2 = "mode_2"  # Constrained gen (0.60-0.90)
MODE_3 = "mode_3"  # Reference only (< 0.60)
MODE_S = "mode_s"  # Autocatalytic synthesis (compound available)

# Domain detection keywords
DOMAIN_PATTERNS: Dict[str, List[str]] = {
    "python-fastapi": [
        "fastapi", "router", "endpoint", "httpexception", "pydantic",
        "basemodel", "apirouter", "query param", "path param", "depends",
    ],
    "python-general": [
        "python", "def ", "class ", "async def", "import", "dataclass",
        "pytest", "typing", "decorator",
    ],
    "docker-compose": [
        "docker", "compose", "container", "service", "dockerfile",
        "volumes", "networks", "healthcheck",
    ],
    "mcp-server": [
        "mcp", "fastmcp", "tool", "resource", "server", "stdio",
        "streamable", "mcp server",
    ],
    "traefik": [
        "traefik", "router", "middleware", "entrypoint", "certresolver",
        "tls", "label",
    ],
    "n8n": [
        "n8n", "workflow", "node", "trigger", "webhook", "automation",
    ],
    "javascript": [
        "javascript", "js", "const ", "let ", "async function", "fetch",
        "promise", "arrow function", "class ",
    ],
    "bash": [
        "bash", "shell", "script", "#!/", "curl", "grep", "awk",
        "sed", "systemd",
    ],
}

# Complexity signals in the query
COMPLEXITY_SIMPLE = ["simple", "basic", "quick", "just", "small", "single"]
COMPLEXITY_MEDIUM = ["with", "that", "including", "and", "plus", "also"]
COMPLEXITY_HIGH = [
    "full", "complete", "complex", "comprehensive", "entire",
    "with auth", "with tests", "production",
]


def _detect_domain(query: str) -> Tuple[str, float]:
    """Detect the primary domain from a generation query.

    Returns (domain_name, confidence) where confidence is 0.0-1.0.
    Defaults to 'python-general' if no domain is detected.
    """
    query_lower = query.lower()
    scores: Dict[str, int] = {}

    for domain, keywords in DOMAIN_PATTERNS.items():
        hits = sum(1 for kw in keywords if kw in query_lower)
        if hits:
            scores[domain] = hits

    if not scores:
        return "python-general", 0.3  # Default with low confidence

    best_domain = max(scores, key=scores.__getitem__)
    max_hits = scores[best_domain]
    total_keywords = len(DOMAIN_PATTERNS[best_domain])
    confidence = min(0.95, max_hits / max(total_keywords * 0.3, 1))

    return best_domain, round(confidence, 2)


def _estimate_complexity(query: str) -> str:
    """Estimate generation complexity from query keywords.

    Returns 'simple' | 'medium' | 'high'.
    """
    query_lower = query.lower()
    high = sum(1 for kw in COMPLEXITY_HIGH if kw in query_lower)
    medium = sum(1 for kw in COMPLEXITY_MEDIUM if kw in query_lower)
    simple = sum(1 for kw in COMPLEXITY_SIMPLE if kw in query_lower)

    word_count = len(query.split())

    if high >= 1 or word_count > 25:
        return "high"
    if medium >= 2 or word_count > 12:
        return "medium"
    return "simple"


def _select_mode(coverage: float, has_compound: bool = False) -> str:
    """Select generation mode based on atom library coverage."""
    if has_compound and coverage >= 0.70:
        return MODE_S
    if coverage >= 0.90:
        return MODE_1
    if coverage >= 0.60:
        return MODE_2
    return MODE_3


class GenerationService:
    """Editor v1 generation orchestrator.

    Classifies intent, estimates coverage, selects mode, and
    assembles context for the appropriate generation pipeline.
    """

    def __init__(self):
        self._chromadb = None
        self._synapse = None

    def _get_chromadb(self):
        if self._chromadb is None:
            from services.chromadb import get_chromadb_service
            self._chromadb = get_chromadb_service()
        return self._chromadb

    def _get_synapse(self):
        if self._synapse is None:
            from services.synapse import get_synapse_service
            self._synapse = get_synapse_service()
        return self._synapse

    # ============================================================
    # E2: Intent Classification
    # ============================================================

    def classify_intent(
        self,
        query: str,
    ) -> Dict[str, Any]:
        """Classify user intent for generation routing.

        Returns domain, complexity, estimated mode, and a
        cleaned search query for atom pre-retrieval.

        Args:
            query: Natural language generation request.

        Returns:
            {
                domain: str,
                domain_confidence: float,
                complexity: simple|medium|high,
                query_cleaned: str,
                keywords: [str],
            }
        """
        domain, domain_conf = _detect_domain(query)
        complexity = _estimate_complexity(query)

        # Extract keywords: nouns + verbs, strip filler words
        _filler = {
            "a", "an", "the", "is", "are", "that", "with", "for", "to",
            "of", "in", "on", "at", "by", "from", "me", "my", "i", "can",
            "you", "please", "build", "create", "make", "generate", "write",
            "add", "give",
        }
        words = re.findall(r'\b[a-z][a-z0-9_-]{2,}\b', query.lower())
        keywords = [w for w in words if w not in _filler][:10]

        # Build a cleaned query for atom search (shorter, keyword-dense)
        query_cleaned = " ".join(keywords[:6]) if keywords else query[:100]

        return {
            "domain": domain,
            "domain_confidence": domain_conf,
            "complexity": complexity,
            "query_cleaned": query_cleaned,
            "keywords": keywords,
        }

    # ============================================================
    # E3: Pre-retrieval Query
    # ============================================================

    async def retrieve_candidate_atoms(
        self,
        query: str,
        domain: str,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """Query the atom library for candidates matching this generation request.

        Runs a semantic search against the ChromaDB atoms collection,
        then estimates coverage as: (matches with distance < 0.6) / limit.

        Args:
            query:  Cleaned search query from classify_intent.
            domain: Detected domain for coverage scoring.
            limit:  Max atoms to retrieve.

        Returns:
            {
                atoms: [{id, name, distance, category}],
                coverage_estimate: float,  # 0.0-1.0
                strong_matches: int,       # distance < 0.40
                partial_matches: int,      # 0.40-0.60
                weak_matches: int,         # 0.60-1.0
            }
        """
        chromadb = self._get_chromadb()
        synapse = self._get_synapse()

        try:
            results = await chromadb.search_similar(
                query=query,
                collection_base="atoms",
                limit=limit,
            )
        except Exception as e:
            logger.warning(f"Atom pre-retrieval failed: {e}")
            results = []

        atoms = []
        strong = partial = weak = 0

        for r in (results or []):
            dist = r.get("distance", 1.0)
            atom_id = r.get("id", "")
            detail = synapse._get_atom_detail(atom_id)
            atoms.append({
                "id": atom_id,
                "name": detail.get("name", atom_id) if detail else atom_id,
                "category": detail.get("category", "") if detail else "",
                "distance": round(dist, 4),
            })
            if dist < 0.40:
                strong += 1
            elif dist < 0.60:
                partial += 1
            else:
                weak += 1

        # Coverage estimate: strong matches weighted most
        weighted = (strong * 1.0 + partial * 0.5) / max(limit * 0.4, 1)
        coverage = round(min(1.0, weighted), 3)

        return {
            "atoms": atoms,
            "coverage_estimate": coverage,
            "strong_matches": strong,
            "partial_matches": partial,
            "weak_matches": weak,
            "total_candidates": len(atoms),
            "domain": domain,
        }

    # ============================================================
    # E4: Mode Selection + Context Assembly
    # ============================================================

    async def plan_generation(
        self,
        query: str,
        session_id: Optional[str] = None,
        mode_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Full E2+E3+E4 pipeline: classify, retrieve, select mode, assemble context.

        This is the entry point for the generation router. Returns everything
        needed to either:
          - Directly assemble from atoms (Mode 1/S)
          - Guide constrained generation (Mode 2)
          - Provide reference context for free generation (Mode 3)

        Args:
            query:         User's natural language generation request.
            session_id:    Active session (for context enrichment).
            mode_override: Force a specific mode (testing/debug).

        Returns:
            {
                query: str,
                intent: {domain, complexity, ...},
                candidates: {atoms, coverage_estimate, ...},
                mode: mode_1|mode_2|mode_3|mode_s,
                mode_reason: str,
                context: {top_atoms, scaffold_hint, assembly_suggestion},
                plan: {steps: [str], estimated_tokens: int},
            }
        """
        # E2: classify intent
        intent = self.classify_intent(query)

        # E3: pre-retrieval
        candidates = await self.retrieve_candidate_atoms(
            query=intent["query_cleaned"],
            domain=intent["domain"],
            limit=20,
        )
        coverage = candidates["coverage_estimate"]

        # E4: mode selection
        # Check if any compound exists for this domain (Mode S gate)
        has_compound = False  # TODO: query compound registry when C1 is built

        if mode_override and mode_override in (MODE_1, MODE_2, MODE_3, MODE_S):
            mode = mode_override
            mode_reason = f"overridden to {mode_override}"
        else:
            mode = _select_mode(coverage, has_compound)
            mode_reason = (
                f"coverage={coverage:.2f} "
                f"(strong={candidates['strong_matches']} "
                f"partial={candidates['partial_matches']})"
            )

        # Build the context block for this mode
        top_atoms = candidates["atoms"][:10]
        scaffold_hint = self._build_scaffold_hint(mode, intent, top_atoms)
        assembly_suggestion = self._build_assembly_suggestion(mode, intent, top_atoms)

        # Estimate token cost of this generation
        est_tokens = {
            MODE_1: 200,   # slot filling is cheap
            MODE_2: 500,   # constrained gen
            MODE_3: 800,   # free gen
            MODE_S: 300,   # synthesis from grammar
        }.get(mode, 500)

        plan_steps = {
            MODE_1: [
                "Select best-matching scaffold from atom candidates",
                "Identify unfilled slots from complexity estimate",
                "Fill slots via LLM with user context",
                "Register output atoms if new patterns emerge",
            ],
            MODE_2: [
                "Present top atom candidates as constraints",
                "Guide LLM to extend/compose from candidates",
                "Validate output against atom fingerprints",
                "Register new atoms from output",
            ],
            MODE_3: [
                "Inject top atom candidates as reference context",
                "Free-generate from LLM",
                "Post-generation: extract + register new atoms",
                "Enrich coverage for next generation",
            ],
            MODE_S: [
                "Decompose request into compound grammar",
                "Synthesize from molecules -> atoms",
                "Register synthesis path on success",
            ],
        }.get(mode, [])

        return {
            "query": query,
            "session_id": session_id,
            "intent": intent,
            "candidates": candidates,
            "mode": mode,
            "mode_reason": mode_reason,
            "context": {
                "top_atoms": top_atoms,
                "scaffold_hint": scaffold_hint,
                "assembly_suggestion": assembly_suggestion,
            },
            "plan": {
                "steps": plan_steps,
                "estimated_tokens": est_tokens,
            },
            "planned_at": datetime.utcnow().isoformat(),
        }

    def _build_scaffold_hint(
        self,
        mode: str,
        intent: Dict[str, Any],
        top_atoms: List[Dict[str, Any]],
    ) -> str:
        """Build a scaffold hint string for Mode 1/2 generation."""
        if not top_atoms:
            return ""
        if mode == MODE_3:
            return ""  # Mode 3 doesn't use scaffold

        names = [a["name"] for a in top_atoms[:5] if a.get("name")]
        domain = intent.get("domain", "")

        if mode == MODE_1:
            return (
                f"Scaffold from atoms: {', '.join(names)}. "
                f"Domain: {domain}. Fill slots for: {intent.get('query_cleaned', '')}"
            )
        elif mode == MODE_2:
            return (
                f"Extend/compose from: {', '.join(names)}. "
                f"Domain: {domain}."
            )
        elif mode == MODE_S:
            return f"Synthesize {domain} from grammar. Seed atoms: {', '.join(names[:3])}"
        return ""

    def _build_assembly_suggestion(
        self,
        mode: str,
        intent: Dict[str, Any],
        top_atoms: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Suggest a direct assembly if Mode 1 has high-confidence atoms."""
        if mode != MODE_1 or not top_atoms:
            return None

        # For Mode 1: suggest assembling the top atoms directly
        strong_atoms = [a for a in top_atoms if a.get("distance", 1.0) < 0.35]
        if not strong_atoms:
            return None

        return {
            "action": "assemble",
            "atom_ids": [a["id"] for a in strong_atoms[:5]],
            "mode": "code",
            "note": f"{len(strong_atoms)} high-confidence atoms (dist < 0.35) — try direct assembly first",
        }


# ============================================================
# Global singleton
# ============================================================

_generation_service: Optional[GenerationService] = None


def get_generation_service() -> GenerationService:
    """Get generation service instance (lazy init)."""
    global _generation_service
    if _generation_service is None:
        _generation_service = GenerationService()
    return _generation_service
