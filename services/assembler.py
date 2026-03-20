"""Assembler Service — Concept-Based Composition Engine

Composes atoms into molecules and organisms through SECTION MERGING,
not template rendering. Atoms declare which section they belong to
(imports, config, middleware, tools, etc.) and the assembler snaps
them together with import deduplication and dependency resolution.

Assembly modes:
  - code:          Merged source with section organization
  - documentation: Prose description of what the assembly does and why
  - manifest:      YAML/JSON configuration representation
  - compressed:    Shorthand notation for token-efficient references
"""
import json
from services import pg_sync
import logging
import sqlite3
from typing import Dict, Any, Optional, List, Set
from datetime import datetime
from collections import OrderedDict

logger = logging.getLogger(__name__)

SECTION_ORDER = [
    "imports", "config", "models", "middleware",
    "tools", "routes", "startup", "shutdown", "utility",
]


class SectionMerger:
    """Collects code from atoms, merges into named sections with dedup."""
    
    def __init__(self):
        self._sections: Dict[str, List[str]] = {s: [] for s in SECTION_ORDER}
        self._import_lines: Set[str] = set()
        self._dependencies: Set[str] = set()
        self._atoms_used: List[Dict[str, Any]] = []
    
    def add_atom(self, atom_id: str, name: str, code: str,
                 section: str, params: Optional[Dict] = None,
                 concept: Optional[Dict] = None):
        if not code or not code.strip():
            return
        section = section if section in SECTION_ORDER else "utility"
        if section == "imports":
            for line in code.strip().split("\n"):
                line = line.strip()
                if line and line not in self._import_lines:
                    self._import_lines.add(line)
        else:
            self._sections[section].append(code.strip())
        self._atoms_used.append({
            "atom_id": atom_id, "name": name, "section": section,
            "concept_essence": concept.get("essence", "") if concept else "",
        })
    
    def get_merged(self) -> Dict[str, str]:
        result = OrderedDict()
        if self._import_lines:
            sorted_imports = sorted(self._import_lines,
                                   key=lambda x: (0 if x.startswith("import ") else 1, x))
            result["imports"] = "\n".join(sorted_imports)
        for section in SECTION_ORDER:
            if section == "imports":
                continue
            blocks = self._sections.get(section, [])
            if blocks:
                result[section] = "\n\n".join(blocks)
        return result
    
    def get_atoms_used(self) -> List[Dict]:
        return self._atoms_used
    
    def get_full_output(self, separator: str = "\n\n") -> str:
        sections = self.get_merged()
        parts = []
        for section_name, content in sections.items():
            if content.strip():
                parts.append(f"# === {section_name.upper()} ===\n{content}")
        return separator.join(parts)


class ConceptAssembler:
    """Composes atoms into larger patterns through concept-aware assembly.
    
    Instead of rendering Jinja2 templates, the assembler:
    1. Takes a list of atom IDs (or archetypes/concept queries)
    2. Reads their composition metadata (section, relationships)
    3. Resolves dependencies (atoms that require other atoms)
    4. Merges their code into section-organized output
    5. Can express the result in multiple modes (code, docs, config)
    """
    
    def __init__(self, db_path: str = "/app/data/cortex.db"):
        self.db_path = db_path
    
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def assemble(self, atom_ids: List[str], mode: str = "code",
                 params: Optional[Dict[str, Dict]] = None,
                 title: Optional[str] = None) -> Dict[str, Any]:
        params = params or {}
        conn = self._get_conn()
        try:
            atoms = []
            warnings = []
            for atom_id in atom_ids:
                row = conn.execute(
                    "SELECT id, name, code, template, parameters_json, meta FROM atoms WHERE id = ?",
                    (atom_id,)
                ).fetchone()
                if not row:
                    warnings.append(f"Atom {atom_id} not found, skipped")
                    continue
                meta = pg_sync.dejson(row["meta"] or "{}")
                atoms.append({
                    "id": row["id"], "name": row["name"], "code": row["code"],
                    "template": row["template"], "params_json": row["parameters_json"],
                    "meta": meta, "concept": meta.get("concept", {}),
                    "composition": meta.get("composition", {}),
                    "relationships": meta.get("relationships", {}),
                })
            if not atoms:
                return {"error": "No valid atoms found", "warnings": warnings}
            
            dependency_warnings = self._check_dependencies(atoms, conn)
            warnings.extend(dependency_warnings)
            
            if mode == "code":
                result = self._assemble_code(atoms, params)
            elif mode == "documentation":
                result = self._assemble_documentation(atoms)
            elif mode == "manifest":
                result = self._assemble_manifest(atoms, params)
            elif mode == "compressed":
                result = self._assemble_compressed(atoms)
            else:
                return {"error": f"Unknown assembly mode: {mode}"}
            
            result["title"] = title or self._generate_title(atoms)
            result["trace"] = {
                "atoms_requested": len(atom_ids), "atoms_assembled": len(atoms),
                "mode": mode,
                "sections_used": list(result.get("sections", {}).keys()) if "sections" in result else [],
                "assembled_at": datetime.utcnow().isoformat(),
            }
            result["warnings"] = warnings
            result["atoms"] = [{"id": a["id"], "name": a["name"],
                               "essence": a["concept"].get("essence", "")} for a in atoms]
            return result
        finally:
            conn.close()
    
    def assemble_by_archetype(self, archetypes: List[str], mode: str = "code",
                              params: Optional[Dict] = None) -> Dict[str, Any]:
        conn = self._get_conn()
        try:
            atom_ids = []
            for arch in archetypes:
                rows = conn.execute("""
                    SELECT id FROM atoms 
                    WHERE json_extract(meta, '$.concept.archetype') = ?
                """, (arch,)).fetchall()
                atom_ids.extend([r["id"] for r in rows])
            if not atom_ids:
                return {"error": f"No atoms found for archetypes: {archetypes}"}
            return self.assemble(atom_ids, mode=mode, params=params,
                               title=f"Assembly: {' + '.join(archetypes)}")
        finally:
            conn.close()
    
    def assemble_molecule(self, molecule_id: str, mode: str = "code",
                          params: Optional[Dict] = None) -> Dict[str, Any]:
        conn = self._get_conn()
        try:
            mol = conn.execute(
                "SELECT id, name, description, atom_ids_json, meta FROM molecules WHERE id = ?",
                (molecule_id,)
            ).fetchone()
            if not mol:
                return {"error": f"Molecule {molecule_id} not found"}
            atom_ids = pg_sync.dejson(mol["atom_ids_json"] or "[]")
            return self.assemble(atom_ids, mode=mode, params=params,
                               title=f"Molecule: {mol['name']}")
        finally:
            conn.close()
    
    def _assemble_code(self, atoms: List[Dict], params: Dict) -> Dict[str, Any]:
        merger = SectionMerger()
        for atom in atoms:
            comp = atom.get("composition", {})
            section = comp.get("primary_section", "utility")
            code = atom["code"]
            template = atom.get("template")
            if template and template != code:
                rendered = self._try_render_template(template, atom, params.get(atom["id"], {}))
                if rendered:
                    code = rendered
            merger.add_atom(atom_id=atom["id"], name=atom["name"], code=code,
                           section=section, concept=atom.get("concept"))
        sections = merger.get_merged()
        return {"mode": "code", "output": merger.get_full_output(),
                "sections": sections, "atoms_assembled": merger.get_atoms_used()}
    
    def _assemble_documentation(self, atoms: List[Dict]) -> Dict[str, Any]:
        parts = []
        archetypes = set()
        for atom in atoms:
            arch = atom.get("concept", {}).get("archetype", "")
            if arch:
                archetypes.add(arch)
        parts.append(f"## Assembly: {', '.join(archetypes) or 'Mixed Concepts'}\n")
        parts.append("### Components\n")
        for atom in atoms:
            concept = atom.get("concept", {})
            understanding = concept.get("understanding", {})
            parts.append(f"**{atom['name']}** — {concept.get('essence', 'No description')}")
            if understanding.get("why"):
                parts.append(f"  *Purpose:* {understanding['why']}")
            parts.append("")
        all_requires = set()
        all_integrates = set()
        for atom in atoms:
            rels = atom.get("relationships", {})
            all_requires.update(rels.get("requires", []))
            all_integrates.update(rels.get("integrates_with", []))
        if all_requires:
            parts.append(f"### Dependencies\nThis assembly requires: {', '.join(all_requires)}\n")
        if all_integrates:
            parts.append(f"### Integration Points\nCommonly pairs with: {', '.join(all_integrates)}\n")
        all_gains = []
        all_costs = []
        for atom in atoms:
            tradeoffs = atom.get("concept", {}).get("understanding", {}).get("tradeoffs", {})
            all_gains.extend(tradeoffs.get("gains", []))
            all_costs.extend(tradeoffs.get("costs", []))
        if all_gains or all_costs:
            parts.append("### Tradeoffs")
            if all_gains:
                parts.append(f"**Gains:** {'; '.join(set(all_gains))}")
            if all_costs:
                parts.append(f"**Costs:** {'; '.join(set(all_costs))}")
        return {"mode": "documentation", "output": "\n".join(parts)}
    
    def _assemble_manifest(self, atoms: List[Dict], params: Dict) -> Dict[str, Any]:
        manifest = {"assembly": {"components": [], "dependencies": []}}
        all_deps = set()
        for atom in atoms:
            comp = atom.get("composition", {})
            concept = atom.get("concept", {})
            rels = atom.get("relationships", {})
            component = {
                "name": atom["name"],
                "archetype": concept.get("archetype", "unknown"),
                "section": comp.get("primary_section", "utility"),
                "parameters": {},
            }
            for p in comp.get("parameters", []):
                val = params.get(atom["id"], {}).get(p["name"], p.get("default"))
                component["parameters"][p["name"]] = val
            manifest["assembly"]["components"].append(component)
            all_deps.update(rels.get("requires", []))
        manifest["assembly"]["dependencies"] = list(all_deps)
        return {"mode": "manifest", "output": json.dumps(manifest, indent=2), "manifest": manifest}
    
    def _assemble_compressed(self, atoms: List[Dict]) -> Dict[str, Any]:
        parts = []
        for atom in atoms:
            concept = atom.get("concept", {})
            comp = atom.get("composition", {})
            arch = concept.get("archetype", "?")[:4]
            section = comp.get("primary_section", "util")[:4]
            param_count = len(comp.get("parameters", []))
            short = f"{arch}:{atom['name']}[{section}]"
            if param_count > 0:
                param_names = [p.get("name", "?") for p in comp.get("parameters", [])]
                short += f"({','.join(param_names)})"
            parts.append(short)
        return {"mode": "compressed", "output": " + ".join(parts),
                "notation": parts, "token_estimate": sum(len(p.split()) for p in parts)}
    
    def _check_dependencies(self, atoms: List[Dict], conn) -> List[str]:
        warnings = []
        assembly_archetypes = set()
        assembly_essences = set()
        for atom in atoms:
            concept = atom.get("concept", {})
            assembly_archetypes.add(concept.get("archetype", ""))
            assembly_essences.add(concept.get("essence", "").lower())
        for atom in atoms:
            rels = atom.get("relationships", {})
            for req in rels.get("requires", []):
                found = any(req.lower() in a.lower() for a in assembly_archetypes)
                if not found:
                    found = any(req.lower() in e for e in assembly_essences)
                if not found:
                    warnings.append(f"Atom '{atom['name']}' requires '{req}' which may not be in this assembly")
        return warnings
    
    def _try_render_template(self, template: str, atom: Dict, params: Dict) -> Optional[str]:
        try:
            from jinja2 import Environment, BaseLoader
            env = Environment(loader=BaseLoader(), keep_trailing_newline=True)
            comp_params = atom.get("composition", {}).get("parameters", [])
            defaults = {p["name"]: p.get("default") for p in comp_params if p.get("name")}
            flat_params = pg_sync.dejson(atom.get("params_json") or "[]")
            for p in flat_params:
                if p.get("name") and p.get("default") is not None:
                    defaults[p["name"]] = p["default"]
            render_params = {**defaults, **params}
            return env.from_string(template).render(**render_params)
        except Exception as e:
            logger.debug(f"Template render fallback for {atom['id']}: {e}")
            return None
    
    def _generate_title(self, atoms: List[Dict]) -> str:
        archetypes = set()
        for a in atoms:
            arch = a.get("concept", {}).get("archetype", "")
            if arch:
                archetypes.add(arch)
        if archetypes:
            return f"Assembly: {' + '.join(sorted(archetypes))}"
        names = [a["name"] for a in atoms[:3]]
        return f"Assembly: {', '.join(names)}" + ("..." if len(atoms) > 3 else "")


_assembler = None

def get_assembler() -> ConceptAssembler:
    global _assembler
    if _assembler is None:
        _assembler = ConceptAssembler()
    return _assembler
