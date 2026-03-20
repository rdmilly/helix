"""Editor Service — Phase 5: Template Extraction & Expression Generation

Transforms raw atom code into parameterized Jinja2 templates,
discovers parameters, and renders expressions with user-provided values.

Uses Haiku for intelligent parameter identification:
- Detects concrete values that should be configurable
- Generates meaningful parameter names and descriptions
- Preserves code structure while adding flexibility

Assembly layer composes multiple atoms (molecules) and molecules (organisms)
into complete, rendered code files.
"""
import json
from services import pg_sync
import re
import logging
import sqlite3
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Jinja2 import with fallback
try:
    from jinja2 import Environment, BaseLoader, TemplateSyntaxError, meta as jinja2_meta
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False
    logger.warning("Jinja2 not installed — template rendering disabled")


EXTRACT_TEMPLATE_PROMPT = """Analyze this code and identify concrete values that should be configurable parameters.

CODE:
```
{code}
```

Return a JSON object with:
1. "template": The code with concrete values replaced by Jinja2 template variables ({{ var_name }})
2. "parameters": Array of parameter objects, each with:
   - "name": snake_case variable name
   - "type": "string" | "int" | "float" | "bool" | "list"
   - "default": the original concrete value
   - "description": what this parameter controls

Rules:
- Replace string literals that are configuration (paths, header names, error messages)
- Replace numeric literals that are configuration (status codes, limits, thresholds)
- Do NOT replace structural code elements (variable names, logic operators, function signatures)
- Do NOT replace True/False/None when used as sentinel values
- Use Jinja2 default filters: {{ var_name | default("original_value") }}
- Keep parameter count reasonable (2-6 per function)
- If the code has NO meaningful parameterizable values, return it unchanged with empty parameters

Respond with ONLY valid JSON, no explanation."""


class EditorService:
    """Template extraction, parameter discovery, and expression rendering."""

    def __init__(self, db_path: str = "/app/data/cortex.db"):
        self.db_path = db_path
        self._jinja_env = None
        if JINJA2_AVAILABLE:
            self._jinja_env = Environment(
                loader=BaseLoader(),
                keep_trailing_newline=True,
                undefined=__import__('jinja2').Undefined
            )

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ========== TEMPLATE EXTRACTION ==========

    async def extract_template(self, atom_id: str, force: bool = False) -> Dict[str, Any]:
        """Use Haiku to extract a parameterized template from atom code.

        Args:
            atom_id: The atom to templatize
            force: Re-extract even if template already differs from code

        Returns:
            Dict with template, parameters, and extraction metadata
        """
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT id, name, code, template, parameters_json, meta FROM atoms WHERE id = ?",
                (atom_id,)
            ).fetchone()

            if not row:
                return {"error": f"Atom {atom_id} not found"}

            code = row["code"]
            existing_template = row["template"]
            existing_params = row["parameters_json"]

            # Skip if already templatized (unless force)
            if not force and existing_template and existing_template != code:
                params = pg_sync.dejson(existing_params) if existing_params else {}
                return {
                    "atom_id": atom_id,
                    "name": row["name"],
                    "status": "already_templatized",
                    "template": existing_template,
                    "parameters": params,
                    "parameter_count": len(params) if isinstance(params, list) else 0
                }

            # Call Haiku to analyze and templatize
            result = await self._haiku_extract(code)

            if "error" in result:
                return {"atom_id": atom_id, "error": result["error"]}

            template_str = result.get("template", code)
            parameters = result.get("parameters", [])

            # Validate template renders without error
            if JINJA2_AVAILABLE:
                try:
                    # Test render with defaults
                    defaults = {p["name"]: p["default"] for p in parameters}
                    tmpl = self._jinja_env.from_string(template_str)
                    rendered = tmpl.render(**defaults)
                    # Sanity check — rendered with defaults should be close to original
                except TemplateSyntaxError as e:
                    logger.warning(f"Template syntax error for {atom_id}: {e}")
                    return {
                        "atom_id": atom_id,
                        "error": f"Generated template has syntax error: {e}",
                        "raw_template": template_str
                    }

            # Save to database
            params_json = json.dumps(parameters)
            meta = pg_sync.dejson(row["meta"] or "{}")
            meta.setdefault("structural", {})
            meta["structural"]["template_format"] = "jinja2"
            meta["structural"]["template_version"] = "3.1"
            meta["structural"]["templatized_at"] = datetime.utcnow().isoformat()
            meta["structural"]["parameter_count"] = len(parameters)

            conn.execute(
                """UPDATE atoms SET template = ?, parameters_json = ?, meta = ?
                   WHERE id = ?""",
                (template_str, params_json, json.dumps(meta), atom_id)
            )
            conn.commit()

            # Log meta event
            conn.execute(
                """INSERT INTO meta_events (id, target_table, target_id, namespace, action, new_value, written_by)
                   VALUES (?, 'atoms', ?, 'structural', 'templatize', ?, 'editor_v1')""",
                (f"evt_{atom_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                 atom_id, json.dumps({"template_format": "jinja2", "parameters": len(parameters)}))
            )
            conn.commit()

            return {
                "atom_id": atom_id,
                "name": row["name"],
                "status": "templatized",
                "template": template_str,
                "parameters": parameters,
                "parameter_count": len(parameters)
            }
        finally:
            conn.close()

    async def extract_all(self, force: bool = False) -> Dict[str, Any]:
        """Extract templates for all atoms that haven't been templatized yet."""
        conn = self._get_conn()
        try:
            if force:
                rows = conn.execute("SELECT id FROM atoms").fetchall()
            else:
                # Only atoms where template == code (not yet parameterized)
                rows = conn.execute(
                    "SELECT id FROM atoms WHERE template = code OR template IS NULL"
                ).fetchall()

            results = []
            for row in rows:
                result = await self.extract_template(row["id"], force=force)
                results.append(result)

            succeeded = sum(1 for r in results if r.get("status") in ("templatized", "already_templatized"))
            failed = sum(1 for r in results if "error" in r)

            return {
                "total": len(results),
                "templatized": succeeded,
                "failed": failed,
                "results": results
            }
        finally:
            conn.close()

    async def _haiku_extract(self, code: str) -> Dict[str, Any]:
        """Call Haiku API to extract template parameters from code."""
        try:
            import httpx

            prompt = EXTRACT_TEMPLATE_PROMPT.format(code=code)

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": self._get_api_key(),
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json"
                    },
                    json={
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 2048,
                        "messages": [{"role": "user", "content": prompt}]
                    }
                )

            if response.status_code != 200:
                return {"error": f"Haiku API error: {response.status_code}"}

            data = response.json()
            text = data["content"][0]["text"]

            # Parse JSON from response (strip markdown fences if present)
            text = text.strip()
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\s*', '', text)
                text = re.sub(r'\s*```$', '', text)

            result = pg_sync.dejson(text)
            return result

        except json.JSONDecodeError as e:
            logger.error(f"Haiku returned invalid JSON: {e}")
            return {"error": f"Invalid JSON from Haiku: {e}"}
        except Exception as e:
            logger.error(f"Haiku extraction failed: {e}")
            return {"error": str(e)}

    def _get_api_key(self) -> str:
        """Get Anthropic API key from environment."""
        import os
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not key:
            raise ValueError("ANTHROPIC_API_KEY not set")
        return key

    # ========== PARAMETER DISCOVERY ==========

    def discover_parameters(self, atom_id: str) -> Dict[str, Any]:
        """Parse a templatized atom and return its parameter definitions.

        Works by scanning the Jinja2 template for {{ var }} and {{ var | default(...) }}
        patterns, cross-referencing with stored parameters_json.
        """
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT id, name, template, parameters_json FROM atoms WHERE id = ?",
                (atom_id,)
            ).fetchone()

            if not row:
                return {"error": f"Atom {atom_id} not found"}

            template_str = row["template"]
            stored_params = pg_sync.dejson(row["parameters_json"] or "[]")

            # Parse Jinja2 template for undeclared variables
            discovered = []
            if JINJA2_AVAILABLE and template_str:
                try:
                    ast = self._jinja_env.parse(template_str)
                    undeclared = jinja2_meta.find_undeclared_variables(ast)

                    # Build map from stored params
                    param_map = {p["name"]: p for p in stored_params} if isinstance(stored_params, list) else {}

                    for var_name in sorted(undeclared):
                        if var_name in param_map:
                            discovered.append(param_map[var_name])
                        else:
                            # Extract default from template if present
                            default_match = re.search(
                                rf'{{{{\s*{re.escape(var_name)}\s*\|\s*default\(([^)]+)\)\s*}}}}',
                                template_str
                            )
                            default_val = default_match.group(1).strip('"\'') if default_match else None
                            discovered.append({
                                "name": var_name,
                                "type": "string",
                                "default": default_val,
                                "description": f"Parameter: {var_name}"
                            })
                except TemplateSyntaxError:
                    discovered = stored_params if isinstance(stored_params, list) else []

            return {
                "atom_id": atom_id,
                "name": row["name"],
                "parameters": discovered,
                "parameter_count": len(discovered),
                "template_available": bool(template_str and template_str != row["template"])
            }
        finally:
            conn.close()

    # ========== EXPRESSION RENDERING ==========

    def render_expression(self, atom_id: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Render an atom template with provided parameters.

        Args:
            atom_id: Atom to render
            params: Parameter values (missing params use defaults)

        Returns:
            Dict with rendered code, used parameters, and any warnings
        """
        if not JINJA2_AVAILABLE:
            return {"error": "Jinja2 not installed — rendering unavailable"}

        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT id, name, code, template, parameters_json, meta FROM atoms WHERE id = ?",
                (atom_id,)
            ).fetchone()

            if not row:
                return {"error": f"Atom {atom_id} not found"}

            template_str = row["template"] or row["code"]
            stored_params = pg_sync.dejson(row["parameters_json"] or "[]")
            params = params or {}

            # Build full param dict with defaults for missing values
            defaults = {p["name"]: p["default"] for p in stored_params} if isinstance(stored_params, list) else {}
            render_params = {**defaults, **params}

            warnings = []

            # Check for unknown params
            known_names = set(defaults.keys())
            for key in params:
                if key not in known_names and known_names:
                    warnings.append(f"Unknown parameter: {key}")

            try:
                tmpl = self._jinja_env.from_string(template_str)
                rendered = tmpl.render(**render_params)
            except TemplateSyntaxError as e:
                return {"error": f"Template syntax error: {e}"}
            except Exception as e:
                return {"error": f"Render error: {e}"}

            # Track render in meta
            meta = pg_sync.dejson(row["meta"] if "meta" in row.keys() else "{}")
            render_count = meta.get("editor", {}).get("render_count", 0) + 1
            meta.setdefault("editor", {})["render_count"] = render_count
            meta["editor"]["last_rendered"] = datetime.utcnow().isoformat()

            conn.execute("UPDATE atoms SET meta = ? WHERE id = ?", (json.dumps(meta), atom_id))
            conn.commit()

            return {
                "atom_id": atom_id,
                "name": row["name"],
                "rendered": rendered,
                "parameters_used": render_params,
                "warnings": warnings,
                "render_count": render_count
            }
        finally:
            conn.close()

    # ========== ASSEMBLY ==========

    def assemble_molecule(self, molecule_id: str, params: Optional[Dict[str, Dict]] = None) -> Dict[str, Any]:
        """Assemble a molecule by rendering all its constituent atoms.

        Args:
            molecule_id: Molecule to assemble
            params: Dict mapping atom_id -> param dict for each atom

        Returns:
            Dict with assembled code, per-atom results, and warnings
        """
        if not JINJA2_AVAILABLE:
            return {"error": "Jinja2 not installed — assembly unavailable"}

        conn = self._get_conn()
        try:
            mol = conn.execute(
                "SELECT id, name, description, atom_ids_json, template FROM molecules WHERE id = ?",
                (molecule_id,)
            ).fetchone()

            if not mol:
                return {"error": f"Molecule {molecule_id} not found"}

            atom_ids = pg_sync.dejson(mol["atom_ids_json"] or "[]")
            params = params or {}

            rendered_atoms = []
            warnings = []
            assembled_parts = []

            for atom_id in atom_ids:
                atom_params = params.get(atom_id, {})
                result = self.render_expression(atom_id, atom_params)

                if "error" in result:
                    warnings.append(f"Atom {atom_id}: {result['error']}")
                    # Fall back to raw code
                    row = conn.execute("SELECT code FROM atoms WHERE id = ?", (atom_id,)).fetchone()
                    assembled_parts.append(row["code"] if row else f"# ERROR: {atom_id} not found")
                else:
                    assembled_parts.append(result["rendered"])
                    rendered_atoms.append({
                        "atom_id": atom_id,
                        "name": result["name"],
                        "parameters_used": result["parameters_used"]
                    })

            assembled = "\n\n".join(assembled_parts)

            # If molecule has its own template, render that with atom outputs
            mol_template = mol["template"]
            if mol_template and mol_template != assembled:
                try:
                    tmpl = self._jinja_env.from_string(mol_template)
                    # Pass rendered atoms as named blocks
                    atom_blocks = {f"atom_{i}": part for i, part in enumerate(assembled_parts)}
                    assembled = tmpl.render(**atom_blocks)
                except Exception as e:
                    warnings.append(f"Molecule template render failed: {e}")

            return {
                "molecule_id": molecule_id,
                "name": mol["name"],
                "description": mol["description"],
                "assembled": assembled,
                "atoms_rendered": rendered_atoms,
                "atom_count": len(atom_ids),
                "warnings": warnings
            }
        finally:
            conn.close()

    def assemble_organism(self, organism_id: str, params: Optional[Dict[str, Dict]] = None) -> Dict[str, Any]:
        """Assemble an organism by assembling all its constituent molecules.

        Args:
            organism_id: Organism to assemble
            params: Dict mapping molecule_id -> {atom_id -> param_dict}
        """
        if not JINJA2_AVAILABLE:
            return {"error": "Jinja2 not installed — assembly unavailable"}

        conn = self._get_conn()
        try:
            org = conn.execute(
                "SELECT id, name, description, molecule_ids_json, template FROM organisms WHERE id = ?",
                (organism_id,)
            ).fetchone()

            if not org:
                return {"error": f"Organism {organism_id} not found"}

            molecule_ids = pg_sync.dejson(org["molecule_ids_json"] or "[]")
            params = params or {}

            rendered_molecules = []
            warnings = []
            assembled_parts = []

            for mol_id in molecule_ids:
                mol_params = params.get(mol_id, {})
                result = self.assemble_molecule(mol_id, mol_params)

                if "error" in result:
                    warnings.append(f"Molecule {mol_id}: {result['error']}")
                else:
                    assembled_parts.append(result["assembled"])
                    rendered_molecules.append({
                        "molecule_id": mol_id,
                        "name": result["name"],
                        "atoms_rendered": result["atoms_rendered"]
                    })

            assembled = "\n\n".join(assembled_parts)

            return {
                "organism_id": organism_id,
                "name": org["name"],
                "description": org["description"],
                "assembled": assembled,
                "molecules_rendered": rendered_molecules,
                "molecule_count": len(molecule_ids),
                "warnings": warnings
            }
        finally:
            conn.close()

    # ========== TEMPLATE LISTING ==========

    def list_templates(self, templatized_only: bool = False) -> Dict[str, Any]:
        """List all atoms with their template status."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT id, name, length(code) as code_len, template, parameters_json, meta FROM atoms ORDER BY name"
            ).fetchall()

            templates = []
            for row in rows:
                params = pg_sync.dejson(row["parameters_json"] or "[]")
                is_templatized = (row["template"] is not None and
                                 row["template"] != conn.execute(
                                     "SELECT code FROM atoms WHERE id = ?", (row["id"],)
                                 ).fetchone()["code"])

                if templatized_only and not is_templatized:
                    continue

                meta = pg_sync.dejson(row["meta"] or "{}")
                structural = meta.get("structural", {})

                templates.append({
                    "atom_id": row["id"],
                    "name": row["name"],
                    "code_length": row["code_len"],
                    "templatized": is_templatized,
                    "template_format": structural.get("template_format", "raw"),
                    "parameter_count": len(params) if isinstance(params, list) else 0,
                    "parameters": params if isinstance(params, list) else [],
                    "render_count": meta.get("editor", {}).get("render_count", 0)
                })

            return {
                "total": len(templates),
                "templatized": sum(1 for t in templates if t["templatized"]),
                "templates": templates
            }
        finally:
            conn.close()

    def get_template(self, atom_id: str) -> Dict[str, Any]:
        """Get full template details for a single atom."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT id, name, code, template, parameters_json, meta FROM atoms WHERE id = ?",
                (atom_id,)
            ).fetchone()

            if not row:
                return {"error": f"Atom {atom_id} not found"}

            params = pg_sync.dejson(row["parameters_json"] or "[]")
            meta = pg_sync.dejson(row["meta"] or "{}")
            is_templatized = row["template"] is not None and row["template"] != row["code"]

            return {
                "atom_id": row["id"],
                "name": row["name"],
                "code": row["code"],
                "template": row["template"],
                "templatized": is_templatized,
                "parameters": params,
                "parameter_count": len(params) if isinstance(params, list) else 0,
                "template_format": meta.get("structural", {}).get("template_format", "raw"),
                "render_count": meta.get("editor", {}).get("render_count", 0),
                "meta": meta
            }
        finally:
            conn.close()


# === Global singleton ===
_editor_service = None

def get_editor_service() -> EditorService:
    global _editor_service
    if _editor_service is None:
        _editor_service = EditorService()
    return _editor_service
