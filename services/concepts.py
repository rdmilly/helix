"""Concept Service — Semantic Concept Extraction & Enrichment

Transforms atoms from "code with templates" into "semantic concepts with
multiple expression modes." Uses Haiku to extract deep understanding of
WHAT a pattern is, WHY it exists, HOW it works, and WHAT it relates to.

All metadata is stored via EDA namespaces in the existing meta JSON field.
Zero schema changes. Records grow richer through exposure.

Namespaces written by this service:
  - concept:        essence, archetype, understanding (what/why/how/constraints/tradeoffs)
  - relationships:  requires, integrates_with, similar_to, composes_with
  - context:        captured_during, tools_observed, project_type, domain_signals
  - composition:    sections (imports/config/middleware/tools/startup), expression_modes
"""
import json
from services import pg_sync
import re
import logging
import sqlite3
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

CONCEPT_EXTRACTION_PROMPT = """Analyze this code and extract its SEMANTIC MEANING as a reusable concept.

CODE:
```
{code}
```

CONTEXT (if available): {context}

You are not extracting template variables. You are understanding what this code REPRESENTS as a concept — independent of language, variable names, or specific values.

Return a JSON object with these fields:

{{
  "essence": "One sentence: what this pattern IS (not what it does). Example: 'API key authentication gate for HTTP request pipelines'",
  
  "archetype": "Primary category. One of: authentication, authorization, database, caching, middleware, networking, configuration, monitoring, logging, error_handling, data_processing, api_endpoint, serialization, scheduling, file_io, messaging, security, testing, deployment, utility",
  
  "understanding": {{
    "what": "What this code represents as a concept (2-3 sentences)",
    "why": "Why this pattern exists — what problem it solves",
    "how": "How it works mechanically (mechanism, not implementation details)",
    "constraints": ["List of things that must be true for this to work"],
    "tradeoffs": {{
      "gains": ["What you get by using this pattern"],
      "costs": ["What you give up or accept"]
    }}
  }},
  
  "relationships": {{
    "requires": ["Concepts this NEEDS to function (e.g., 'http_request_pipeline', 'credential_store')"],
    "integrates_with": ["Concepts this commonly pairs with"],
    "similar_to": ["Related concepts that solve similar problems differently"],
    "composes_with": ["Concepts this can be assembled with to form larger patterns"]
  }},
  
  "sections": {{
    "primary": "Which section this code primarily belongs to. One of: imports, config, models, middleware, tools, routes, startup, shutdown, utility",
    "also_needs": ["Other sections this code requires to exist"]
  }},
  
  "parameters": [
    {{
      "name": "snake_case parameter name",
      "type": "string|int|float|bool|list|dict",
      "default": "the concrete value from the code",
      "description": "what this parameter controls",
      "semantic_role": "What role this value plays in the concept (e.g., 'threshold', 'identifier', 'endpoint', 'credential_key', 'error_message', 'limit')"
    }}
  ],
  
  "expression_modes": {{
    "code": true,
    "config": "Can this be expressed as YAML/JSON config? true/false",
    "documentation": "Can this be meaningfully described in prose? true/false",
    "api_call": "Could this be invoked as an API parameter set? true/false"
  }},
  
  "signals": {{
    "language_agnostic": "Could this concept be implemented in any language? true/false",
    "framework_specific": "Name of framework if tied to one, otherwise null",
    "complexity": "trivial|simple|moderate|complex",
    "stability": "How often would this pattern change? stable|evolving|volatile"
  }}
}}

Rules:
- Think about the CONCEPT, not the specific code.
- Parameters should capture configurable VALUES, not structural code elements.
- Relationships should reference abstract concepts, not specific implementations.
- If the code is too trivial for meaningful concept extraction (e.g., a bare __init__), return minimal data with "archetype": "utility" and "signals.complexity": "trivial".

Respond with ONLY valid JSON, no explanation."""


class ConceptService:
    def __init__(self, db_path: str = "/app/data/cortex.db"):
        self.db_path = db_path
        self._namespace_versions = {
            "concept": "1.0", "relationships": "1.0",
            "context": "1.0", "composition": "1.0",
        }
    
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def register_namespaces(self):
        conn = self._get_conn()
        try:
            namespaces = [
                ("concept", "concept_service_v1",
                 '{"essence": "string", "archetype": "string", "understanding": "object", "signals": "object"}',
                 "Semantic concept: essence, archetype, understanding (what/why/how), signals",
                 '["atoms", "molecules", "organisms"]'),
                ("relationships", "concept_service_v1",
                 '{"requires": "array", "integrates_with": "array", "similar_to": "array", "composes_with": "array"}',
                 "Concept relationships: requires, integrates_with, similar_to, composes_with",
                 '["atoms", "molecules"]'),
                ("context", "concept_service_v1",
                 '{"captured_during": "string", "tools_observed": "array", "project_type": "string"}',
                 "Capture context: what was being built/installed/configured when pattern was observed",
                 '["atoms"]'),
                ("composition", "concept_service_v1",
                 '{"sections": "object", "expression_modes": "object", "parameters": "array"}',
                 "Composition metadata: section assignments, expression modes, rich parameters",
                 '["atoms", "molecules"]'),
            ]
            for ns, registered_by, schema, desc, applies_to in namespaces:
                conn.execute("""
                    INSERT INTO meta_namespaces 
                    (namespace, registered_by, fields_schema, description, applies_to, version)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (ns, registered_by, schema, desc, applies_to, self._namespace_versions[ns]))
            conn.commit()
            logger.info("Concept namespaces registered (4 namespaces)")
        finally:
            conn.close()
    
    async def extract_concept(self, atom_id: str, context: str = "", force: bool = False) -> Dict[str, Any]:
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT id, name, code, meta FROM atoms WHERE id = ?", (atom_id,)).fetchone()
            if not row:
                return {"error": f"Atom {atom_id} not found"}
            
            meta = pg_sync.dejson(row["meta"] or "{}")
            if not force and "concept" in meta and meta["concept"].get("essence"):
                return {
                    "atom_id": atom_id, "name": row["name"], "status": "already_enriched",
                    "concept": meta["concept"], "relationships": meta.get("relationships", {}),
                    "composition": meta.get("composition", {}),
                }
            
            result = await self._haiku_extract_concept(row["code"], context)
            if "error" in result:
                return {"atom_id": atom_id, "error": result["error"]}
            
            now = datetime.utcnow().isoformat()
            meta["concept"] = {
                "essence": result.get("essence", ""),
                "archetype": result.get("archetype", "utility"),
                "understanding": result.get("understanding", {}),
                "signals": result.get("signals", {}),
                "_version": self._namespace_versions["concept"],
                "_enriched_at": now, "_enriched_by": "concept_service_v1",
            }
            rels = result.get("relationships", {})
            meta["relationships"] = {
                "requires": rels.get("requires", []),
                "integrates_with": rels.get("integrates_with", []),
                "similar_to": rels.get("similar_to", []),
                "composes_with": rels.get("composes_with", []),
                "_version": self._namespace_versions["relationships"], "_enriched_at": now,
            }
            sections = result.get("sections", {})
            params = result.get("parameters", [])
            expr_modes = result.get("expression_modes", {})
            meta["composition"] = {
                "primary_section": sections.get("primary", "utility"),
                "also_needs": sections.get("also_needs", []),
                "expression_modes": expr_modes, "parameters": params,
                "_version": self._namespace_versions["composition"], "_enriched_at": now,
            }
            
            flat_params = [{"name": p.get("name", ""), "type": p.get("type", "string"),
                           "default": p.get("default"), "description": p.get("description", "")}
                          for p in params]
            
            conn.execute("UPDATE atoms SET parameters_json = ?, meta = ? WHERE id = ?",
                        (json.dumps(flat_params), json.dumps(meta), atom_id))
            self._log_event(conn, "atoms", atom_id, "concept", "enrich", {
                "archetype": meta["concept"]["archetype"],
                "parameters": len(params), "sections": sections.get("primary", "unknown"),
            })
            conn.commit()
            return {
                "atom_id": atom_id, "name": row["name"], "status": "enriched",
                "concept": meta["concept"], "relationships": meta["relationships"],
                "composition": meta["composition"],
            }
        finally:
            conn.close()
    
    async def extract_all_concepts(self, force: bool = False, context: str = "") -> Dict[str, Any]:
        conn = self._get_conn()
        try:
            if force:
                rows = conn.execute("SELECT id FROM atoms").fetchall()
            else:
                rows = conn.execute("""
                    SELECT id FROM atoms 
                    WHERE json_extract(meta, '$.concept.essence') IS NULL
                       OR json_extract(meta, '$.concept.essence') = ''
                """).fetchall()
            results = []
            for row in rows:
                result = await self.extract_concept(row["id"], context=context, force=force)
                results.append(result)
            enriched = sum(1 for r in results if r.get("status") in ("enriched", "already_enriched"))
            failed = sum(1 for r in results if "error" in r)
            return {"total": len(results), "enriched": enriched, "failed": failed, "results": results}
        finally:
            conn.close()
    
    async def enrich_context(self, atom_id: str, context_data: Dict[str, Any]) -> Dict[str, Any]:
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT id, meta FROM atoms WHERE id = ?", (atom_id,)).fetchone()
            if not row:
                return {"error": f"Atom {atom_id} not found"}
            meta = pg_sync.dejson(row["meta"] or "{}")
            ctx = meta.get("context", {"_version": self._namespace_versions["context"],
                                       "captured_during": [], "tools_observed": [],
                                       "project_types": [], "domain_signals": []})
            if context_data.get("captured_during"):
                activities = ctx.get("captured_during", [])
                if context_data["captured_during"] not in activities:
                    activities.append(context_data["captured_during"])
                ctx["captured_during"] = activities
            if context_data.get("tools_observed"):
                tools = ctx.get("tools_observed", [])
                for tool in context_data["tools_observed"]:
                    if tool not in tools:
                        tools.append(tool)
                ctx["tools_observed"] = tools
            if context_data.get("project_type"):
                types = ctx.get("project_types", [])
                if context_data["project_type"] not in types:
                    types.append(context_data["project_type"])
                ctx["project_types"] = types
            ctx["_last_enriched"] = datetime.utcnow().isoformat()
            meta["context"] = ctx
            conn.execute("UPDATE atoms SET meta = ? WHERE id = ?", (json.dumps(meta), atom_id))
            conn.commit()
            return {"atom_id": atom_id, "status": "context_enriched", "context": ctx}
        finally:
            conn.close()
    
    def get_concept(self, atom_id: str) -> Dict[str, Any]:
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT id, name, code, template, parameters_json, meta FROM atoms WHERE id = ?",
                (atom_id,)
            ).fetchone()
            if not row:
                return {"error": f"Atom {atom_id} not found"}
            meta = pg_sync.dejson(row["meta"] or "{}")
            return {
                "atom_id": row["id"], "name": row["name"], "code": row["code"],
                "concept": meta.get("concept", {}),
                "relationships": meta.get("relationships", {}),
                "composition": meta.get("composition", {}),
                "context": meta.get("context", {}),
                "structural": meta.get("structural", {}),
                "has_concept": bool(meta.get("concept", {}).get("essence")),
            }
        finally:
            conn.close()
    
    def find_by_archetype(self, archetype: str) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT id, name, meta FROM atoms 
                WHERE json_extract(meta, '$.concept.archetype') = ?
            """, (archetype,)).fetchall()
            return [{"atom_id": r["id"], "name": r["name"],
                     "essence": pg_sync.dejson(r["meta"] or "{}").get("concept", {}).get("essence", ""),
                     "archetype": archetype} for r in rows]
        finally:
            conn.close()
    
    def find_composable(self, atom_id: str) -> Dict[str, Any]:
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT id, name, meta FROM atoms WHERE id = ?", (atom_id,)).fetchone()
            if not row:
                return {"error": f"Atom {atom_id} not found"}
            meta = pg_sync.dejson(row["meta"] or "{}")
            rels = meta.get("relationships", {})
            composes_with = rels.get("composes_with", [])
            integrates_with = rels.get("integrates_with", [])
            candidates = []
            all_atoms = conn.execute("SELECT id, name, meta FROM atoms WHERE id != ?", (atom_id,)).fetchall()
            for a in all_atoms:
                a_meta = pg_sync.dejson(a["meta"] or "{}")
                a_concept = a_meta.get("concept", {})
                a_archetype = a_concept.get("archetype", "")
                a_essence = a_concept.get("essence", "")
                score = 0
                match_reasons = []
                for ref in composes_with:
                    if ref.lower() in a_essence.lower() or ref.lower() in a_archetype.lower():
                        score += 2
                        match_reasons.append(f"composes_with: {ref}")
                for ref in integrates_with:
                    if ref.lower() in a_essence.lower() or ref.lower() in a_archetype.lower():
                        score += 1
                        match_reasons.append(f"integrates_with: {ref}")
                a_rels = a_meta.get("relationships", {})
                our_archetype = meta.get("concept", {}).get("archetype", "")
                our_essence = meta.get("concept", {}).get("essence", "")
                for ref in a_rels.get("composes_with", []):
                    if ref.lower() in our_essence.lower() or ref.lower() in our_archetype.lower():
                        score += 2
                        match_reasons.append(f"reverse_compose: {ref}")
                if score > 0:
                    candidates.append({"atom_id": a["id"], "name": a["name"],
                                      "essence": a_essence, "archetype": a_archetype,
                                      "score": score, "match_reasons": match_reasons})
            candidates.sort(key=lambda x: x["score"], reverse=True)
            return {"atom_id": atom_id, "name": row["name"], "composable_atoms": candidates}
        finally:
            conn.close()
    
    async def _haiku_extract_concept(self, code: str, context: str = "") -> Dict[str, Any]:
        try:
            import httpx
            prompt = CONCEPT_EXTRACTION_PROMPT.format(
                code=code, context=context or "No additional context available"
            )
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
            text = data["content"][0]["text"].strip()
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\s*', '', text)
                text = re.sub(r'\s*```$', '', text)
            return pg_sync.dejson(text)
        except json.JSONDecodeError as e:
            logger.error(f"Haiku returned invalid JSON: {e}")
            return {"error": f"Invalid JSON from Haiku: {e}"}
        except Exception as e:
            logger.error(f"Concept extraction failed: {e}")
            return {"error": str(e)}
    
    def _get_api_key(self) -> str:
        import os
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not key:
            raise ValueError("ANTHROPIC_API_KEY not set")
        return key
    
    def _log_event(self, conn, table: str, target_id: str, namespace: str,
                   action: str, data: Dict):
        evt_id = f"evt_{target_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        conn.execute("""
            INSERT INTO meta_events (id, target_table, target_id, namespace, action, new_value, written_by)
            VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
        """, (evt_id, table, target_id, namespace, action, json.dumps(data), "concept_service_v1"))


_concept_service = None

def get_concept_service() -> ConceptService:
    global _concept_service
    if _concept_service is None:
        _concept_service = ConceptService()
        _concept_service.register_namespaces()
    return _concept_service
