"""Synapse Service - Context Assembly & Search Engine

Phase 3: The Synapse connects stored DNA knowledge back to active sessions.
Searches across atoms, sessions, entities, and decisions to assemble
relevant context for injection into new conversations.
"""
import asyncio
import json
from services import pg_sync
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from services.database import get_db
from services.chromadb import get_chromadb_service
from services.meta import get_meta_service
from services import conversation_store

logger = logging.getLogger(__name__)

# Token estimation: ~4 chars per token (conservative)
_CHARS_PER_TOKEN = 4


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // _CHARS_PER_TOKEN)


class SynapseService:
    """Context assembly and search across the Helix knowledge base."""
    
    def __init__(self):
        self.db = get_db()
        self.chromadb = get_chromadb_service()
        self.meta = get_meta_service()
    
    # ============================================================
    # Session Lifecycle
    # ============================================================
    
    def start_session(
        self,
        session_id: str,
        provider: str = "anthropic",
        model: str = "unknown",
        tags: Optional[List[str]] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a new session and optionally inject context."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if session already exists
            cursor.execute("SELECT id FROM sessions WHERE id = ?", (session_id,))
            if cursor.fetchone():
                return self.get_session(session_id)
            
            # Create session
            cursor.execute("""
                INSERT INTO sessions (id, provider, model, significance, meta)
                VALUES (?, ?, ?, 0, '{}')
            """, (session_id, provider, model))
            conn.commit()
        
        # Write lifecycle meta
        self.meta.write_meta("sessions", session_id, "lifecycle", {
            "started_at": datetime.utcnow().isoformat(),
            "provider": provider,
            "model": model,
            "tags": tags or [],
            "started_by": "synapse_v1",
        }, written_by="synapse_v1")
        
        # Write custom meta if provided
        if meta:
            self.meta.write_meta("sessions", session_id, "custom",
                                 meta, written_by="synapse_v1")
        
        logger.info(f"Session started: {session_id}")
        return self.get_session(session_id)
    
    def end_session(
        self,
        session_id: str,
        summary: Optional[str] = None,
        outcome: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Close a session and record final state."""
        end_data = {
            "closed_at": datetime.utcnow().isoformat(),
            "closed_by": "synapse_v1",
        }
        if summary:
            end_data["final_summary"] = summary
        if outcome:
            end_data["outcome"] = outcome
        
        self.meta.write_meta("sessions", session_id, "lifecycle",
                             end_data, written_by="synapse_v1")
        
        logger.info(f"Session ended: {session_id}")
        return self.get_session(session_id)
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get full session details with all meta namespaces."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, provider, model, significance, meta, created_at
                FROM sessions WHERE id = ?
            """, (session_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            session = {
                "session_id": row[0],
                "provider": row[1],
                "model": row[2],
                "significance": row[3],
                "created_at": row[5],
            }
        
        # Gather all meta namespaces (namespace=None returns all)
        try:
            session["meta"] = self.meta.read_meta("sessions", session_id)
        except ValueError:
            session["meta"] = {}
        
        return session
    
    def list_sessions(
        self,
        limit: int = 20,
        offset: int = 0,
        min_significance: int = 0,
    ) -> List[Dict[str, Any]]:
        """List sessions with optional filtering."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, provider, model, significance, created_at
                FROM sessions
                WHERE significance >= ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """, (min_significance, limit, offset))
            
            return [
                {
                    "session_id": row[0],
                    "provider": row[1],
                    "model": row[2],
                    "significance": row[3],
                    "created_at": row[4],
                }
                for row in cursor.fetchall()
            ]
    
    # ============================================================
    # Search
    # ============================================================
    
    def search_atoms(
        self,
        query: Optional[str] = None,
        name: Optional[str] = None,
        category: Optional[str] = None,
        language: Optional[str] = None,
        min_significance: int = 0,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Search atoms in the DNA library by various criteria."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            conditions = ["1=1"]
            params: list = []
            
            if name:
                conditions.append("name LIKE ?")
                params.append(f"%{name}%")
            
            if category:
                conditions.append("json_extract(meta, '$.semantic.category') = ?")
                params.append(category)
            
            if language:
                conditions.append("json_extract(meta, '$.structural.language') = ?")
                params.append(language)
            
            where_clause = " AND ".join(conditions)
            
            cursor.execute(f"""
                SELECT id, name, full_name, structural_fp, semantic_fp,
                       fp_version, occurrence_count, first_seen, meta
                FROM atoms
                WHERE {where_clause}
                ORDER BY occurrence_count DESC, first_seen DESC
                LIMIT ?
            """, params + [limit])
            
            atoms = []
            for row in cursor.fetchall():
                atom_meta = pg_sync.dejson(row[8]) if row[8] else {}
                structural = atom_meta.get("structural", {})
                semantic = atom_meta.get("semantic", {})
                atoms.append({
                    "id": row[0],
                    "name": row[1],
                    "full_name": row[2],
                    "category": semantic.get("category", "general"),
                    "language": structural.get("language", "unknown"),
                    "line_count": structural.get("line_count", 0),
                    "structural_fp": row[3],
                    "semantic_fp": row[4],
                    "occurrence_count": row[6],
                    "first_seen": row[7],
                    "meta": atom_meta,
                })
            
            return atoms
    
    async def semantic_search(
        self,
        query: str,
        collections: Optional[List[str]] = None,
        limit: int = 10,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Semantic search across ChromaDB collections."""
        if collections is None:
            collections = ["atoms", "sessions", "entities", "intelligence"]
        
        results = {}
        
        for collection_base in collections:
            try:
                docs = await self.chromadb.search_similar(
                    query=query,
                    collection_base=collection_base,
                    limit=limit,
                )
                results[collection_base] = docs or []
            except Exception as e:
                logger.error(f"Semantic search failed for {collection_base}: {e}")
                results[collection_base] = []
        
        return results
    
    # ============================================================
    # Context Assembly
    # ============================================================
    
    async def assemble_context(
        self,
        query: str,
        session_id: Optional[str] = None,
        max_atoms: int = 10,
        max_decisions: int = 5,
        max_sessions: int = 5,
        include_entities: bool = True,
        since_session_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Assemble relevant context for injection into a session."""
        context: Dict[str, Any] = {
            "query": query,
            "atoms": [],
            "conversation_chunks": [],
            "decisions": [],
            "intelligence_items": [],
            "related_sessions": [],
            "entities": {},
            "stats": {},
            "recent_shard": {},
        }
        
        # 1. Semantic search for relevant atoms
        atom_results = await self.chromadb.search_similar(
            query=query,
            collection_base="atoms",
            limit=max_atoms,
        )
        
        for result in (atom_results or []):
            atom_id = result.get("id", "")
            atom_detail = self._get_atom_detail(atom_id)
            if atom_detail:
                context["atoms"].append({
                    "id": atom_id,
                    "name": atom_detail.get("name", ""),
                    "category": atom_detail.get("category", ""),
                    "distance": result.get("distance"),
                    "meta": atom_detail.get("meta", {}),
                })
        
        # 2. Search for relevant sessions
        session_results = await self.chromadb.search_similar(
            query=query,
            collection_base="sessions",
            limit=max_sessions,
        )
        
        for result in (session_results or []):
            context["related_sessions"].append({
                "id": result.get("id", ""),
                "text": result.get("document", ""),
                "distance": result.get("distance"),
                "metadata": result.get("metadata", {}),
            })
        
        # 2.5 Search conversation RAG (MemBrain captures)
        try:
            conv_results = await conversation_store.hybrid_search(
                query=query,
                limit=max_sessions,
            )
            for chunk in conv_results.get("results", []):
                content = chunk.get("content", "")
                if content and len(content) > 20:
                    context["conversation_chunks"].append({
                        "session_id": chunk.get("session_id", ""),
                        "content": content[:600],
                        "score": chunk.get("score", 0),
                        "source": chunk.get("source", ""),
                        "timestamp": chunk.get("timestamp", ""),
                        "topic_hint": chunk.get("topic_hint", ""),
                    })
        except Exception as e:
            logger.warning(f"Conversation RAG search failed: {e}")

        # 2.7 Search intelligence collection
        try:
            intel_results = await self.chromadb.search_similar(
                query=query,
                collection_base="intelligence",
                limit=8,
            )
            for result in (intel_results or []):
                doc = result.get("document", "")
                meta = result.get("metadata", {})
                tag = meta.get("tag", "")
                if doc and tag:
                    context["intelligence_items"].append({
                        "tag": tag,
                        "content": doc.replace(f"[{tag}] ", "", 1),
                        "component": meta.get("component", ""),
                        "context": meta.get("context", ""),
                        "distance": result.get("distance"),
                    })
        except Exception as e:
            logger.warning(f"Intelligence search failed: {e}")

        # 2.8 Shard assembly
        try:
            from services.shard import get_shard_assembler
            _assembler = get_shard_assembler()
            recent_shard = _assembler.assemble_recent_shard(
                hours=168,
                token_budget=600,
                include_decisions=True,
                include_entities=True,
                include_atoms=True,
                include_summaries=False,
            )
            context["recent_shard"] = recent_shard
        except Exception as e:
            logger.warning(f"Recent shard assembly failed (non-fatal): {e}")
            context["recent_shard"] = {}

        # 2.9 Per-atom delta shards when since_session_id provided
        if since_session_id and context["atoms"]:
            try:
                from services.shard import get_shard_assembler
                _assembler = get_shard_assembler()
                for _atom in context["atoms"][:4]:
                    _atom_id = _atom.get("id", "")
                    if _atom_id:
                        _delta = _assembler.assemble_shard(
                            "atom", _atom_id,
                            since_session_id=since_session_id,
                            token_budget=200,
                        )
                        _atom["shard"] = _delta
            except Exception as e:
                logger.debug(f"Per-atom delta assembly failed (non-fatal): {e}")

        # 3. Gather decisions from related sessions
        for session in context["related_sessions"][:max_sessions]:
            decisions = self._get_session_decisions(session.get("id", ""))
            context["decisions"].extend(decisions[:max_decisions])
        
        # Deduplicate decisions
        seen: set = set()
        unique_decisions = []
        for d in context["decisions"]:
            key = d.get("decision", "")
            if key and key not in seen:
                seen.add(key)
                unique_decisions.append(d)
        context["decisions"] = unique_decisions[:max_decisions]
        
        # 4. Gather entities
        if include_entities:
            entity_results = await self.chromadb.search_similar(
                query=query,
                collection_base="entities",
                limit=5,
            )
            
            all_entities: Dict[str, set] = {
                "people": set(), "projects": set(),
                "services": set(), "technologies": set(),
            }
            for result in (entity_results or []):
                meta = result.get("metadata", {})
                if meta:
                    for key in all_entities:
                        vals = meta.get(key, [])
                        if isinstance(vals, list):
                            all_entities[key].update(vals)
                        elif isinstance(vals, str):
                            all_entities[key].add(vals)
            
            context["entities"] = {k: list(v) for k, v in all_entities.items() if v}

            # 4b. Neo4j KG neighbors
            try:
                from services.neo4j_store import get_neo4j_store
                neo4j = get_neo4j_store()
                if neo4j._initialized:
                    kg_neighbors: list = []
                    seen_names = set()
                    for result in (entity_results or [])[:3]:
                        ename = (result.get("metadata") or {}).get("name") or result.get("id", "")
                        if ename and ename not in seen_names:
                            seen_names.add(ename)
                            neighbors = neo4j.get_entity_neighbors(ename, depth=1, limit=5)
                            for n in neighbors:
                                kg_neighbors.append({
                                    "from": ename,
                                    "to": n["name"],
                                    "relation": n["relation_type"],
                                    "type": n["entity_type"],
                                })
                    context["kg_neighbors"] = kg_neighbors[:15]
            except Exception as _e:
                context["kg_neighbors"] = []
                logger.debug(f"Neo4j KG enrich failed (non-fatal): {_e}")
        
        # 5. Build injection text
        context["injection_text"] = self._format_injection(context)
        
        # 6. Stats
        context["stats"] = {
            "atoms_found": len(context["atoms"]),
            "decisions_found": len(context["decisions"]),
            "intelligence_items_found": len(context["intelligence_items"]),
            "sessions_found": len(context["related_sessions"]),
            "conversation_chunks_found": len(context["conversation_chunks"]),
            "entities_found": sum(len(v) for v in context["entities"].values()),
            "shard_tokens_used": context.get("recent_shard", {}).get("tokens_used", 0),
        }
        
        # 7. Store injection meta on session if provided
        if session_id:
            try:
                self.meta.write_meta("sessions", session_id, "context_injection", {
                    "query": query,
                    "injected_at": datetime.utcnow().isoformat(),
                    "stats": context["stats"],
                }, written_by="synapse_v1")
            except ValueError:
                pass
        
        logger.info(
            f"Context assembled: {context['stats']['atoms_found']} atoms, "
            f"{context['stats']['decisions_found']} decisions, "
            f"{context['stats']['intelligence_items_found']} intel_items, "
            f"{context['stats']['sessions_found']} sessions, "
            f"{context['stats']['conversation_chunks_found']} conv_chunks"
        )
        
        return context

    # ============================================================
    # Tier 1 On-Demand Context Assembly  (Phase 2.1 + 2.2)
    # ============================================================

    async def assemble_tier1(
        self,
        query: str,
        session_id: str,
        budget: int = 1000,
        compress: bool = True,
    ) -> Dict[str, Any]:
        """Budget-aware Tier 1 on-demand context enrichment.

        Phase 2.1: Fires all 4 stores in parallel via asyncio.gather, then
        packs results into the token budget. Priority:
          1. Intelligence items (design memory)
          2. Conversation chunks (recent captured context)
          3. Atoms (code pattern library)
          4. Sessions (related past sessions)
          5. KG neighbors (graph relationships)

        Phase 2.2: Optionally compresses the assembled body through the
        server-side Compression Engine (shorthand layer) before returning,
        reducing token count by ~15-25% on typical context text.

        Args:
            query: The user's current turn / topic.
            session_id: Active Claude session ID.
            budget: Token budget 500-2000.
            compress: Apply compression to output (Phase 2.2).

        Returns:
            Dict with injection_text, sources, tokens_used, stats.
        """
        budget = max(500, min(2000, budget))
        char_budget = budget * _CHARS_PER_TOKEN
        chars_used = 0

        # --- Phase A: Parallel 4-store query ---
        async def _safe(coro, default):
            try:
                return await coro
            except Exception as exc:
                logger.warning(f"Tier1 store query failed (non-fatal): {exc}")
                return default

        atoms_raw, sessions_raw, intel_raw, conv_raw = await asyncio.gather(
            _safe(self.chromadb.search_similar(query, "atoms", limit=20), []),
            _safe(self.chromadb.search_similar(query, "sessions", limit=10), []),
            _safe(self.chromadb.search_similar(query, "intelligence", limit=15), []),
            _safe(conversation_store.hybrid_search(query=query, limit=10), {}),
        )

        # KG neighbors: sync — run in thread executor
        kg_neighbors: List[Dict[str, Any]] = []
        try:
            from services.neo4j_store import get_neo4j_store
            neo4j = get_neo4j_store()
            if neo4j._initialized:
                loop = asyncio.get_event_loop()
                entity_names = []
                for r in (atoms_raw or [])[:3]:
                    ename = (r.get("metadata") or {}).get("name") or r.get("id", "")
                    if ename:
                        entity_names.append(ename)
                def _kg_query():
                    out = []
                    seen_e = set()
                    for ename in entity_names:
                        if ename in seen_e:
                            continue
                        seen_e.add(ename)
                        for n in neo4j.get_entity_neighbors(ename, depth=1, limit=4):
                            out.append({
                                "from": ename,
                                "to": n["name"],
                                "relation": n["relation_type"],
                                "type": n["entity_type"],
                            })
                    return out[:12]
                kg_neighbors = await loop.run_in_executor(None, _kg_query)
        except Exception as _ke:
            logger.debug(f"KG neighbors failed (non-fatal): {_ke}")

        # --- Phase B: Budget-aware packing ---
        parts: List[str] = []
        sources: Dict[str, int] = {
            "intelligence": 0,
            "conversation_chunks": 0,
            "atoms": 0,
            "sessions": 0,
            "kg_neighbors": 0,
        }

        def _add(text: str, source_key: str) -> bool:
            nonlocal chars_used
            t = text.strip()
            if not t:
                return False
            if chars_used + len(t) + 1 > char_budget:
                return False
            parts.append(t)
            chars_used += len(t) + 1
            sources[source_key] += 1
            return True

        # 1. Intelligence items
        priority_tags = ["INVARIANT", "ASSUMPTION", "RISK", "CONSTRAINT",
                         "COUPLING", "DECISION", "TRADEOFF", "REJECTED", "PATTERN"]
        grouped_intel: Dict[str, list] = {}
        for result in (intel_raw or []):
            doc = result.get("document", "")
            meta = result.get("metadata", {})
            tag = meta.get("tag", "")
            if doc and tag:
                grouped_intel.setdefault(tag, []).append({
                    "tag": tag,
                    "content": doc.replace(f"[{tag}] ", "", 1),
                    "component": meta.get("component", ""),
                    "distance": result.get("distance", 1.0),
                })
        intel_section_lines: List[str] = []
        for tag in priority_tags:
            for item in grouped_intel.get(tag, []):
                comp = f" ({item['component']}):" if item.get("component") else ":"
                line = f"- [{tag}]{comp} {item['content'][:200]}"
                intel_section_lines.append(line)
        if intel_section_lines:
            _add("## Design Memory", "intelligence")
            for line in intel_section_lines:
                if not _add(line, "intelligence"):
                    break

        # 2. Conversation chunks
        conv_chunks = []
        if isinstance(conv_raw, dict):
            for chunk in conv_raw.get("results", []):
                content = chunk.get("content", "")
                if content and len(content) > 20:
                    conv_chunks.append(chunk)
        if conv_chunks:
            _add("## Recent Conversation Context", "conversation_chunks")
            for chunk in conv_chunks:
                ts = chunk.get("timestamp", "")[:10]
                content = chunk.get("content", "")[:400]
                label = f"[{ts}] " if ts else ""
                if not _add(f"{label}{content}", "conversation_chunks"):
                    break

        # 3. Atoms
        atom_items: List[Dict[str, Any]] = []
        for result in (atoms_raw or []):
            atom_id = result.get("id", "")
            detail = self._get_atom_detail(atom_id)
            if detail:
                atom_items.append({
                    "name": detail.get("name", ""),
                    "category": detail.get("category", ""),
                    "meta": detail.get("meta", {}),
                    "distance": result.get("distance", 1.0),
                })
        if atom_items:
            _add("## Code Patterns", "atoms")
            for atom in atom_items:
                meta = atom.get("meta", {})
                semantic = meta.get("semantic", {})
                tags = semantic.get("semantic_tags", []) if isinstance(semantic, dict) else []
                tag_str = ", ".join(tags[:3]) if tags else "general"
                line = f"- **{atom['name']}** ({atom['category']}) [{tag_str}]"
                if not _add(line, "atoms"):
                    break

        # 4. Related sessions
        if sessions_raw:
            _add("## Related Sessions", "sessions")
            for result in (sessions_raw or []):
                text = result.get("document", "")
                if text:
                    if not _add(f"- {text[:200]}", "sessions"):
                        break

        # 5. KG neighbors
        if kg_neighbors:
            _add("## Knowledge Graph", "kg_neighbors")
            for n in kg_neighbors:
                line = f"- {n['from']} --[{n['relation']}]--> {n['to']} ({n['type']})"
                if not _add(line, "kg_neighbors"):
                    break

        # --- Phase C: Server-side compression (Phase 2.2) ---
        # Apply shorthand compression to assembled body text.
        # Layers used: ["shorthand"] only — prose-safe word abbreviation.
        # Skips pattern_ref (code bodies only) and pruning (already budget-packed).
        body = "\n".join(parts) if parts else "No relevant context found."
        compression_stats: Dict[str, Any] = {"skipped": not compress}

        if compress and parts:
            try:
                from services.compression import get_compression_service
                compressor = get_compression_service()
                comp_result = compressor.compress(
                    text=body,
                    provider="synapse_tier1",
                    model="context_enrichment",
                    session_id=session_id,
                    max_tokens=0,  # no pruning — already budget-packed
                    layers=["shorthand"],
                )
                body = comp_result["compressed"]
                compression_stats = {
                    "skipped": False,
                    "tokens_before": comp_result["tokens_original"],
                    "tokens_after": comp_result["tokens_compressed"],
                    "tokens_saved": comp_result["tokens_saved"],
                    "ratio": comp_result["compression_ratio"],
                    "dictionary_version": comp_result["dictionary_version"],
                    "log_id": comp_result["log_id"],
                }
                logger.debug(
                    f"Tier1 compression: {comp_result['tokens_original']} -> "
                    f"{comp_result['tokens_compressed']} tokens "
                    f"(ratio={comp_result['compression_ratio']})"
                )
            except Exception as _ce:
                logger.debug(f"Tier1 compression failed (non-fatal): {_ce}")
                compression_stats = {"skipped": True, "error": str(_ce)}

        # Re-estimate tokens after compression
        tokens_used = _estimate_tokens(body)

        # --- Phase D: Wrap as sandwich block ---
        injection_text = (
            f"<!-- HELIX TIER 1 [budget={budget}t] [session={session_id[:16]}] -->\n"
            f"{body}\n"
            f"<!-- END HELIX TIER 1 -->"
        )

        # Store Tier 1 meta on session (non-fatal)
        try:
            self.meta.write_meta("sessions", session_id, "tier1_injection", {
                "query": query[:200],
                "injected_at": datetime.utcnow().isoformat(),
                "budget": budget,
                "tokens_used": tokens_used,
                "sources": sources,
                "compressed": compress and not compression_stats.get("skipped", True),
            }, written_by="synapse_tier1")
        except Exception:
            pass

        logger.info(
            f"Tier1 assembled: {tokens_used}/{budget} tokens | "
            f"intel={sources['intelligence']} conv={sources['conversation_chunks']} "
            f"atoms={sources['atoms']} sessions={sources['sessions']} "
            f"kg={sources['kg_neighbors']} | "
            f"compress={'yes' if compress and not compression_stats.get('skipped') else 'no'} "
            f"ratio={compression_stats.get('ratio', 'n/a')}"
        )

        return {
            "session_id": session_id,
            "query": query,
            "budget": budget,
            "tokens_used": tokens_used,
            "injection_text": injection_text,
            "sources": sources,
            "stats": {
                "atoms_raw": len(atoms_raw or []),
                "sessions_raw": len(sessions_raw or []),
                "intel_raw": len(intel_raw or []),
                "conv_chunks_raw": len(conv_chunks),
                "kg_neighbors_raw": len(kg_neighbors),
                "chars_used": chars_used,
                "char_budget": char_budget,
                "compression": compression_stats,
            },
        }

    # ============================================================
    # Internal Helpers
    # ============================================================
    
    def _get_atom_detail(self, atom_id: str) -> Optional[Dict[str, Any]]:
        """Get atom details from SQLite."""
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, full_name, occurrence_count, meta
                FROM atoms WHERE id = ?
            """, (atom_id,))
            row = cursor.fetchone()
            if row:
                atom_meta = pg_sync.dejson(row[4]) if row[4] else {}
                structural = atom_meta.get("structural", {})
                semantic = atom_meta.get("semantic", {})
                return {
                    "id": row[0], "name": row[1], "full_name": row[2],
                    "category": semantic.get("category", "general"),
                    "language": structural.get("language", "unknown"),
                    "line_count": structural.get("line_count", 0),
                    "occurrence_count": row[3],
                    "meta": atom_meta,
                }
        return None
    
    def _get_session_decisions(self, session_id: str) -> List[Dict[str, Any]]:
        """Get decisions from a session's meta."""
        try:
            meta = self.meta.read_meta("sessions", session_id, "decisions")
            if meta and isinstance(meta, dict):
                return meta.get("items", [])
        except (ValueError, KeyError):
            pass
        return []
    
    def _format_injection(self, context: Dict[str, Any]) -> str:
        """Format assembled context into injectable text."""
        parts = []
        
        if context["atoms"]:
            parts.append("## Relevant Code Patterns")
            for atom in context["atoms"]:
                meta = atom.get("meta", {})
                semantic = meta.get("semantic", {})
                tags = semantic.get("semantic_tags", []) if isinstance(semantic, dict) else []
                parts.append(
                    f"- **{atom['name']}** ({atom['category']}) "
                    f"[{', '.join(tags[:3]) if tags else 'general'}]"
                )
        
        if context.get("intelligence_items"):
            parts.append("\n## Design Memory")
            priority_tags = ["INVARIANT", "ASSUMPTION", "RISK", "CONSTRAINT", "COUPLING"]
            other_tags = ["DECISION", "TRADEOFF", "REJECTED", "PATTERN"]
            grouped = {}
            for item in context["intelligence_items"]:
                tag = item.get("tag", "")
                grouped.setdefault(tag, []).append(item)
            for tag in priority_tags + other_tags:
                for item in grouped.get(tag, []):
                    component = f" ({item['component']}):" if item.get("component") else ":"
                    ctx_note = f" — {item['context'][:80]}" if item.get("context") else ""
                    parts.append(f"- [{tag}]{component} {item['content'][:200]}{ctx_note}")

        if context["decisions"]:
            parts.append("\n## Prior Decisions")
            for decision in context["decisions"]:
                parts.append(f"- [{decision.get('type', 'general')}] {decision.get('decision', '')}")
        
        if context.get("conversation_chunks"):
            parts.append("\n## Recent Conversation Context")
            for chunk in context["conversation_chunks"][:5]:
                ts = chunk.get("timestamp", "")[:10]
                content = chunk.get("content", "")[:400]
                label = f"[{ts}]" if ts else ""
                parts.append(f"{label} {content}")

        if context["related_sessions"]:
            parts.append("\n## Related Sessions")
            for session in context["related_sessions"]:
                text = session.get("text", "")
                if text:
                    parts.append(f"- {text[:200]}")
        
        if context["entities"]:
            parts.append("\n## Known Entities")
            for entity_type, entities in context["entities"].items():
                if entities:
                    parts.append(f"- {entity_type}: {', '.join(entities[:10])}")
        
        _shard = context.get("recent_shard", {}) if isinstance(context, dict) else {}
        _shard_text = _shard.get("injection_text", "") if isinstance(_shard, dict) else ""
        if _shard_text:
            parts.append("\n" + _shard_text)

        return "\n".join(parts) if parts else "No relevant context found."


# Global service instance
_synapse_service = None


def get_synapse_service() -> SynapseService:
    """Get synapse service instance (lazy init)."""
    global _synapse_service
    if _synapse_service is None:
        _synapse_service = SynapseService()
    return _synapse_service
