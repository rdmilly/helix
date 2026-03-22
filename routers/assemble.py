"""Assembler Router — Phase 5

Pre-staging: suggest atoms/molecules before Claude asks.
Spec auto-gen: cluster ADRs -> draft spec docs.

Endpoints:
  POST /api/v1/assemble/suggest   - suggest atoms for a task description
  POST /api/v1/assemble/spec      - auto-gen spec from ADR cluster
  GET  /api/v1/assemble/prestage  - recent high-recurrence scaffold candidates
"""
import json
import logging
from typing import Optional, List
from datetime import datetime, timezone
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

log = logging.getLogger("helix.assemble")
router = APIRouter(prefix="/api/v1/assemble")


class SuggestRequest(BaseModel):
    task: str
    context: Optional[str] = None
    limit: int = 5


class SpecRequest(BaseModel):
    topic: str           # e.g. "mcp architecture"
    min_adrs: int = 2   # min ADR cluster size to draft from


@router.post("/suggest")
async def suggest_atoms(req: SuggestRequest):
    """Suggest Forge atoms relevant to a task. Pre-staging core."""
    try:
        from services.assembler import get_assembler
        asm = get_assembler()
        # Use archetype matching against task keywords
        keywords = req.task.lower().split()
        # Map keywords to archetypes
        archetype_map = {
            'router': ['fastapi_router', 'api_endpoint'],
            'api': ['fastapi_router', 'api_endpoint'],
            'docker': ['docker_compose', 'dockerfile'],
            'deploy': ['docker_compose', 'deployment'],
            'auth': ['auth_middleware', 'jwt_auth'],
            'search': ['search_handler', 'fts_search'],
            'cron': ['scheduler_job', 'background_task'],
            'job': ['scheduler_job', 'background_task'],
            'kg': ['kg_entity', 'neo4j_query'],
            'graph': ['kg_entity', 'neo4j_query'],
            'compress': ['compression_handler', 'language_compression'],
            'ingest': ['ingestion_handler', 'conversation_ingest'],
        }
        archetypes = []
        for kw in keywords:
            for key, vals in archetype_map.items():
                if key in kw:
                    archetypes.extend(vals)
        archetypes = list(dict.fromkeys(archetypes))[:4]  # dedup, limit

        if archetypes:
            result = asm.assemble_by_archetype(archetypes, mode='documentation')
            atoms_used = result.get('atoms_used', [])
        else:
            atoms_used = []

        return JSONResponse({
            'task': req.task,
            'archetypes_matched': archetypes,
            'suggestions': atoms_used[:req.limit],
            'count': len(atoms_used),
        })
    except Exception as e:
        log.error(f"suggest_atoms: {e}")
        return JSONResponse({'error': str(e)}, status_code=500)


@router.get("/prestage")
async def get_prestage_candidates():
    """Return high-recurrence entities that are scaffold candidates."""
    try:
        from services import pg_sync
        conn = pg_sync.sqlite_conn()
        try:
            rows = conn.execute(
                """
                SELECT name, entity_type, description, mention_count,
                       attributes_json
                FROM entities
                WHERE mention_count >= 2
                  AND entity_type = 'adr'
                ORDER BY mention_count DESC
                LIMIT 10
                """
            ).fetchall()
        finally:
            conn.close()

        candidates = []
        for row in rows:
            attrs = json.loads(row[4] or '{}')
            candidates.append({
                'name': row[0],
                'type': row[1],
                'description': (row[2] or '')[:120],
                'recurrence': row[3],
                'scaffold_candidate': attrs.get('scaffold_candidate', False),
            })

        return JSONResponse({'candidates': candidates, 'count': len(candidates)})
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=500)


@router.post("/spec")
async def generate_spec(req: SpecRequest):
    """Auto-generate a spec doc stub from ADR cluster matching a topic."""
    try:
        from services import pg_sync
        from services.haiku import get_haiku_service
        import asyncio

        conn = pg_sync.sqlite_conn()
        try:
            # Find decisions matching topic
            rows = conn.execute(
                """
                SELECT content, session_id, created_at
                FROM structured_archive
                WHERE collection = 'decisions'
                  AND content ILIKE %s
                ORDER BY created_at DESC
                LIMIT 20
                """,
                (f'%{req.topic}%',)
            ).fetchall()
        finally:
            conn.close()

        if len(rows) < req.min_adrs:
            return JSONResponse({
                'error': f'Only {len(rows)} ADRs found for topic "{req.topic}", need {req.min_adrs}',
                'found': len(rows)
            })

        # Build context for Haiku
        decisions_text = '\n'.join(f'- {r[0][:200]}' for r in rows[:10])
        haiku = get_haiku_service()
        prompt = f"""Generate a concise spec document stub for: {req.topic}

Based on these decisions:
{decisions_text}

Format: markdown with sections: Overview, Decision Summary, Implementation Notes, Open Questions.
Keep it under 400 words."""

        spec_content = await haiku._call_api(
            system="You are a technical spec writer. Output clean markdown only.",
            user_message=prompt,
            max_tokens=600
        )

        if spec_content:
            # Write to working-kb
            from pathlib import Path
            spec_dir = Path('/app/working-kb/specs')
            spec_dir.mkdir(parents=True, exist_ok=True)
            slug = req.topic.lower().replace(' ', '-')[:40]
            spec_path = spec_dir / f'auto-spec-{slug}.md'
            spec_path.write_text(
                f'# Spec: {req.topic}\n_Auto-generated {datetime.now(timezone.utc).strftime("%Y-%m-%d")} from {len(rows)} ADRs_\n\n{spec_content}',
                encoding='utf-8'
            )

        return JSONResponse({
            'topic': req.topic,
            'adrs_used': len(rows),
            'spec': spec_content or 'generation failed',
            'written': bool(spec_content),
        })
    except Exception as e:
        log.error(f"generate_spec: {e}")
        return JSONResponse({'error': str(e)}, status_code=500)
