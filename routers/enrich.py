"""Enrichment Router — namespace backfill and enrichment endpoints.

All namespaces that require batch computation rather than real-time writes.
Called manually or on a schedule, never on the hot path.
"""
from fastapi import APIRouter

router = APIRouter(prefix="/api/v1/scan/enrich", tags=["Enrichment"])


@router.post("/domain")
async def enrich_domain(limit: int = 5000):
    """Classify domain namespace for all unclassified atoms."""
    from services.domain_classifier import backfill_domain
    count = await backfill_domain(limit=limit)
    return {"status": "ok", "atoms_classified": count}


@router.post("/convention")
async def enrich_convention(limit: int = 5000):
    """Analyze coding conventions for atoms missing the namespace."""
    from services.convention_analyzer import backfill_convention
    count = await backfill_convention(limit=limit)
    return {"status": "ok", "atoms_analyzed": count}


@router.post("/exchange_context")
async def enrich_exchange_context(limit: int = 500):
    """Backfill exchange_context by linking build exchanges to atom names."""
    from services.exchange_linker import backfill_exchange_context
    count = await backfill_exchange_context(limit=limit)
    return {"status": "ok", "links_written": count}


@router.post("/quality")
async def enrich_quality(limit: int = 5000):
    """Bootstrap quality namespace from occurrence counts."""
    from services.quality_tracker import bootstrap_quality_from_occurrence
    count = await bootstrap_quality_from_occurrence(limit=limit)
    return {"status": "ok", "atoms_processed": count}


@router.post("/compression")
async def enrich_compression(limit: int = 100):
    """Assign compression symbols to universal atoms (occurrence > 8)."""
    from services.compression_optimizer import run_compression_pass
    count = await run_compression_pass(limit=limit)
    return {"status": "ok", "atoms_compressed": count}


@router.post("/similarity")
async def enrich_similarity(limit: int = 500):
    """Build similarity clusters using pgvector IVFFlat ANN.
    
    Queries pgvector embeddings table directly (not ChromaDB).
    All atom embeddings live in postgres.embeddings (source_type=atoms).
    ~0.23s per atom with IVFFlat index. Run in batches of 500.
    """
    from services.similarity_cluster import build_similarity_clusters
    count = await build_similarity_clusters(limit=limit)
    return {
        "status": "ok",
        "atoms_clustered": count,
        "limit": limit,
        "backend": "pgvector_ivfflat"
    }


@router.post("/agent_preference")
async def enrich_agent_preference(limit: int = 200):
    """Bootstrap agent_preference from build exchanges."""
    from services.agent_preference_bootstrap import bootstrap_from_exchanges
    result = await bootstrap_from_exchanges(limit=limit)
    return {"status": "ok", **result}


@router.post("/failure/logs")
async def enrich_failure_from_logs(logs: str, project: str = "unknown"):
    """Parse a log blob and attribute failures to atoms."""
    from services.failure_tracker import scan_logs_for_failures
    count = await scan_logs_for_failures(logs, project=project)
    return {"status": "ok", "failure_events_written": count}


@router.post("/decay")
async def enrich_decay():
    """Run decay job. CURRENTLY DISABLED — enable in services/decay_job.py first."""
    from services.decay_job import run_decay
    result = await run_decay()
    return {"status": result.get("status", "ok"), **result}


@router.post("/embeddings")
async def enrich_embeddings(limit: int = 2000, force: bool = False):
    """Embed atoms into ChromaDB. Only embeds atoms where embedded_at IS NULL.
    
    Uses embedded_at timestamp on atoms table to track state.
    Pass force=true to re-embed all atoms regardless of status.
    Auto-runs after: POST /api/v1/scan/enrich/similarity (which checks this first).
    """
    from services.chromadb import get_chromadb_service
    from services.database import get_db
    from datetime import datetime
    
    chroma = get_chromadb_service()
    if not chroma._initialized:
        await chroma.initialize()
    
    db = get_db()
    with db.get_connection() as conn:
        if force:
            atoms = conn.execute(
                "SELECT id, name, code FROM atoms ORDER BY occurrence_count DESC LIMIT %s",
                (limit,)
            ).fetchall()
        else:
            # Only un-embedded atoms
            atoms = conn.execute(
                "SELECT id, name, code FROM atoms WHERE embedded_at IS NULL ORDER BY occurrence_count DESC LIMIT %s",
                (limit,)
            ).fetchall()
    
    embedded = 0
    skipped = 0
    failed = 0
    now = datetime.utcnow().isoformat()
    
    for atom_id, name, code in atoms:
        text = f"{name} {(code or '')[:300]}"
        try:
            success = await chroma.add_document(
                collection_base="atoms",
                doc_id=atom_id,
                text=text,
                metadata={"name": name}
            )
            if success:
                # Mark embedded in postgres
                with db.get_connection() as conn:
                    conn.execute(
                        "UPDATE atoms SET embedded_at = %s WHERE id = %s",
                        (now, atom_id)
                    )
                    conn.commit()
                embedded += 1
            else:
                skipped += 1
        except Exception:
            failed += 1
    
    with db.get_connection() as conn:
        total_embedded = conn.execute("SELECT COUNT(*) FROM atoms WHERE embedded_at IS NOT NULL").fetchone()[0]
        total_atoms = conn.execute("SELECT COUNT(*) FROM atoms").fetchone()[0]
    
    return {
        "status": "ok",
        "newly_embedded": embedded,
        "skipped_already_done": skipped,
        "failed": failed,
        "total_embedded": total_embedded,
        "total_atoms": total_atoms,
        "remaining": total_atoms - total_embedded,
    }


@router.post("/all")
async def enrich_all():
    """Run all enrichment backfills in sequence."""
    from services.domain_classifier import backfill_domain
    from services.convention_analyzer import backfill_convention
    from services.exchange_linker import backfill_exchange_context
    from services.quality_tracker import bootstrap_quality_from_occurrence
    from services.compression_optimizer import run_compression_pass
    from services.database import get_db
    domain = await backfill_domain(5000)
    convention = await backfill_convention(5000)
    exchange = await backfill_exchange_context(500)
    quality = await bootstrap_quality_from_occurrence(5000)
    compressed = await run_compression_pass(100)

    # Refresh namespace coverage on scorecards
    db = get_db()
    with db.get_connection() as conn:
        conn.execute("""
            UPDATE atom_scorecards sc
            SET
                namespace_count = sub.ns_count,
                namespace_coverage = ROUND((sub.ns_count::numeric / 23.0), 3),
                namespaces_present = sub.ns_list
            FROM (
                SELECT target_id,
                    COUNT(DISTINCT namespace) as ns_count,
                    STRING_AGG(DISTINCT namespace, ',' ORDER BY namespace) as ns_list
                FROM meta_events GROUP BY target_id
            ) sub
            WHERE sc.atom_id = sub.target_id
        """)
        conn.commit()
        top = conn.execute("""
            SELECT namespace_count, ROUND(namespace_coverage * 100) as pct, COUNT(*) as atoms
            FROM atom_scorecards
            GROUP BY namespace_count, pct
            ORDER BY namespace_count DESC LIMIT 5
        """).fetchall()

    return {
        "status": "ok",
        "domain_classified": domain,
        "convention_analyzed": convention,
        "exchange_links_written": exchange,
        "quality_bootstrapped": quality,
        "atoms_compressed": compressed,
        "namespace_coverage_top5": [{"namespaces": r[0], "coverage_pct": r[1], "atoms": r[2]} for r in top],
    }
