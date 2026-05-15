"""Similarity Cluster - writes similarity_cluster meta namespace.

Uses pgvector IVFFlat ANN index. CTE pattern forces index usage.
Query time: ~0.23s per atom with 4271 embeddings indexed.
"""
import logging
from services.database import get_db
from services.meta import get_meta_service
from datetime import datetime

log = logging.getLogger(__name__)
NEIGHBOR_COUNT = 5
SIMILARITY_THRESHOLD = 0.3

async def build_similarity_clusters(limit: int = 500) -> int:
    db = get_db()
    meta = get_meta_service()

    with db.get_connection() as conn:
        unclustered = conn.execute("""
            SELECT a.id, a.name
            FROM atoms a
            JOIN embeddings e ON e.source_id = a.id AND e.source_type = 'atoms'
            WHERE e.embedding IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM meta_events m
                WHERE m.target_id = a.id AND m.namespace = 'similarity_cluster'
            )
            LIMIT %s
        """, (limit,)).fetchall()

    if not unclustered:
        log.info('similarity_cluster: all atoms clustered')
        return 0

    log.info(f'similarity_cluster: clustering {len(unclustered)} atoms')
    written = 0

    for atom_id, atom_name in unclustered:
        try:
            with db.get_connection() as conn:
                conn.execute('SET enable_seqscan = off')
                # CTE forces IVFFlat index for ANN lookup (~0.23s per atom)
                rows = conn.execute("""
                    WITH target AS (
                        SELECT embedding FROM embeddings
                        WHERE source_id = %s AND source_type = 'atoms'
                    )
                    SELECT e.source_id, a.name, 1 - (e.embedding <=> t.embedding) as sim
                    FROM embeddings e, target t
                    JOIN atoms a ON a.id = e.source_id
                    WHERE e.source_type = 'atoms' AND e.source_id != %s
                    ORDER BY e.embedding <=> t.embedding
                    LIMIT %s
                """, (atom_id, atom_id, NEIGHBOR_COUNT + 10)).fetchall()

            # Filter by threshold after ANN retrieval
            neighbors = [(nid, nname, float(sim)) for nid, nname, sim in rows if float(sim) >= SIMILARITY_THRESHOLD]

            meta.write_meta(
                'atoms', atom_id, 'similarity_cluster',
                {
                    'cluster_size': len(neighbors) + 1,
                    'nearest_neighbors': [
                        {'atom_id': nid, 'name': nname, 'similarity': round(sim, 3)}
                        for nid, nname, sim in neighbors[:NEIGHBOR_COUNT]
                    ],
                    'has_neighbors': len(neighbors) > 0,
                    'clustered_at': datetime.utcnow().isoformat(),
                    'method': 'pgvector_ivfflat'
                },
                written_by='similarity_v2'
            )
            if neighbors:
                written += 1
        except Exception as e:
            log.warning(f'similarity_cluster {atom_id}: {e}')

    log.info(f'similarity_cluster: {written}/{len(unclustered)} atoms found neighbors')
    return written
