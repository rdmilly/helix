"""Similarity Cluster - writes similarity_cluster meta namespace.

Uses pgvector cosine similarity (<->) to find nearest neighbors.
All embeddings live in the `embeddings` table (source_type='atoms').
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
        # Get atoms that haven't been clustered yet
        unclustered = conn.execute("""
            SELECT a.id, a.name
            FROM atoms a
            WHERE NOT EXISTS (
                SELECT 1 FROM meta_events m
                WHERE m.target_id = a.id AND m.namespace = 'similarity_cluster'
            )
            AND EXISTS (
                SELECT 1 FROM embeddings e
                WHERE e.source_id = a.id AND e.source_type = 'atoms' AND e.embedding IS NOT NULL
            )
            LIMIT %s
        """, (limit,)).fetchall()

    if not unclustered:
        log.info('similarity_cluster: no unclustered atoms with embeddings')
        return 0

    log.info(f'similarity_cluster: clustering {len(unclustered)} atoms via pgvector')
    written = 0

    for atom_id, atom_name in unclustered:
        try:
            with db.get_connection() as conn:
                # Find N nearest neighbors using pgvector cosine distance
                neighbors = conn.execute("""
                    SELECT
                        e2.source_id as neighbor_id,
                        a2.name as neighbor_name,
                        1 - (e1.embedding <=> e2.embedding) as similarity
                    FROM embeddings e1
                    JOIN embeddings e2
                        ON e2.source_type = 'atoms'
                        AND e2.source_id != e1.source_id
                        AND e2.embedding IS NOT NULL
                    JOIN atoms a2 ON a2.id = e2.source_id
                    WHERE e1.source_id = %s
                      AND e1.source_type = 'atoms'
                      AND 1 - (e1.embedding <=> e2.embedding) >= %s
                    ORDER BY e1.embedding <=> e2.embedding
                    LIMIT %s
                """, (atom_id, SIMILARITY_THRESHOLD, NEIGHBOR_COUNT)).fetchall()

            if neighbors:
                meta.write_meta(
                    'atoms', atom_id, 'similarity_cluster',
                    {
                        'cluster_id': f'cluster_{atom_id[:8]}',
                        'cluster_size': len(neighbors) + 1,
                        'nearest_neighbors': [
                            {'atom_id': nid, 'name': nname, 'similarity': round(float(sim), 3)}
                            for nid, nname, sim in neighbors
                        ],
                        'clustered_at': datetime.utcnow().isoformat(),
                        'method': 'pgvector_cosine'
                    },
                    written_by='chromadb_v1'
                )
                written += 1
        except Exception as e:
            log.debug(f'similarity_cluster skipped {atom_id}: {e}')

    log.info(f'similarity_cluster: {written}/{len(unclustered)} atoms clustered')
    return written
