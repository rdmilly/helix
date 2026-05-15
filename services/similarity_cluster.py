"""Similarity Cluster - writes similarity_cluster meta namespace."""
import logging
from services.database import get_db
from services.meta import get_meta_service
from datetime import datetime
log = logging.getLogger(__name__)
NEIGHBOR_COUNT = 5
SIMILARITY_THRESHOLD = 0.3
async def build_similarity_clusters(limit=500):
    from config import CHROMADB_HOST, CHROMADB_PORT
    import httpx
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        atoms = conn.execute("SELECT a.id, a.name FROM atoms a WHERE NOT EXISTS (SELECT 1 FROM meta_events m WHERE m.target_id = a.id AND m.namespace = 'similarity_cluster') LIMIT %s", (limit,)).fetchall()
    chromadb_url = f'http://{CHROMADB_HOST}:{CHROMADB_PORT}'
    written = 0
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            col_resp = await client.get(f'{chromadb_url}/api/v1/collections')
            cols = col_resp.json()
            atom_col = next((c for c in cols if 'atom' in c.get('name', '').lower()), None)
            if not atom_col:
                log.warning('No atoms ChromaDB collection found. Run embedding pass first.')
                return 0
            col_id = atom_col['id']
        except Exception as e:
            log.warning(f'ChromaDB not reachable: {e}')
            return 0
        for atom_id, name in atoms:
            try:
                resp = await client.post(f'{chromadb_url}/api/v1/collections/{col_id}/query', json={'query_texts': [name], 'n_results': NEIGHBOR_COUNT + 1, 'include': ['distances']})
                result = resp.json()
                ids = result.get('ids', [[]])[0]
                distances = result.get('distances', [[]])[0]
                neighbors = [{'atom_id': nid, 'similarity': round(1 - dist, 3)} for nid, dist in zip(ids, distances) if nid != atom_id and (1 - dist) >= SIMILARITY_THRESHOLD]
                if neighbors:
                    meta.write_meta('atoms', atom_id, 'similarity_cluster', {'cluster_id': f'cluster_{atom_id[:8]}', 'cluster_size': len(neighbors) + 1, 'nearest_neighbors': neighbors[:5], 'clustered_at': datetime.utcnow().isoformat()}, written_by='chromadb_v1')
                    written += 1
            except Exception as e:
                log.debug(f'Similarity cluster skipped {atom_id}: {e}')
    log.info(f'Similarity cluster: {written} atoms clustered')
    return written
