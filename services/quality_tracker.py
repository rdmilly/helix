"""Quality Tracker - writes quality meta namespace."""
import logging
from services.database import get_db
from services.meta import get_meta_service
from datetime import datetime
log = logging.getLogger(__name__)
async def record_atom_verified(atom_id, context, success=True):
    import json
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        existing = conn.execute("SELECT new_value FROM meta_events WHERE target_id = %s AND namespace = 'quality' ORDER BY id DESC LIMIT 1", (atom_id,)).fetchone()
    data = json.loads(existing[0]) if existing else {'deploy_success_count': 0, 'deploy_failure_count': 0, 'contexts_verified': [], 'correction_count': 0}
    if success:
        data['deploy_success_count'] = data.get('deploy_success_count', 0) + 1
        ctxs = data.get('contexts_verified', [])
        if context not in ctxs: ctxs.append(context)
        data['contexts_verified'] = ctxs[-20:]
    else:
        data['deploy_failure_count'] = data.get('deploy_failure_count', 0) + 1
    total = data['deploy_success_count'] + data.get('deploy_failure_count', 0)
    data['deploy_success_rate'] = round(data['deploy_success_count'] / max(1, total), 3)
    data['last_verified_at'] = datetime.utcnow().isoformat()
    meta.write_meta('atoms', atom_id, 'quality', data, written_by='deployer_v1')
async def bootstrap_quality_from_occurrence(limit=5000):
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        atoms = conn.execute("SELECT a.id, a.name, a.occurrence_count FROM atoms a WHERE NOT EXISTS (SELECT 1 FROM meta_events m WHERE m.target_id = a.id AND m.namespace = 'quality') ORDER BY a.occurrence_count DESC LIMIT %s", (limit,)).fetchall()
    written = 0
    for atom_id, name, occ in atoms:
        meta.write_meta('atoms', atom_id, 'quality', {'deploy_success_count': occ, 'deploy_failure_count': 0, 'deploy_success_rate': 1.0 if occ > 0 else 0.0, 'contexts_verified': ['occurrence_bootstrap'], 'correction_count': 0, 'bootstrapped': True}, written_by='deployer_v1'); written += 1
    return written
