"""Failure Tracker - writes failure meta namespace."""
import re, logging
from services.database import get_db
from services.meta import get_meta_service
from datetime import datetime
log = logging.getLogger(__name__)
async def record_atom_failure(atom_id, error_type, error_message, project=None, is_rollback=False):
    import json
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        existing = conn.execute("SELECT new_value FROM meta_events WHERE target_id = %s AND namespace = 'failure' ORDER BY id DESC LIMIT 1", (atom_id,)).fetchone()
        occ = conn.execute('SELECT occurrence_count FROM atoms WHERE id = %s', (atom_id,)).fetchone()
    data = json.loads(existing[0]) if existing else {'error_count': 0, 'rollback_count': 0, 'error_contexts': [], 'failure_rate': 0.0}
    if is_rollback: data['rollback_count'] = data.get('rollback_count', 0) + 1
    else:
        data['error_count'] = data.get('error_count', 0) + 1; data['last_error_type'] = error_type; data['last_error_at'] = datetime.utcnow().isoformat()
        contexts = data.get('error_contexts', []); contexts.append({'type': error_type, 'message': error_message[:200], 'project': project, 'at': datetime.utcnow().isoformat()}); data['error_contexts'] = contexts[-10:]
    occ_count = occ[0] if occ else 1
    total_failures = data['error_count'] + data.get('rollback_count', 0)
    data['failure_rate'] = round(total_failures / max(1, occ_count), 3)
    meta.write_meta('atoms', atom_id, 'failure', data, written_by='deployer_v1')
async def scan_logs_for_failures(log_text, project='unknown'):
    db = get_db()
    with db.get_connection() as conn:
        atoms = {row[1]: row[0] for row in conn.execute('SELECT id, name FROM atoms').fetchall()}
    tb_pattern = re.compile(r'in\s+([a-z_][a-zA-Z0-9_]+)\b')
    error_pattern = re.compile(r'(\w+Error|\w+Exception):\s*(.+)')
    found_fns = set(tb_pattern.findall(log_text))
    error_match = error_pattern.search(log_text)
    error_type = error_match.group(1) if error_match else 'UnknownError'
    error_msg = error_match.group(2)[:200] if error_match else log_text[:200]
    recorded = 0
    for fn_name in found_fns:
        if fn_name in atoms:
            await record_atom_failure(atoms[fn_name], error_type, error_msg, project=project); recorded += 1
    return recorded
