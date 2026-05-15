"""Agent Preference Tracker - writes agent_preference meta namespace."""
import logging
from datetime import datetime
from services.database import get_db
from services.meta import get_meta_service
log = logging.getLogger(__name__)
ACTION_ACCEPTED = 'accepted'
ACTION_REJECTED = 'rejected'
ACTION_MODIFIED = 'modified'
async def record_suggestion_outcome(atom_id, action, session_id=None, original_code=None, replacement_code=None, context=None):
    import json
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        existing = conn.execute("SELECT new_value FROM meta_events WHERE target_id = %s AND namespace = 'agent_preference' ORDER BY id DESC LIMIT 1", (atom_id,)).fetchone()
    data = json.loads(existing[0]) if existing else {'suggestion_count': 0, 'acceptance_count': 0, 'modification_count': 0, 'rejection_count': 0, 'corrections': []}
    data['suggestion_count'] = data.get('suggestion_count', 0) + 1
    data['last_suggested_at'] = datetime.utcnow().isoformat()
    if action == ACTION_ACCEPTED: data['acceptance_count'] = data.get('acceptance_count', 0) + 1
    elif action == ACTION_REJECTED: data['rejection_count'] = data.get('rejection_count', 0) + 1
    elif action == ACTION_MODIFIED:
        data['modification_count'] = data.get('modification_count', 0) + 1
        if replacement_code:
            corrections = data.get('corrections', [])
            corrections.append({'original': (original_code or '')[:500], 'replaced_with': replacement_code[:500], 'at': datetime.utcnow().isoformat(), 'session_id': session_id, 'context': context})
            data['corrections'] = corrections[-20:]
    data['acceptance_rate'] = round(data['acceptance_count'] / max(1, data['suggestion_count']), 3)
    meta.write_meta('atoms', atom_id, 'agent_preference', data, written_by='assembler_v1')
