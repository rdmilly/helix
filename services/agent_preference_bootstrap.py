"""Bootstrap agent_preference from exchange data."""
import re, logging
from services.database import get_db
from services.agent_preference_tracker import record_suggestion_outcome, ACTION_ACCEPTED
log = logging.getLogger(__name__)
async def bootstrap_from_exchanges(limit=200):
    db = get_db()
    with db.get_connection() as conn:
        atoms = {row[1]: row[0] for row in conn.execute('SELECT id, name FROM atoms').fetchall()}
        builds = conn.execute("SELECT id, what_happened, decision FROM exchanges WHERE exchange_type = 'build' AND what_happened IS NOT NULL LIMIT %s", (limit,)).fetchall()
    accepted = 0
    for ex_id, text, decision in builds:
        full_text = (text or '') + ' ' + (decision or '')
        for name, atom_id in atoms.items():
            if re.search(r'\b' + re.escape(name) + r'\b', full_text):
                await record_suggestion_outcome(atom_id=atom_id, action=ACTION_ACCEPTED, session_id=ex_id, context='bootstrap_from_build_exchange')
                accepted += 1
    return {'accepted_signals': accepted, 'exchanges_processed': len(builds)}
