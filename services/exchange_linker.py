"""Exchange Linker - writes exchange_context meta namespace."""
import re, logging, json
from services.database import get_db
from services.meta import get_meta_service
log = logging.getLogger(__name__)
CONTEXT_TYPE_KEYWORDS = {'debug': ['error','bug','fix','crash','traceback','fail'],'refactor': ['refactor','clean','improve','rewrite'],'test': ['test','spec','assert','mock'],'review': ['review','check','analyze','audit'],'build': ['build','create','add','implement','write','new'],'discuss': ['why','how','what','explain']}
def _detect_context_type(text):
    for ctype, keywords in CONTEXT_TYPE_KEYWORDS.items():
        if any(kw in text.lower() for kw in keywords): return ctype
    return 'build'
async def link_exchange_to_atoms(exchange_id, exchange_text, context_type=None):
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        atoms = {row[1]: row[0] for row in conn.execute('SELECT id, name FROM atoms').fetchall()}
    ctype = context_type or _detect_context_type(exchange_text)
    linked = 0
    for name, atom_id in atoms.items():
        if re.search(r'\b' + re.escape(name) + r'\b', exchange_text):
            with db.get_connection() as conn:
                existing = conn.execute("SELECT new_value FROM meta_events WHERE target_id = %s AND namespace = 'exchange_context' ORDER BY id DESC LIMIT 1", (atom_id,)).fetchone()
            data = json.loads(existing[0]) if existing else {'exchange_ids': [], 'context_types': [], 'total_references': 0}
            ids = data.get('exchange_ids', [])
            if exchange_id not in ids: ids.append(exchange_id)
            types = data.get('context_types', [])
            if ctype not in types: types.append(ctype)
            meta.write_meta('atoms', atom_id, 'exchange_context', {'exchange_ids': ids[-50:], 'context_types': types, 'last_exchange_id': exchange_id, 'total_references': len(ids), 'most_recent_context': ctype}, written_by='worker_v1')
            linked += 1
    return linked
async def backfill_exchange_context(limit=500):
    db = get_db()
    with db.get_connection() as conn:
        exchanges = conn.execute("SELECT id, what_happened FROM exchanges WHERE exchange_type IN ('build', 'deploy', 'review') AND what_happened IS NOT NULL ORDER BY created_at DESC LIMIT %s", (limit,)).fetchall()
    total = 0
    for ex_id, content in exchanges:
        if content: total += await link_exchange_to_atoms(ex_id, content)
    return total
