"""Compression Optimizer - writes compression meta namespace."""
import logging
from services.database import get_db
from services.meta import get_meta_service
from datetime import datetime
log = logging.getLogger(__name__)
SYMBOL_POOL = list('αβγδεζηθικλμνξοπρστυφχψω') + [f'α{i}' for i in range(1, 50)]
def _next_symbol(used):
    for s in SYMBOL_POOL:
        if s not in used: return s
    return f'atom_{len(used)}'
async def assign_compression(atom_id):
    import json
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        existing = conn.execute("SELECT new_value FROM meta_events WHERE namespace = 'compression'").fetchall()
        atom_code = conn.execute('SELECT code FROM atoms WHERE id = %s', (atom_id,)).fetchone()
    used = set()
    for row in existing:
        try:
            d = json.loads(row[0])
            if d.get('shorthand'): used.add(d['shorthand'])
        except Exception: pass
    symbol = _next_symbol(used)
    code_len = len(atom_code[0]) if atom_code and atom_code[0] else 100
    token_savings = max(0, (code_len // 4) - len(symbol))
    data = {'shorthand': symbol, 'dictionary_version': 'v1.0', 'token_savings_estimate': token_savings, 'usage_count': 0, 'acceptance_rate': 1.0, 'assigned_at': datetime.utcnow().isoformat()}
    meta.write_meta('atoms', atom_id, 'compression', data, written_by='optimizer_v1')
    return data
async def run_compression_pass(limit=100):
    db = get_db()
    with db.get_connection() as conn:
        candidates = conn.execute("SELECT a.id FROM atoms a WHERE a.occurrence_count > 8 AND NOT EXISTS (SELECT 1 FROM meta_events m WHERE m.target_id = a.id AND m.namespace = 'compression') ORDER BY a.occurrence_count DESC LIMIT %s", (limit,)).fetchall()
    count = 0
    for (atom_id,) in candidates:
        await assign_compression(atom_id); count += 1
    return count
