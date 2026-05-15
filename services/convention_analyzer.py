"""Convention Analyzer - writes convention meta namespace."""
import re, logging
from services.database import get_db
from services.meta import get_meta_service
log = logging.getLogger(__name__)
def _detect_naming(name):
    if re.match(r'^[a-z][a-z0-9_]*$', name): return 'snake_case'
    if re.match(r'^[a-z][a-zA-Z0-9]*$', name): return 'camelCase'
    if re.match(r'^[A-Z][a-zA-Z0-9]*$', name): return 'PascalCase'
    return 'mixed'
def _detect_error_pattern(code):
    if 'raise ' in code and 'return None' not in code: return 'raise'
    if 'return None' in code and 'raise ' not in code: return 'return_none'
    return 'mixed'
def _detect_return_style(code):
    returns = re.findall(r'\breturn\b', code)
    explicit = re.findall(r'\breturn\s+\S', code)
    if not returns: return 'no_return'
    if len(explicit) == len(returns): return 'always_explicit'
    return 'mixed'
def analyze_convention(name, code, is_async=False):
    return {'naming_style': _detect_naming(name), 'error_pattern': _detect_error_pattern(code), 'has_docstring': '"""' in code or "'''" in code, 'return_style': _detect_return_style(code), 'async_style': 'async' if is_async else 'sync', 'uses_type_hints': ':' in code and '->' in code, 'convention_version': '1.0'}
async def backfill_convention(limit=5000):
    db = get_db(); meta = get_meta_service()
    with db.get_connection() as conn:
        atoms = conn.execute("SELECT a.id, a.name, a.code FROM atoms a LEFT JOIN meta_events m ON m.target_id = a.id AND m.namespace = 'convention' WHERE m.id IS NULL LIMIT %s", (limit,)).fetchall()
    written = 0
    for atom_id, name, code in atoms:
        meta.write_meta('atoms', atom_id, 'convention', analyze_convention(name, code or '', 'async def' in (code or '')), written_by='convention_v1'); written += 1
    return written
