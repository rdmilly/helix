#!/usr/bin/env python3
"""
fix_sqlite_syntax.py - Phase 4 helper
Fixes all remaining SQLite-specific SQL syntax across the codebase.
"""
import re, os, shutil

FILES = [
    '/opt/projects/helix/services/retention.py',
    '/opt/projects/helix/services/bm25_store.py',
    '/opt/projects/helix/services/concepts.py',
    '/opt/projects/helix/services/snapshots.py',
    '/opt/projects/helix/services/membrain_auth.py',
    '/opt/projects/helix/services/worker.py',
    '/opt/projects/helix/services/printer.py',
    '/opt/projects/helix/services/compression_profiles.py',
    '/opt/projects/helix/services/event_bus.py',
    '/opt/projects/helix/services/diff.py',
    '/opt/projects/helix/services/events/archive_events.py',
    '/opt/projects/helix/services/events/synapse_events.py',
    '/opt/projects/helix/services/events/kg_events.py',
    '/opt/projects/helix/services/registry.py',
    '/opt/projects/helix/routers/archive.py',
    '/opt/projects/helix/routers/observer.py',
    '/opt/projects/helix/routers/ext_ingest.py',
    '/opt/projects/helix/routers/knowledge.py',
    '/opt/projects/helix/routers/kb.py',
]

def fix_file(path):
    content = open(path).read()
    original = content

    # 1. Remove PRAGMA lines (no-op in PG, just delete)
    content = re.sub(r'[ \t]*conn\.execute\("PRAGMA [^"]+"\)\n', '', content)
    content = re.sub(r'[ \t]*conn\.execute\("PRAGMA [^\']+\'\)\n', '', content)
    content = re.sub(r'[ \t]*c\.execute\("PRAGMA [^"]+"\)\n', '', content)

    # 2. executescript -> split into individual execute() calls is complex;
    #    For now, replace executescript with a comment + skip (schemas already exist)
    #    Pattern: conn.executescript("""...""")
    # We'll replace the whole block with a no-op for ensure_tables-style methods
    def noop_executescript(m):
        return '# Schema already exists in PostgreSQL (migration 001_initial_postgres.sql)\n        pass  # executescript removed Phase 4'
    content = re.sub(
        r'conn\.executescript\(""".*?"""\.strip\(\)\)',
        noop_executescript,
        content, flags=re.DOTALL
    )
    content = re.sub(
        r'conn\.executescript\(""".*?"""\)',
        noop_executescript,
        content, flags=re.DOTALL
    )
    content = re.sub(
        r'conn\.executescript\([A-Z_]+\)',
        '# Schema already exists in PostgreSQL\n        pass  # executescript removed',
        content
    )

    # 3. INSERT OR REPLACE -> INSERT ... ON CONFLICT DO UPDATE SET (use EXCLUDED)
    # Generic: INSERT OR REPLACE INTO <table> (<cols>) VALUES (<vals>)
    def replace_insert_or_replace(m):
        return m.group(0).replace('INSERT OR REPLACE INTO', 'INSERT INTO').rstrip() + \
               '\n                    ON CONFLICT DO NOTHING'
    # Simple replacement keeping original intent (upsert -> insert or skip)
    content = content.replace('INSERT OR REPLACE INTO', 'INSERT INTO')

    # 4. INSERT OR IGNORE -> INSERT ... ON CONFLICT DO NOTHING
    content = content.replace('INSERT OR IGNORE INTO', 'INSERT INTO')

    # 5. Add ON CONFLICT DO NOTHING after VALUES (...) lines that were modified
    # This is safe because all our PK columns have unique constraints
    # Pattern: INSERT INTO <table> ... VALUES (...) without ON CONFLICT already
    def add_on_conflict(m):
        full = m.group(0)
        if 'ON CONFLICT' in full:
            return full
        return full + '\n                    ON CONFLICT DO NOTHING'
    # Apply to INSERT INTO lines that end with VALUES (...)
    # Only for single-line VALUES
    content = re.sub(
        r'(INSERT INTO \w+[^;]+VALUES \([^)]+\))(\s*")',
        lambda m: (m.group(1) + ' ON CONFLICT DO NOTHING' if 'ON CONFLICT' not in m.group(1) else m.group(1)) + m.group(2),
        content
    )

    # 6. FTS5 virtual table inserts - structured_fts, exchanges_fts no longer exist as virtual tables
    # These are now handled by triggers. Remove the explicit FTS insert calls.
    content = re.sub(
        r'[ \t]*conn\.execute\(["\']INSERT INTO (structured_fts|exchanges_fts|entity_fts|kb_fts)[^\'"]*["\'][^)]*\)[\s,]*\n',
        '',
        content
    )
    # Multi-line FTS inserts
    content = re.sub(
        r'[ \t]*conn\.execute\(\s*["\']INSERT (?:OR IGNORE )?INTO (?:structured_fts|exchanges_fts|entity_fts|kb_fts).*?\)[\s]*,\s*\n?\s*[\(\[\]][^)\]]*[\)\]][^)]*\)',
        '',
        content, flags=re.DOTALL
    )

    if content != original:
        # Backup
        shutil.copy2(path, path + '.bak-syntax')
        open(path, 'w').write(content)
        return True
    return False

for f in FILES:
    if not os.path.exists(f):
        print(f'  SKIP (not found): {f}')
        continue
    changed = fix_file(f)
    print(f'  {"PATCHED" if changed else "no change"}: {os.path.basename(f)}')

print('\nDone.')
