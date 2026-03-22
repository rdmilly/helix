"""Intelligence Chain Service — Phase 3

Builds the intent→ADR→impl chain in the KG, writes ADR files,
calculates recurrence scores, and auto-generates journal entries.

Called by the Haiku reconciler after extracting decisions.
"""
import json
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from services import pg_sync

log = logging.getLogger("helix.intelligence_chain")

ADR_DIR = Path("/app/working-kb/adrs")
JOURNAL_PATH = Path("/app/working-kb/journal.md")


# ── KG Chain ──────────────────────────────────────────────────────────────

def build_kg_chain(decision_text: str, session_id: str, conn) -> dict:
    """Wire intent→ADR→impl nodes in KG for a decision."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        # Entity: the decision itself
        adr_id = "ADR-" + hashlib.sha256(decision_text.encode()).hexdigest()[:8].upper()
        conn.execute(
            """INSERT INTO entities (id, name, entity_type, description, attributes_json, first_seen, last_seen, meta, mention_count)
               VALUES (%s, %s, %s, %s, %s, %s, %s, '{}', 1)
               ON CONFLICT (name) DO UPDATE SET
                 mention_count = entities.mention_count + 1,
                 last_seen = EXCLUDED.last_seen,
                 description = CASE WHEN entities.description = '' THEN EXCLUDED.description ELSE entities.description END""",
            (adr_id, adr_id, 'adr', decision_text[:300], json.dumps({'session_id': session_id, 'full': decision_text[:1000]}), now, now)
        )
        # Entity: the session (intent node)
        conn.execute(
            """INSERT INTO entities (id, name, entity_type, description, attributes_json, first_seen, last_seen, meta, mention_count)
               VALUES (%s, %s, %s, %s, %s, %s, %s, '{}', 1)
               ON CONFLICT (name) DO UPDATE SET mention_count = entities.mention_count + 1, last_seen = EXCLUDED.last_seen""",
            (session_id[:12], session_id, 'session', f'Session: {session_id}', '{}', now, now)
        )
        # Relationship: session -[produced]-> adr
        conn.execute(
            """INSERT INTO kg_relationships (source_name, target_name, relation_type, description, session_id, created_at)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (source_name, target_name, relation_type) DO UPDATE SET
                 created_at = EXCLUDED.created_at""",
            (session_id, adr_id, 'produced', f'Session produced decision: {decision_text[:100]}', session_id, now)
        )
        conn.commit()
        return {'adr_id': adr_id, 'status': 'chained'}
    except Exception as e:
        log.error(f"build_kg_chain error: {e}")
        try: conn.rollback()
        except: pass
        return {'error': str(e)}


# ── ADR File Writer ────────────────────────────────────────────────────────

def write_adr_file(adr_id: str, decision_text: str, session_id: str, date_str: str) -> bool:
    """Write an ADR markdown file to working-kb/adrs/."""
    try:
        ADR_DIR.mkdir(parents=True, exist_ok=True)
        path = ADR_DIR / f"{adr_id}.md"
        if path.exists():
            return False  # already written
        content = f"""# {adr_id}: {decision_text[:80]}
**Date:** {date_str}
**Status:** Decided
**Session:** {session_id}

## Decision
{decision_text}

## Context
Auto-extracted by Haiku reconciler from session {session_id}.

## Consequences
_To be filled in manually or by subsequent reconciler runs._
"""
        path.write_text(content, encoding='utf-8')
        log.info(f"Wrote {path}")
        return True
    except Exception as e:
        log.error(f"write_adr_file {adr_id}: {e}")
        return False


# ── Recurrence Scoring ────────────────────────────────────────────────────

def update_recurrence_scores(conn) -> int:
    """Update mention_count-based recurrence on entities. Returns count updated."""
    try:
        # Entities mentioned in 3+ sessions are scaffold candidates
        result = conn.execute(
            """UPDATE entities SET
                 attributes_json = attributes_json || jsonb_build_object(
                   'recurrence_score', mention_count,
                   'scaffold_candidate', mention_count >= 3
                 )
               WHERE entity_type = 'adr'
               AND mention_count >= 2
               RETURNING id"""
        ).fetchall()
        conn.commit()
        return len(result)
    except Exception as e:
        log.error(f"update_recurrence: {e}")
        try: conn.rollback()
        except: pass
        return 0


# ── Journal Auto-generation ───────────────────────────────────────────────

def append_journal_entry(date_str: str, session_id: str, decisions: list, patterns: list) -> bool:
    """Append a session summary entry to journal.md."""
    try:
        if not decisions and not patterns:
            return False
        JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        lines = [f"\n---\n\n## {date_str} — session {session_id[:8]}"]
        if decisions:
            lines.append("\n### Decisions")
            for d in decisions[:5]:
                lines.append(f"- {d[:120]}")
        if patterns:
            lines.append("\n### Patterns")
            for p in patterns[:3]:
                lines.append(f"- {p[:120]}")
        entry = "\n".join(lines) + "\n"
        with open(JOURNAL_PATH, 'a', encoding='utf-8') as f:
            f.write(entry)
        return True
    except Exception as e:
        log.error(f"append_journal: {e}")
        return False

def write_file_journal_entry(path: str, session_id: str, context: str = '', git_result: dict = None) -> bool:
    """Write a structured journal entry for a file edit.
    
    Called by file.written pipeline to document every real code change
    as a public engineering journal entry.
    """
    try:
        from pathlib import Path as _Path
        from datetime import datetime, timezone
        
        JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        time_str = datetime.now(timezone.utc).strftime('%H:%M UTC')
        rel_path = path
        # Try to make path relative to known roots
        for root in ['/opt/projects/memory-ext/', '/opt/projects/helix/', '/opt/projects/']:
            if path.startswith(root):
                rel_path = path[len(root):]
                break
        
        commit_info = ''
        if git_result and git_result.get('status') == 'committed':
            msg = git_result.get('commit_msg', '')
            pushed = '↑ pushed' if git_result.get('pushed') else 'local only'
            commit_info = f'\n- git: {msg} ({pushed})'
        
        lines = [
            f'\n---\n',
            f'## {date_str} {time_str} — {rel_path}',
        ]
        if context:
            lines.append(f'- {context}')
        lines.append(f'- file: `{rel_path}`')
        if commit_info:
            lines.append(commit_info.strip())
        lines.append(f'- session: {session_id[:12]}')
        
        entry = '\n'.join(lines) + '\n'
        with open(JOURNAL_PATH, 'r+', encoding='utf-8') as f:
            existing = f.read()
            # Insert after the header line
            header_end = existing.find('\n', existing.find('_Auto-appended')) + 1
            if header_end > 0:
                f.seek(header_end)
                rest = existing[header_end:]
                f.write(entry + rest)
            else:
                f.seek(0, 2)  # append
                f.write(entry)
        return True
    except Exception as e:
        log.error(f"write_file_journal: {e}")
        return False



# ── Main chain runner ─────────────────────────────────────────────────────

def run_intelligence_chain(session_id: str, decisions: list, patterns: list,
                           failures: list, date_str: str = None) -> dict:
    """Run full Phase 3 chain for a session's extracted intelligence."""
    if not date_str:
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    stats = {'kg_chained': 0, 'adrs_written': 0, 'recurrence_updated': 0, 'journal': False}

    conn = pg_sync.sqlite_conn()
    try:
        for decision in decisions:
            text = decision.get('decision') or decision.get('text') or str(decision)
            if not text or len(text) < 10:
                continue
            result = build_kg_chain(text, session_id, conn)
            if 'adr_id' in result:
                stats['kg_chained'] += 1
                if write_adr_file(result['adr_id'], text, session_id, date_str):
                    stats['adrs_written'] += 1

        stats['recurrence_updated'] = update_recurrence_scores(conn)

        decision_texts = [d.get('decision') or d.get('text', '') for d in decisions]
        pattern_texts = [p.get('content') or p.get('text', '') for p in patterns]
        if append_journal_entry(date_str, session_id, decision_texts, pattern_texts):
            stats['journal'] = True

    finally:
        conn.close()

    return stats
