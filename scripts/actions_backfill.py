#!/usr/bin/env python3
"""
actions_backfill.py — Seed ACTIONS atoms from historical tool call data.

Reads all observer_actions, rebuilds session sequences, counts cross-session
frequency, and promotes high-frequency patterns directly to the atom store.

Run: python3 /opt/projects/helix/scripts/actions_backfill.py
"""
import sqlite3
import json
import hashlib
import uuid
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path

DB_PATH = '/app/data/cortex.db'
SEQUENCE_WINDOW = 5      # max tools in a sequence
MIN_LENGTH = 2           # min tools to be a sequence
PROMOTE_THRESHOLD = 3    # min occurrences to become an atom


def _hash_seq(seq):
    return hashlib.md5(json.dumps(seq).encode()).hexdigest()[:12]


def _short_name(seq):
    """e.g. ['gateway__ssh_execute', 'helix__helix_file_write'] -> 'ssh_execute__file_write'"""
    parts = []
    for t in seq[:3]:
        # Strip server prefix (gateway__, workingdocs__, etc.)
        short = t.split('__')[-1] if '__' in t else t
        # Truncate long names
        short = short[:20]
        parts.append(short)
    return '__then__'.join(parts)


def run_backfill():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print(f"[backfill] Connecting to {DB_PATH}")

    # Load all tool calls ordered by session + timestamp
    rows = conn.execute("""
        SELECT session_id, tool_name, timestamp, error
        FROM observer_actions
        WHERE tool_name != ''
        ORDER BY session_id, timestamp
    """).fetchall()

    print(f"[backfill] Loaded {len(rows)} tool calls")

    # Group by session
    sessions = defaultdict(list)
    for r in rows:
        sid = r['session_id'] or '_global_'
        sessions[sid].append(r['tool_name'])

    print(f"[backfill] {len(sessions)} sessions")

    # Build sequence frequency map across all sessions
    seq_counts = defaultdict(int)       # hash -> count
    seq_data = {}                        # hash -> {seq, sessions}

    for sid, tools in sessions.items():
        seen_in_session = set()  # dedupe within session
        for start in range(len(tools)):
            for length in range(MIN_LENGTH, min(SEQUENCE_WINDOW + 1, len(tools) - start + 1)):
                seq = tools[start:start + length]
                h = _hash_seq(seq)
                if h not in seen_in_session:
                    seq_counts[h] += 1
                    seq_data[h] = seq
                    seen_in_session.add(h)

    print(f"[backfill] {len(seq_counts)} unique sequences found")

    # Find promotable sequences
    promotable = [(h, seq_data[h], seq_counts[h])
                  for h, count in seq_counts.items()
                  if count >= PROMOTE_THRESHOLD]
    promotable.sort(key=lambda x: x[2], reverse=True)

    print(f"[backfill] {len(promotable)} sequences at threshold ({PROMOTE_THRESHOLD}+)")
    print()

    # Ensure observer_sequences has promoted_to_atom column
    try:
        conn.execute("ALTER TABLE observer_sequences ADD COLUMN promoted_to_atom TEXT DEFAULT NULL")
        conn.commit()
    except Exception:
        pass  # already exists

    promoted = 0
    skipped = 0

    for h, seq, count in promotable[:100]:  # cap at 100 initial atoms
        atom_name = _short_name(seq)
        full_name = ' -> '.join(seq)

        # Check if atom already exists
        existing = conn.execute(
            "SELECT id FROM atoms WHERE name = ? LIMIT 1", (atom_name,)
        ).fetchone()

        if existing:
            # Update occurrence count
            conn.execute(
                "UPDATE atoms SET occurrence_count = ?, last_seen = ? WHERE name = ?",
                (count, datetime.now(timezone.utc).isoformat(), atom_name)
            )
            skipped += 1
            continue

        # Build code representation
        code_lines = [
            f"# ACTIONS pattern: {full_name}",
            f"# Frequency: {count} sessions",
            f"# Steps: {len(seq)}",
            "",
        ]
        for i, tool in enumerate(seq):
            server = tool.split('__')[0] if '__' in tool else 'unknown'
            name = tool.split('__')[-1] if '__' in tool else tool
            code_lines.append(f"# Step {i+1}: {name} (via {server})")

        code = '\n'.join(code_lines)

        # Build meta with sequence info
        meta = json.dumps({
            'structural': {
                'sequence': seq,
                'length': len(seq),
                'type': 'ACTIONS',
            },
            'semantic': {
                'category': 'ACTIONS',
                'frequency': count,
                'source': 'backfill',
            },
            'provenance': {
                'first_seen': 'backfill_2026-04-20',
                'occurrence_count': count,
            }
        })

        atom_id = f"act_{h}"
        now = datetime.now(timezone.utc).isoformat()

        try:
            conn.execute("""
                INSERT OR IGNORE INTO atoms
                (id, name, full_name, code, template, parameters_json,
                 structural_fp, semantic_fp, fp_version,
                 first_seen, last_seen, occurrence_count, meta)
                VALUES (?, ?, ?, ?, NULL, '[]', ?, ?, 'v1', ?, ?, ?, ?)
            """, (
                atom_id, atom_name, full_name, code,
                h, h + '_sem',  # structural + semantic fingerprints
                now, now, count, meta
            ))
            conn.commit()

            print(f"  [{count:3d}x] {atom_name}")
            print(f"        {' -> '.join(s.split('__')[-1] for s in seq)}")
            promoted += 1

        except Exception as e:
            print(f"  [ERR] {atom_name}: {e}")

    conn.close()

    print()
    print(f"[backfill] Done. Promoted: {promoted} | Updated: {skipped}")
    print(f"[backfill] Run helix_search_kb or scaffold_query to verify")


if __name__ == '__main__':
    run_backfill()
