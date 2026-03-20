#!/usr/bin/env python3
"""
Helix Phase 7D — Context Lifecycle Bootstrap
============================================
Bootstraps lifecycle sessions from FTS conversations,
scores significance, and verifies the master context endpoint.

Usage:
  python3 /opt/projects/helix/scripts/lifecycle_phase7d.py [--dry-run] [--limit N]
"""
import sys
import json
import time
import sqlite3
import argparse
import requests
from collections import defaultdict
from datetime import datetime, timezone

HELIX_URL = "http://127.0.0.1:9050"
FTS_DB    = "/opt/projects/helix/data/conversations_fts.db"
CORTEX_DB = "/opt/projects/helix/data/cortex.db"

def log(m): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)

def get_fts_sessions():
    """Read all unique sessions from FTS with aggregated metadata."""
    conn = sqlite3.connect(FTS_DB, timeout=10)
    rows = conn.execute("SELECT c0, c4, c6 FROM conversation_fts_content").fetchall()
    conn.close()

    sessions = defaultdict(lambda: {
        "session_id": "",
        "source": "",
        "model": "",
        "name": "",
        "timestamp": "",
        "chunks": 0,
        "has_decision": False,
        "has_failure": False,
        "has_code": False,
        "topic_hints": [],
        "char_count": 0,
    })

    for (chunk_id, topic_hint, meta_json) in rows:
        try:
            meta = json.loads(meta_json) if meta_json else {}
        except:
            meta = {}
        sid = meta.get("session_id") or chunk_id.split(":")[0]
        s = sessions[sid]
        s["session_id"] = sid
        s["source"] = meta.get("source", "")
        s["model"] = meta.get("model", "")
        s["name"] = meta.get("name", "")
        if not s["timestamp"]:
            s["timestamp"] = meta.get("timestamp", "")
        s["chunks"] += 1
        s["char_count"] += int(meta.get("char_count", 0))
        if meta.get("has_decision") == "True": s["has_decision"] = True
        if meta.get("has_failure") == "True":  s["has_failure"] = True
        if meta.get("has_code") == "True":     s["has_code"] = True
        if topic_hint and len(s["topic_hints"]) < 3:
            s["topic_hints"].append(topic_hint[:120])

    return list(sessions.values())


def score_significance(s):
    """Score 0-100 based on content signals."""
    score = 0.0
    if s["has_decision"]: score += 30
    if s["has_code"]:     score += 20
    if s["has_failure"]:  score += 10
    # Chunk density
    score += min(s["chunks"] * 2, 20)
    # Character depth
    score += min(s["char_count"] / 500, 20)
    return min(round(score, 1), 100.0)


def get_existing_session_ids():
    conn = sqlite3.connect(CORTEX_DB, timeout=10)
    rows = conn.execute("SELECT id FROM sessions").fetchall()
    conn.close()
    return {r[0] for r in rows}


def bootstrap_sessions(dry_run=False, limit=None):
    log("=== Phase 7D: Session Bootstrap ===")
    fts_sessions = get_fts_sessions()
    existing = get_existing_session_ids()
    log(f"  FTS sessions: {len(fts_sessions)} | Existing lifecycle: {len(existing)}")

    new_sessions = [s for s in fts_sessions if s["session_id"] not in existing]
    if limit:
        new_sessions = new_sessions[:limit]
    log(f"  To create: {len(new_sessions)}")

    if dry_run:
        log("  DRY RUN — sample:")
        for s in new_sessions[:5]:
            sig = score_significance(s)
            log(f"    [{sig:5.1f}] {s['session_id'][:40]} chunks={s['chunks']} dec={s['has_decision']} code={s['has_code']}")
        return 0

    ok = err = 0
    for s in new_sessions:
        sig = score_significance(s)
        summary = " | ".join(s["topic_hints"][:2]) if s["topic_hints"] else ""
        payload = {
            "session_id": s["session_id"],
            "provider": s["source"] or "unknown",
            "model": s["model"] or "",
            "tags": [],
            "meta": {
                "name": s["name"],
                "chunks": s["chunks"],
                "char_count": s["char_count"],
                "has_decision": s["has_decision"],
                "has_code": s["has_code"],
                "has_failure": s["has_failure"],
                "significance": sig,
                "source_timestamp": s["timestamp"],
            }
        }
        try:
            resp = requests.post(f"{HELIX_URL}/api/v1/lifecycle/session/start",
                json=payload, timeout=15)
            resp.raise_for_status()
            # Update significance directly — lifecycle/start sets it to 0
            # Patch via direct DB write since there's no PATCH endpoint yet
            ok += 1
        except Exception as e:
            err += 1
            if err <= 3: log(f"  ERR {s['session_id'][:30]}: {e}")

        if ok % 50 == 0 and ok > 0:
            log(f"  Progress: {ok}/{len(new_sessions)}")
        time.sleep(0.01)

    # Patch significance scores in bulk via SQLite
    log(f"  Created {ok} sessions ({err} errors). Patching significance scores...")
    conn = sqlite3.connect(CORTEX_DB, timeout=10)
    patched = 0
    for s in new_sessions:
        sig = score_significance(s)
        conn.execute("UPDATE sessions SET significance = ? WHERE id = ?", (sig, s["session_id"]))
        patched += 1
    conn.commit()
    conn.close()
    log(f"  Patched {patched} significance scores")
    return ok


def verify_context_endpoints():
    log("=== Verifying context endpoints ===")
    # Inject
    try:
        r = requests.post(f"{HELIX_URL}/api/v1/context/inject",
            json={"query": "helix deployment docker", "max_atoms": 3, "max_sessions": 3},
            timeout=15)
        data = r.json()
        log(f"  /context/inject: {len(data.get('atoms', []))} atoms, {len(data.get('sessions', []))} sessions")
    except Exception as e:
        log(f"  /context/inject ERROR: {e}")

    # Search
    try:
        r = requests.get(f"{HELIX_URL}/api/v1/search/atoms?query=docker&limit=3", timeout=10)
        data = r.json()
        log(f"  /search/atoms: {data.get('count', 0)} results")
    except Exception as e:
        log(f"  /search/atoms ERROR: {e}")

    # Lifecycle sessions list
    try:
        r = requests.get(f"{HELIX_URL}/api/v1/lifecycle/sessions?limit=5", timeout=10)
        data = r.json()
        total = data.get("count", 0)
        log(f"  /lifecycle/sessions: {total} returned (limit 5)")
    except Exception as e:
        log(f"  /lifecycle/sessions ERROR: {e}")


def show_stats():
    log("=== Final Stats ===")
    conn = sqlite3.connect(CORTEX_DB, timeout=10)
    total = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    sig_dist = conn.execute("""
        SELECT
            SUM(CASE WHEN significance >= 60 THEN 1 ELSE 0 END) as high,
            SUM(CASE WHEN significance >= 30 AND significance < 60 THEN 1 ELSE 0 END) as mid,
            SUM(CASE WHEN significance < 30 THEN 1 ELSE 0 END) as low
        FROM sessions
    """).fetchone()
    top = conn.execute("""
        SELECT id, significance, provider, meta FROM sessions
        ORDER BY significance DESC LIMIT 5
    """).fetchall()
    conn.close()

    log(f"  Total lifecycle sessions: {total}")
    if sig_dist:
        log(f"  Significance: high(>=60)={sig_dist[0]} mid(30-59)={sig_dist[1]} low(<30)={sig_dist[2]}")
    log("  Top 5 by significance:")
    for row in top:
        try:
            m = json.loads(row[3]) if row[3] else {}
            name = m.get("name", "") or m.get("analysis", {}).get("summary", "")[:40]
        except:
            name = ""
        log(f"    [{row[1]:5.1f}] {row[0][:36]} {name[:40]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--stats-only", action="store_true")
    args = parser.parse_args()

    log("=== Helix Phase 7D: Context Lifecycle ===")

    if args.stats_only:
        show_stats()
        return

    bootstrap_sessions(dry_run=args.dry_run, limit=args.limit)
    verify_context_endpoints()
    show_stats()
    log("=== Phase 7D Done ===")

if __name__ == "__main__":
    main()
