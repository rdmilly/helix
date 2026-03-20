"""Migrate Memory v0.5.1 conversation chunks to Helix Cortex."""
import sqlite3
import json
import time
import sys
import httpx

MEMORY_FTS = "/var/lib/docker/volumes/context-engine_ce-data/_data/conversations_fts.db"
HELIX = "http://127.0.0.1:9050"
BATCH_PAUSE = 0.3

def main():
    dry_run = "--dry-run" in sys.argv

    r = httpx.get(f"{HELIX}/health", timeout=10)
    print(f"Helix: {r.json().get('status')}")
    r = httpx.get(f"{HELIX}/api/v1/conversations/stats", timeout=10)
    pre = r.json()
    print(f"Pre-migration: {pre.get('fts_chunks', 0)} chunks, {pre.get('fts_sessions', 0)} sessions")

    db = sqlite3.connect(MEMORY_FTS, timeout=30)
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    cur.execute("""
        SELECT c0 as chunk_id, c1 as session_id, c2 as source,
               c3 as timestamp, c4 as topic_hint, c5 as content, c6 as meta_json
        FROM conversation_fts_content
        WHERE length(c5) > 50 AND c5 NOT LIKE '%Empty%'
        ORDER BY c3 ASC
    """)
    rows = cur.fetchall()
    db.close()
    print(f"Loaded {len(rows)} meaningful chunks")

    sessions = {}
    for r in rows:
        sid = r["session_id"]
        if sid not in sessions:
            sessions[sid] = {"chunks": [], "source": r["source"], "timestamp": r["timestamp"]}
        sessions[sid]["chunks"].append({
            "chunk_id": r["chunk_id"], "content": r["content"],
            "timestamp": r["timestamp"], "meta": r["meta_json"],
        })
        if r["timestamp"] and r["timestamp"] > sessions[sid]["timestamp"]:
            sessions[sid]["timestamp"] = r["timestamp"]

    print(f"Found {len(sessions)} sessions")

    if dry_run:
        total_chars = sum(sum(len(c["content"]) for c in s["chunks"]) for s in sessions.values())
        print(f"DRY RUN: {len(sessions)} sessions, {len(rows)} chunks, {total_chars:,} chars")
        return

    client = httpx.Client(timeout=60)
    migrated = 0
    errors = 0
    total_chunks_created = 0

    for i, (sid, data) in enumerate(sessions.items()):
        ordered = sorted(data["chunks"], key=lambda c: c["chunk_id"])
        full_text = "\n\n".join(c["content"] for c in ordered)
        meta = {}
        try:
            meta = json.loads(ordered[0].get("meta", "{}"))
        except Exception:
            pass

        try:
            resp = client.post(f"{HELIX}/api/v1/conversations/ingest", json={
                "text": full_text,
                "session_id": sid,
                "source": data["source"] or "memory-migration",
                "timestamp": data["timestamp"] or "",
                "metadata": {
                    "migrated_from": "memory-v0.5.1",
                    "original_chunks": len(ordered),
                    "name": meta.get("name", ""),
                    "model": meta.get("model", ""),
                },
                "scan_code": True,
            }, timeout=60)

            if resp.status_code == 200:
                result = resp.json()
                migrated += 1
                total_chunks_created += result.get("chunks", 0)
                if migrated % 50 == 0:
                    print(f"  Progress: {migrated}/{len(sessions)} ({total_chunks_created} chunks)")
            else:
                errors += 1
                if errors <= 5:
                    print(f"  Error {sid}: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Exception {sid}: {e}")

        if (i + 1) % 10 == 0:
            time.sleep(BATCH_PAUSE)

    client.close()

    try:
        r = httpx.get(f"{HELIX}/api/v1/conversations/stats", timeout=10)
        post = r.json()
    except Exception:
        post = {}

    print("=" * 60)
    print("MIGRATION COMPLETE")
    print(f"  Sessions migrated: {migrated}")
    print(f"  Errors:            {errors}")
    print(f"  Chunks created:    {total_chunks_created}")
    print(f"  Before: {pre.get('fts_chunks', 0)} chunks / {pre.get('fts_sessions', 0)} sessions")
    print(f"  After:  {post.get('fts_chunks', '?')} chunks / {post.get('fts_sessions', '?')} sessions")
    print("=" * 60)

if __name__ == "__main__":
    main()
