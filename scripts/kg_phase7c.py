#!/usr/bin/env python3
"""
Helix Phase 7C — Knowledge Graph Bootstrap
==========================================
1. Migrates Memory KG entities/relationships/mentions -> Helix API
2. Pattern-extracts new entities from Helix FTS chunks

Usage:
  python3 /opt/projects/helix/scripts/kg_phase7c.py [--skip-migrate] [--skip-extract] [--dry-run]
"""
import re
import sys
import json
import time
import sqlite3
import argparse
import requests
from collections import defaultdict
from datetime import datetime, timezone

HELIX_URL   = "http://127.0.0.1:9050"
MEMORY_KG   = "/var/lib/docker/volumes/memory_memory-data/_data/knowledge_graph.db"
HELIX_FTS   = "/opt/projects/helix/data/conversations_fts.db"

PATTERNS = {
    "docker_container": [
        r'\bcontainer[s]?\s+([a-z][a-z0-9_-]{2,40})\b',
        r'\bdocker\s+(?:exec|start|stop|restart|logs|inspect)\s+([a-z][a-z0-9_-]{2,40})\b',
        r'\bcontainer[_\s]name["\s:=]+([a-z][a-z0-9_-]{2,40})\b',
    ],
    "domain": [
        r'\b([a-z0-9][a-z0-9-]{1,40}\.millyweb\.com)\b',
    ],
    "project": [
        r'/opt/projects/([a-z][a-z0-9_-]{1,40})(?:/|$)',
        r'\b(helix(?:-cortex)?|memory(?:-dev)?|mcp-provision-filter|mcp-gateway|forge|transcript-bridge|gnome|postiz|clientflow|n8n|printblocks)\b',
    ],
    "mcp_server": [
        r'\bmcp[_-]([a-z][a-z0-9_-]{2,40})\b',
        r'"server":\s*"([a-z][a-z0-9_-]{2,40})"',
    ],
    "service": [
        r'\b(traefik|postgres(?:ql)?|redis|chromadb|qdrant|minio|infisical|n8n|postiz|uptime.?kuma|netdata|caddy|nginx|fastapi|uvicorn|fastmcp|sqlite3?)\b',
    ],
    "person": [
        r'\b(Ryan|Ashley|Legend)\b',
    ],
    "port": [
        r'port\s+(\d{4,5})\b',
        r':(\d{4,5})(?:/|\b)',
    ],
}

SKIP = {
    "the","and","for","with","from","that","this","have","will",
    "docker","python3","python","bash","sh","cat","ls","curl",
    "true","false","null","none","new","old","get","set","use",
    "all","some","any","one","just","also","then","when",
    "app","api","dev","prod","data","log","run","bin","etc",
    "test","tmp","var","opt","lib","usr","home","root","mnt",
    "8080","8000","8443","80","443","22","3000",
}

def _now(): return datetime.now(timezone.utc).isoformat()
def log(m): print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)

def migrate_memory_kg(dry_run=False):
    log("=== Phase 1: Memory KG Migration ===")
    try:
        conn = sqlite3.connect(MEMORY_KG, timeout=10)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        log(f"  ERROR: {e}"); return 0, 0

    entities = conn.execute(
        "SELECT id, name, entity_type, description, attributes, first_seen, last_seen, mention_count FROM entities"
    ).fetchall()
    # Join to get names
    relationships = conn.execute("""
        SELECT e1.name as source, e2.name as target, r.relation_type, r.description, r.session_id, r.created_at
        FROM relationships r
        JOIN entities e1 ON r.source_id = e1.id
        JOIN entities e2 ON r.target_id = e2.id
    """).fetchall()
    mentions = conn.execute(
        "SELECT e.name as entity_name, m.session_id, m.context, m.mentioned_at FROM mentions m JOIN entities e ON m.entity_id = e.id"
    ).fetchall()
    conn.close()

    log(f"  Source: {len(entities)} entities, {len(relationships)} rels, {len(mentions)} mentions")
    if dry_run:
        log("  DRY RUN"); return len(entities), len(relationships)

    entity_list = []
    for e in entities:
        try: attrs = json.loads(e["attributes"]) if e["attributes"] else {}
        except: attrs = {}
        entity_list.append({
            "name": e["name"], "entity_type": e["entity_type"],
            "description": e["description"] or "", "attributes": attrs,
            "first_seen": e["first_seen"], "last_seen": e["last_seen"],
            "mention_count": e["mention_count"] or 0,
        })

    rel_list = [{
        "source_name": r["source"], "target_name": r["target"],
        "relation_type": r["relation_type"], "description": r["description"] or "",
        "session_id": r["session_id"] or "", "created_at": r["created_at"] or _now()
    } for r in relationships]

    mention_list = [{
        "entity_name": m["entity_name"], "session_id": m["session_id"] or "",
        "context": m["context"] or "", "mentioned_at": m["mentioned_at"] or _now()
    } for m in mentions]

    e_ok = r_ok = m_ok = 0
    BATCH = 200

    # Entities first
    for i in range(0, len(entity_list), BATCH):
        chunk = entity_list[i:i+BATCH]
        try:
            resp = requests.post(f"{HELIX_URL}/api/v1/knowledge/migrate",
                json={"entities": chunk, "relationships": [], "mentions": []}, timeout=30)
            resp.raise_for_status()
            e_ok += resp.json().get("entities", 0)
        except Exception as ex:
            log(f"  Entity batch {i//BATCH+1} ERR: {ex}")

    # Relationships
    for i in range(0, len(rel_list), BATCH):
        chunk = rel_list[i:i+BATCH]
        try:
            resp = requests.post(f"{HELIX_URL}/api/v1/knowledge/migrate",
                json={"entities": [], "relationships": chunk, "mentions": []}, timeout=30)
            resp.raise_for_status()
            r_ok += resp.json().get("relationships", 0)
        except Exception as ex:
            log(f"  Rel batch {i//BATCH+1} ERR: {ex}")

    # Mentions in batches (large dataset)
    for i in range(0, len(mention_list), BATCH):
        chunk = mention_list[i:i+BATCH]
        try:
            resp = requests.post(f"{HELIX_URL}/api/v1/knowledge/migrate",
                json={"entities": [], "relationships": [], "mentions": chunk}, timeout=30)
            resp.raise_for_status()
            m_ok += resp.json().get("mentions", 0)
        except Exception as ex:
            if i < 5: log(f"  Mention batch {i//BATCH+1} ERR: {ex}")
        if i % 2000 == 0 and i > 0:
            log(f"  mentions progress: {m_ok}/{len(mention_list)}")

    log(f"  Migrated: {e_ok}e {r_ok}r {m_ok}m")
    return e_ok, r_ok


def extract_entities_from_fts(dry_run=False):
    log("=== Phase 2: Pattern Extraction from FTS ===")
    try:
        conn = sqlite3.connect(HELIX_FTS, timeout=10)
        rows = conn.execute("SELECT c5 FROM conversation_fts_content").fetchall()
        conn.close()
    except Exception as e:
        log(f"  ERROR: {e}"); return 0

    log(f"  {len(rows)} chunks")
    compiled = {et: [re.compile(p, re.IGNORECASE) for p in plist] for et, plist in PATTERNS.items()}

    counts = defaultdict(lambda: defaultdict(int))
    for (text,) in rows:
        if not text: continue
        for etype, pats in compiled.items():
            for pat in pats:
                for m in pat.finditer(text):
                    raw = m.group(1).strip().rstrip('/')
                    if not raw or len(raw) < 3: continue
                    name = f"port:{raw}" if etype == "port" else raw
                    if name.lower() in SKIP: continue
                    counts[name][etype] += 1

    to_upsert = []
    for name, tc in counts.items():
        total = sum(tc.values())
        if total < 2: continue
        best_type = max(tc, key=tc.get)
        to_upsert.append({"name": name, "entity_type": best_type,
                          "description": "", "mention_count": total})

    to_upsert.sort(key=lambda x: x["mention_count"], reverse=True)
    log(f"  {len(to_upsert)} candidates (>=2 mentions)")

    if dry_run:
        for e in to_upsert[:20]:
            log(f"    [{e['entity_type']:20}] {e['name']:40} x{e['mention_count']}")
        return len(to_upsert)

    ok = 0
    for i in range(0, len(to_upsert), 100):
        chunk = to_upsert[i:i+100]
        try:
            resp = requests.post(f"{HELIX_URL}/api/v1/knowledge/migrate",
                json={"entities": chunk, "relationships": [], "mentions": []}, timeout=30)
            resp.raise_for_status()
            ok += resp.json().get("entities", 0)
        except Exception as ex:
            log(f"  Extract batch {i//100+1} ERR: {ex}")
        time.sleep(0.02)

    log(f"  Upserted {ok} entities from extraction")
    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-migrate", action="store_true")
    parser.add_argument("--skip-extract", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log("=== Helix Phase 7C: Knowledge Graph Bootstrap ===")
    try:
        s = requests.get(f"{HELIX_URL}/api/v1/knowledge/stats", timeout=10).json()
        log(f"  Before: {s['entities']}e {s['relationships']}r {s['mentions']}m")
    except Exception as e:
        log(f"  Can't reach Helix: {e}"); sys.exit(1)

    if not args.skip_migrate:
        migrate_memory_kg(dry_run=args.dry_run)
    if not args.skip_extract:
        extract_entities_from_fts(dry_run=args.dry_run)

    if not args.dry_run:
        try:
            s = requests.get(f"{HELIX_URL}/api/v1/knowledge/stats", timeout=10).json()
            log(f"\n=== Final ===")
            log(f"  Entities: {s['entities']}")
            log(f"  Relationships: {s['relationships']}")
            log(f"  Mentions: {s['mentions']}")
            log(f"  By type: {s.get('by_type', {})}")
            log(f"  Top: {[e['name'] for e in s.get('top_entities', [])[:8]]}")
        except: pass

    log("=== Phase 7C Done ===")

if __name__ == "__main__":
    main()
