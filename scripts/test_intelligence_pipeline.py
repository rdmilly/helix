#!/usr/bin/env python3
"""
test_intelligence_pipeline.py - End-to-end integration test for the 9-tag intelligence pipeline.

Usage: docker exec helix-cortex python3 /app/scripts/test_intelligence_pipeline.py
"""
import asyncio
import json
import sqlite3
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, '/app')

from services.haiku import get_haiku_service
from services.database import get_db_path
from services.synapse import get_synapse_service

TEST_CONV_ID = f"test-intel-{str(uuid.uuid4())[:8]}"

TEST_TEXT = (
    "We decided to use ChromaDB for vector storage instead of pgvector because it has a simpler API "
    "and native collection management. We ruled out pgvector because it requires raw SQL for vector "
    "operations and we would need to manage embeddings manually. "
    "We are assuming that all conversation sessions come from a single user (Ryan). If multi-user "
    "support is needed later, the session_id schema will need to change. "
    "The intelligence collection in ChromaDB must always be initialized at startup before any writes "
    "attempt to use it. This is an invariant - writes silently drop if the collection does not exist. "
    "There is a risk that the Haiku API rate limit could cause intelligence extraction to fail silently "
    "for high-volume flush periods. No mitigation exists yet. "
    "The ext_ingest router is tightly coupled to the chromadb service - if chromadb is down, "
    "intelligence vectors are silently dropped with only a warning log. "
    "We accepted a tradeoff: per-item ChromaDB writes instead of batch writes, "
    "because the ChromaDB client does not support batch upsert in this version. "
    "A useful pattern: always syntax-check Python files with ast.parse before triggering a "
    "container rebuild. Catches escaping errors immediately."
)

PASS = 0
FAIL = 0

def ok(msg):
    global PASS
    PASS += 1
    print(f"  OK: {msg}")

def warn(msg):
    print(f"  WARN: {msg}")

def fail(msg):
    global FAIL
    FAIL += 1
    print(f"  FAIL: {msg}")


async def test_extract_intelligence():
    print("\n=== TEST 1: extract_intelligence() ===")
    haiku = get_haiku_service()
    items = await haiku.extract_intelligence(TEST_TEXT)
    print(f"  Extracted {len(items)} items:")
    by_tag = {}
    for item in items:
        tag = item.get('tag', '?')
        by_tag.setdefault(tag, []).append(item)
        print(f"    [{tag}] ({item.get('component','')}) {item.get('content','')[:80]}")
    if len(items) > 0:
        ok(f"extract_intelligence returned {len(items)} items")
    else:
        fail("extract_intelligence returned 0 items - check Haiku API key")
    expected = {'DECISION', 'ASSUMPTION', 'INVARIANT', 'RISK', 'COUPLING', 'TRADEOFF', 'REJECTED', 'PATTERN'}
    found = set(by_tag.keys())
    missing = expected - found
    if missing:
        warn(f"some expected tags not found: {missing}")
    else:
        ok("all 8 expected tag types present")
    return items


async def test_db_routing(items):
    print("\n=== TEST 2: DB table routing ===")
    db_path = get_db_path()
    now = datetime.now(timezone.utc).isoformat()
    by_tag = defaultdict(list)
    for item in items:
        tag = item.get('tag', '')
        if tag:
            by_tag[tag].append(item)

    with sqlite3.connect(db_path) as conn:
        # DECISION -> decisions
        before = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        for item in by_tag.get('DECISION', []):
            conn.execute(
                "INSERT INTO decisions (id, session_id, decision, rationale, project, created_at, meta) VALUES (?,?,?,?,?,?,?)",
                (str(uuid.uuid4())[:12], TEST_CONV_ID, item['content'], item.get('context',''), item.get('component','test'), now, '{}')
            )
        added = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0] - before
        ok(f"decisions += {added}") if added > 0 else warn(f"decisions += 0 (no DECISION items extracted)")

        # RISK -> anomalies
        before = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
        for item in by_tag.get('RISK', []):
            confidence = float(item.get('confidence', 0.7))
            severity = 'high' if confidence >= 0.85 else 'medium'
            conn.execute(
                "INSERT INTO anomalies (id, type, description, evidence, severity, state, session_id, created_at, meta) VALUES (?,?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4())[:12], 'risk', item['content'], item.get('context',''), severity, 'open', TEST_CONV_ID, now, '{}')
            )
        added = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0] - before
        ok(f"anomalies += {added}") if added > 0 else warn("anomalies += 0 (no RISK items extracted)")

        # PATTERN -> conventions
        before = conn.execute("SELECT COUNT(*) FROM conventions").fetchone()[0]
        for item in by_tag.get('PATTERN', []):
            conn.execute(
                "INSERT INTO conventions (id, pattern, description, confidence, occurrences, scope, first_seen, meta) VALUES (?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4())[:12], item['content'], item.get('context',''), float(item.get('confidence',0.7)), 1, item.get('component','general'), now, '{}')
            )
        added = conn.execute("SELECT COUNT(*) FROM conventions").fetchone()[0] - before
        ok(f"conventions += {added}") if added > 0 else warn("conventions += 0 (no PATTERN items extracted)")

        # COUPLING + INVARIANT -> kg_relationships
        before = conn.execute("SELECT COUNT(*) FROM kg_relationships").fetchone()[0]
        for tag in ('COUPLING', 'INVARIANT'):
            for item in by_tag.get(tag, []):
                conn.execute(
                    "INSERT INTO kg_relationships (source_name, target_name, relation_type, description, created_at, session_id) VALUES (?,?,?,?,?,?)",
                    (item.get('component','unknown'), item['content'][:80], tag, item['content'], now, TEST_CONV_ID)
                )
        added = conn.execute("SELECT COUNT(*) FROM kg_relationships").fetchone()[0] - before
        ok(f"kg_relationships += {added}") if added > 0 else warn("kg_relationships += 0 (no COUPLING/INVARIANT items)")

        conn.commit()


async def test_chromadb_collection():
    print("\n=== TEST 3: ChromaDB intelligence collection ===")
    from services.chromadb import get_chromadb_service
    chroma = get_chromadb_service()
    if not chroma._initialized:
        await chroma.initialize()
    intel_id = chroma._collection_ids.get('intelligence')
    if intel_id:
        ok(f"intelligence collection initialized: {intel_id[:16]}...")
    else:
        fail("intelligence collection NOT in _collection_ids - vectors will silently drop")


async def test_synapse_stats():
    print("\n=== TEST 4: synapse stats and injection ===")
    synapse = get_synapse_service()
    ctx = await synapse.assemble_context("ChromaDB collection intelligence extraction design memory")
    stats = ctx.get('stats', {})
    print(f"  Stats: {stats}")
    if 'intelligence_items_found' in stats:
        ok(f"intelligence_items_found in stats = {stats['intelligence_items_found']}")
    else:
        fail("intelligence_items_found missing from stats")
    injection = ctx.get('injection_text', '')
    if '## Design Memory' in injection:
        ok("Design Memory block present in injection text")
        lines = [l for l in injection.split('\n') if l.startswith('- [')]
        for line in lines[:5]:
            print(f"    {line}")
    else:
        warn("No Design Memory block (vectors not yet populated - run a flush first)")


async def cleanup():
    print("\n=== CLEANUP ===")
    db_path = get_db_path()
    with sqlite3.connect(db_path) as conn:
        for table, col in [('decisions','session_id'),('anomalies','session_id'),('kg_relationships','session_id')]:
            count = conn.execute(f"DELETE FROM {table} WHERE {col}=?", (TEST_CONV_ID,)).rowcount
            if count:
                print(f"  Removed {count} test rows from {table}")
        conn.commit()


async def main():
    print(f"Intelligence Pipeline Integration Test")
    print(f"Session: {TEST_CONV_ID}")
    print("=" * 50)
    try:
        items = await test_extract_intelligence()
        if items:
            await test_db_routing(items)
        await test_chromadb_collection()
        await test_synapse_stats()
    finally:
        await cleanup()
    print(f"\n=== RESULT: {PASS} passed, {FAIL} failed ===")
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == '__main__':
    asyncio.run(main())
