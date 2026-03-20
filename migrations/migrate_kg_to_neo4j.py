#!/usr/bin/env python3
"""
migrate_kg_to_neo4j.py - Phase 5
Migrate entities and kg_relationships from PostgreSQL into Neo4j.

Runs inside helix-cortex container (has neo4j driver + PG access).
"""
import os, sys, json, time
import psycopg2
import psycopg2.extras
from neo4j import GraphDatabase

DSN       = os.getenv('POSTGRES_DSN',
    'host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix')
NEO4J_URI  = os.getenv('NEO4J_URI',  'bolt://helix-neo4j:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASS = os.getenv('NEO4J_PASSWORD', '0613b6ff20972862e43798fbdced449e')
BATCH      = 100

def run():
    pg  = psycopg2.connect(DSN)
    pg.autocommit = True
    cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    driver.verify_connectivity()
    print(f'Connected: PG + Neo4j {NEO4J_URI}')

    # ---- Setup indexes ----
    with driver.session() as s:
        s.run('CREATE INDEX entity_id   IF NOT EXISTS FOR (e:Entity) ON (e.id)')
        s.run('CREATE INDEX entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name)')
        s.run('CREATE INDEX entity_type IF NOT EXISTS FOR (e:Entity) ON (e.entity_type)')
    print('Indexes created')

    # ---- 1. Migrate entities ----
    cur.execute("""
        SELECT id, name, entity_type, description,
               first_seen, last_seen,
               COALESCE(mention_count, 0) AS mention_count
        FROM entities
        WHERE name IS NOT NULL AND name != ''
        ORDER BY mention_count DESC NULLS LAST
    """)
    entities = cur.fetchall()
    print(f'\n[1/2] Migrating {len(entities)} entities...')

    ok = skip = 0
    for i in range(0, len(entities), BATCH):
        chunk = entities[i:i+BATCH]
        with driver.session() as s:
            s.run("""
                UNWIND $rows AS e
                MERGE (n:Entity {id: e.id})
                SET   n.name          = e.name,
                      n.entity_type   = e.entity_type,
                      n.description   = e.description,
                      n.first_seen    = e.first_seen,
                      n.last_seen     = e.last_seen,
                      n.mention_count = e.mention_count
            """, {"rows": [
                {
                    "id":            r['id'],
                    "name":          r['name'],
                    "entity_type":   r['entity_type'] or 'unknown',
                    "description":   r['description'] or '',
                    "first_seen":    str(r['first_seen'] or ''),
                    "last_seen":     str(r['last_seen'] or ''),
                    "mention_count": int(r['mention_count'] or 0),
                } for r in chunk
            ]})
        ok += len(chunk)
        print(f'  entities: {ok}/{len(entities)}')
    print(f'  Done: {ok} entities')

    # ---- 2. Migrate relationships ----
    cur.execute("""
        SELECT source_name, target_name, relation_type,
               description, session_id, created_at
        FROM kg_relationships
        WHERE source_name IS NOT NULL AND target_name IS NOT NULL
        ORDER BY created_at DESC NULLS LAST
    """)
    rels = cur.fetchall()
    print(f'\n[2/2] Migrating {len(rels)} relationships...')

    ok = 0
    for i in range(0, len(rels), BATCH):
        chunk = rels[i:i+BATCH]
        with driver.session() as s:
            s.run("""
                UNWIND $rows AS r
                MERGE (a:Entity {name: r.source_name})
                MERGE (b:Entity {name: r.target_name})
                MERGE (a)-[rel:RELATES_TO {relation_type: r.relation_type}]->(b)
                SET   rel.description = r.description,
                      rel.session_id  = r.session_id,
                      rel.created_at  = r.created_at
            """, {"rows": [
                {
                    "source_name":   r['source_name'],
                    "target_name":   r['target_name'],
                    "relation_type": r['relation_type'] or 'RELATED',
                    "description":   r['description'] or '',
                    "session_id":    r['session_id'] or '',
                    "created_at":    str(r['created_at'] or ''),
                } for r in chunk
            ]})
        ok += len(chunk)
        print(f'  relationships: {ok}/{len(rels)}')
    print(f'  Done: {ok} relationships')

    # ---- Verify ----
    with driver.session() as s:
        n_nodes = s.run('MATCH (e:Entity) RETURN count(e) AS n').single()['n']
        n_rels  = s.run('MATCH ()-[r:RELATES_TO]->() RETURN count(r) AS n').single()['n']
        sample  = s.run(
            'MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) '
            'RETURN a.name, r.relation_type, b.name LIMIT 5'
        ).data()

    print(f'\n=== Neo4j after migration ===')
    print(f'  Entity nodes:  {n_nodes}')
    print(f'  Relationships: {n_rels}')
    print(f'  Sample edges:')
    for row in sample:
        print(f'    ({row["a.name"]}) -[{row["r.relation_type"]}]-> ({row["b.name"]})')

    driver.close()
    pg.close()

if __name__ == '__main__':
    run()
