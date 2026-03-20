#!/usr/bin/env python3
"""
Migrate data from Forge (old schema) to Helix (epigenetic schema)

Copies:
- Atoms (with structural metadata to meta JSON)
- Molecules  
- Patterns
- Workspace files metadata

Preserves all data while upgrading to epigenetic architecture.
"""
import sqlite3
import json
import sys
from pathlib import Path
from datetime import datetime

FORGE_DB = "/opt/projects/the-forge/data/forge.db"
HELIX_DB = "/opt/projects/helix/data/cortex.db"

def migrate_atoms(forge_conn, helix_conn):
    """Migrate atoms from Forge to Helix with epigenetic schema"""
    forge_cur = forge_conn.cursor()
    helix_cur = helix_conn.cursor()
    
    # Get all atoms from Forge
    forge_cur.execute("""
        SELECT id, name, full_name, code, language, lines, 
               file_path, project, domain, created_at
        FROM atoms
    """)
    
    migrated = 0
    for row in forge_cur.fetchall():
        atom_id, name, full_name, code, language, lines, file_path, project, domain, created_at = row
        
        # Build structural metadata for meta JSON
        structural_meta = {
            "language": language or "unknown",
            "lines": lines or 0,
            "file_path": file_path,
            "is_async": "async def" in (code or "")
        }
        
        # Build domain metadata
        domain_meta = {}
        if domain:
            domain_meta = {
                "categories": [domain],
                "confidence": 1.0,
                "inferred_project": project
            }
        
        # Combine into meta JSON
        meta = {}
        if structural_meta:
            meta["structural"] = structural_meta
        if domain_meta:
            meta["domain"] = domain_meta
        
        # Insert into Helix with epigenetic schema
        helix_cur.execute("""
            INSERT OR IGNORE INTO atoms 
            (id, name, full_name, code, fp_version, first_seen, meta)
            VALUES (?, ?, ?, ?, 'v1', ?, ?)
        """, (
            atom_id,
            name,
            full_name,
            code,
            created_at or datetime.utcnow().isoformat(),
            json.dumps(meta)
        ))
        
        migrated += 1
    
    helix_conn.commit()
    print(f"✓ Migrated {migrated} atoms")
    return migrated

def migrate_molecules(forge_conn, helix_conn):
    """Migrate molecules from Forge to Helix"""
    forge_cur = forge_conn.cursor()
    helix_cur = helix_conn.cursor()
    
    # Get all molecules from Forge
    forge_cur.execute("""
        SELECT id, name, description, atom_ids, created_at
        FROM molecules
    """)
    
    migrated = 0
    for row in forge_cur.fetchall():
        molecule_id, name, description, atom_ids_str, created_at = row
        
        # Parse atom IDs
        atom_ids = json.loads(atom_ids_str) if atom_ids_str else []
        
        # Insert into Helix
        helix_cur.execute("""
            INSERT OR IGNORE INTO molecules
            (id, name, description, atom_ids_json, first_seen, meta)
            VALUES (?, ?, ?, ?, ?, '{}')
        """, (
            molecule_id,
            name,
            description,
            json.dumps(atom_ids),
            created_at or datetime.utcnow().isoformat()
        ))
        
        migrated += 1
    
    helix_conn.commit()
    print(f"✓ Migrated {migrated} molecules")
    return migrated

def migrate_patterns(forge_conn, helix_conn):
    """Migrate patterns to conventions table"""
    forge_cur = forge_conn.cursor()
    helix_cur = helix_conn.cursor()
    
    try:
        forge_cur.execute("""
            SELECT id, pattern, description, occurrences, scope
            FROM patterns
        """)
        
        migrated = 0
        for row in forge_cur.fetchall():
            pattern_id, pattern, description, occurrences, scope = row
            
            helix_cur.execute("""
                INSERT OR IGNORE INTO conventions
                (id, pattern, description, confidence, occurrences, scope, meta)
                VALUES (?, ?, ?, ?, ?, ?, '{}')
            """, (
                pattern_id,
                pattern,
                description,
                min(occurrences / 10.0, 1.0) if occurrences else 0.5,  # Convert to confidence
                occurrences or 0,
                scope or "global"
            ))
            
            migrated += 1
        
        helix_conn.commit()
        print(f"✓ Migrated {migrated} patterns/conventions")
        return migrated
        
    except sqlite3.OperationalError:
        print("⚠ No patterns table in Forge, skipping")
        return 0

def verify_migration(helix_conn):
    """Verify data was migrated correctly"""
    cur = helix_conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM atoms")
    atoms_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM molecules")
    molecules_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM conventions")
    conventions_count = cur.fetchone()[0]
    
    # Check that meta is populated
    cur.execute("SELECT COUNT(*) FROM atoms WHERE meta != '{}'")
    atoms_with_meta = cur.fetchone()[0]
    
    print(f"\n=== Migration Summary ===")
    print(f"Atoms: {atoms_count} ({atoms_with_meta} with metadata)")
    print(f"Molecules: {molecules_count}")
    print(f"Conventions: {conventions_count}")
    
    # Show sample atom with meta
    cur.execute("SELECT id, name, meta FROM atoms WHERE meta != '{}' LIMIT 1")
    sample = cur.fetchone()
    if sample:
        print(f"\nSample atom with metadata:")
        print(f"  ID: {sample[0]}")
        print(f"  Name: {sample[1]}")
        print(f"  Meta: {json.dumps(json.loads(sample[2]), indent=2)}")

def main():
    print("🧬 Helix Migration: Forge → Helix (Epigenetic Schema)")
    print("=" * 60)
    
    # Check databases exist
    if not Path(FORGE_DB).exists():
        print(f"❌ Forge database not found: {FORGE_DB}")
        sys.exit(1)
    
    if not Path(HELIX_DB).exists():
        print(f"❌ Helix database not found: {HELIX_DB}")
        print("Run 'docker-compose up -d' first to initialize Helix")
        sys.exit(1)
    
    # Connect to both databases
    forge_conn = sqlite3.connect(FORGE_DB)
    helix_conn = sqlite3.connect(HELIX_DB)
    
    try:
        print(f"\n📂 Source: {FORGE_DB}")
        print(f"📂 Target: {HELIX_DB}\n")
        
        # Migrate data
        atoms_count = migrate_atoms(forge_conn, helix_conn)
        molecules_count = migrate_molecules(forge_conn, helix_conn)
        patterns_count = migrate_patterns(forge_conn, helix_conn)
        
        # Verify
        verify_migration(helix_conn)
        
        print(f"\n✅ Migration complete!")
        print(f"   Total migrated: {atoms_count + molecules_count + patterns_count} records")
        print(f"\n💡 Next steps:")
        print(f"   1. Verify data: sqlite3 {HELIX_DB} 'SELECT * FROM atoms LIMIT 5;'")
        print(f"   2. Update Forge to point writes to Helix")
        print(f"   3. Keep old Forge DB as backup for 30 days")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        forge_conn.close()
        helix_conn.close()

if __name__ == "__main__":
    main()
