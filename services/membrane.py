"""Membrane Service - Forge<>Helix Bridge

The Membrane is the semi-permeable interface between the external world
(Forge pattern catalog) and Helix internal DNA store. Handles:
1. Bulk import: Pull all atoms from Forge catalog API into Helix
2. Incremental sync: Poll Forge for atoms newer than last sync
3. Webhook intake: Accept push notifications from Forge on new scans
4. Field mapping: Transform Forge atom schema to Helix atom schema
"""
import hashlib
import json
from services import pg_sync
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import httpx

from config import CURRENT_FP_VERSION
from services.database import get_db
from services.chromadb import get_chromadb_service
from services.embeddings import get_embedding_service

logger = logging.getLogger(__name__)

FORGE_BASE_URL = "http://localhost:9095"
FORGE_CATALOG_ATOMS = f"{FORGE_BASE_URL}/api/forge/catalog/atoms"
FORGE_CATALOG_MOLECULES = f"{FORGE_BASE_URL}/api/forge/catalog/molecules"
FORGE_CATALOG_STATS = f"{FORGE_BASE_URL}/api/forge/catalog/stats"
BATCH_SIZE = 50


class MembraneService:
    """Forge<>Helix bridge - the cell membrane"""

    def __init__(self):
        self.db = get_db()
        self._http_client: Optional[httpx.AsyncClient] = None
        self._import_stats = {
            "total_imported": 0, "total_skipped": 0,
            "total_updated": 0, "total_errors": 0,
            "last_import": None,
        }

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def close(self):
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()

    async def forge_health(self) -> Dict[str, Any]:
        try:
            client = await self._get_client()
            resp = await client.get(f"{FORGE_BASE_URL}/health")
            if resp.status_code == 200:
                return {"healthy": True, **resp.json()}
            return {"healthy": False, "status_code": resp.status_code}
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    async def fetch_forge_atoms(self, limit: int = BATCH_SIZE, offset: int = 0) -> Tuple[List[Dict], int]:
        client = await self._get_client()
        resp = await client.get(FORGE_CATALOG_ATOMS, params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        data = resp.json()
        return data.get("atoms", []), data.get("total", 0)

    def _map_forge_atom(self, forge_atom: Dict[str, Any]) -> Dict[str, Any]:
        fp = forge_atom.get("fingerprint", "")
        code = forge_atom.get("code", "")
        language = forge_atom.get("language", "unknown")
        section = forge_atom.get("section", "unknown")
        token_count = forge_atom.get("token_count", 0)
        projects = forge_atom.get("projects", [])
        name = forge_atom.get("name") or self._infer_name(code, section)

        structural_fp = hashlib.sha256(f"struct:{language}:{code.strip().lower()}".encode()).hexdigest()[:12]
        semantic_fp = hashlib.sha256(f"semantic:{section}:{code}".encode()).hexdigest()[:12]

        if token_count <= 10: size_bucket = "xs"
        elif token_count <= 50: size_bucket = "sm"
        elif token_count <= 200: size_bucket = "md"
        elif token_count <= 500: size_bucket = "lg"
        else: size_bucket = "xl"

        category_map = {"imports": "import", "tools": "function", "models": "model",
                        "config": "config", "infrastructure": "infrastructure", "routing": "routing"}
        category = category_map.get(section, section)

        meta = {
            "structural": {
                "language": language, "is_async": "async " in code,
                "is_method": "self" in code.split("\n")[0] if code else False,
                "parent_class": None, "param_count": 0,
                "has_return_type": "->" in code.split("\n")[0] if code else False,
                "has_docstring": '"""' in code or "'''" in code,
                "line_count": code.count("\n") + 1,
                "token_estimate": token_count, "size_bucket": size_bucket,
                "decorator_count": code.count("@") if section == "tools" else 0,
            },
            "semantic": {"tags": [section, language], "category": category},
            "provenance": {
                "projects": projects, "files": [],
                "first_seen_project": projects[0] if projects else "forge",
                "source": "forge_import",
            },
            "forge": {"fingerprint": fp, "section": section, "occurrences": forge_atom.get("occurrences", 1)},
        }

        return {
            "id": f"atom_{fp}", "name": name, "full_name": name,
            "code": code, "template": code, "parameters_json": "{}",
            "structural_fp": structural_fp, "semantic_fp": semantic_fp,
            "fp_version": CURRENT_FP_VERSION,
            "first_seen": forge_atom.get("first_seen", datetime.now(timezone.utc).isoformat()),
            "last_seen": forge_atom.get("last_seen", datetime.now(timezone.utc).isoformat()),
            "occurrence_count": forge_atom.get("occurrences", 1),
            "meta": json.dumps(meta),
        }

    def _infer_name(self, code: str, section: str) -> str:
        if not code: return "unknown"
        lines = code.strip().split("\n")
        first_line = lines[0].strip()
        if first_line.startswith("def ") or first_line.startswith("async def "):
            return first_line.split("(")[0].replace("def ", "").replace("async ", "").strip()
        if first_line.startswith("class "):
            return first_line.split("(")[0].split(":")[0].replace("class ", "").strip()
        if first_line.startswith("import ") or first_line.startswith("from "):
            return first_line[:60]
        if "=" in first_line and section == "config":
            return first_line.split("=")[0].strip()[:40]
        return first_line[:40] if first_line else "unknown"

    def _upsert_atom(self, atom: Dict[str, Any]) -> str:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, occurrence_count FROM atoms WHERE id = ?", (atom["id"],))
            existing = cursor.fetchone()
            if existing:
                new_count = max(existing[1], atom["occurrence_count"])
                cursor.execute(
                    "UPDATE atoms SET occurrence_count=?, last_seen=?, meta=? WHERE id=?",
                    (new_count, atom["last_seen"], atom["meta"], atom["id"]),
                )
                conn.commit()
                return "updated"
            else:
                cursor.execute(
                    """INSERT INTO atoms (id, name, full_name, code, template, parameters_json,
                     structural_fp, semantic_fp, fp_version, first_seen, last_seen, occurrence_count, meta)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (atom["id"], atom["name"], atom["full_name"], atom["code"], atom["template"],
                     atom["parameters_json"], atom["structural_fp"], atom["semantic_fp"],
                     atom["fp_version"], atom["first_seen"], atom["last_seen"],
                     atom["occurrence_count"], atom["meta"]),
                )
                conn.commit()
                return "inserted"

    async def bulk_import_atoms(self, dry_run: bool = False) -> Dict[str, Any]:
        stats = {
            "imported": 0, "updated": 0, "skipped": 0, "errors": 0,
            "total_forge": 0, "pages": 0, "dry_run": dry_run,
            "started": datetime.now(timezone.utc).isoformat(), "errors_detail": [],
        }
        health = await self.forge_health()
        if not health.get("healthy"):
            return {"error": "Forge unreachable", "detail": health}

        _, total = await self.fetch_forge_atoms(limit=1, offset=0)
        stats["total_forge"] = total
        logger.info(f"Membrane: Starting bulk import of {total} Forge atoms (dry_run={dry_run})")

        offset = 0
        chromadb = get_chromadb_service()
        embed_svc = get_embedding_service()

        while offset < total:
            try:
                atoms, _ = await self.fetch_forge_atoms(limit=BATCH_SIZE, offset=offset)
                stats["pages"] += 1
                for forge_atom in atoms:
                    try:
                        helix_atom = self._map_forge_atom(forge_atom)
                        if dry_run:
                            stats["imported"] += 1
                            continue
                        result = self._upsert_atom(helix_atom)
                        if result == "inserted": stats["imported"] += 1
                        elif result == "updated": stats["updated"] += 1
                        else: stats["skipped"] += 1

                        # Index in ChromaDB (best-effort)
                        try:
                            embedding = await embed_svc.embed(helix_atom["code"])
                            if embedding:
                                meta_parsed = pg_sync.dejson(helix_atom["meta"])
                                await chromadb.upsert_atom(
                                    atom_id=helix_atom["id"], code=helix_atom["code"],
                                    meta={"name": helix_atom["name"],
                                          "language": meta_parsed.get("structural", {}).get("language", "unknown"),
                                          "category": meta_parsed.get("semantic", {}).get("category", "unknown")},
                                    embedding=embedding,
                                )
                        except Exception as e:
                            logger.warning(f"ChromaDB index failed for {helix_atom['id']}: {e}")
                    except Exception as e:
                        stats["errors"] += 1
                        if len(stats["errors_detail"]) < 10:
                            stats["errors_detail"].append(f"{forge_atom.get('fingerprint','?')}: {str(e)[:100]}")
                        logger.error(f"Membrane import error: {e}")
                offset += BATCH_SIZE
                logger.info(f"Membrane: Page {stats['pages']} — {offset}/{total}")
            except Exception as e:
                stats["errors"] += 1
                stats["errors_detail"].append(f"Page error at offset {offset}: {e}")
                logger.error(f"Membrane page fetch error: {e}")
                break

        stats["finished"] = datetime.now(timezone.utc).isoformat()
        stats["duration_seconds"] = round(
            (datetime.fromisoformat(stats["finished"]) - datetime.fromisoformat(stats["started"])).total_seconds(), 1
        )
        self._import_stats["total_imported"] += stats["imported"]
        self._import_stats["total_updated"] += stats["updated"]
        self._import_stats["total_errors"] += stats["errors"]
        self._import_stats["last_import"] = stats["finished"]
        logger.info(f"Membrane: Bulk import done — {stats['imported']} new, {stats['updated']} updated, {stats['errors']} errors in {stats['duration_seconds']}s")
        return stats

    async def incremental_sync(self) -> Dict[str, Any]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(last_seen) FROM atoms")
            row = cursor.fetchone()
            last_sync = row[0] if row and row[0] else "2020-01-01T00:00:00"
        stats = {"new": 0, "updated": 0, "checked": 0, "errors": 0}
        offset = 0
        total = 1
        while offset < total:
            atoms, total = await self.fetch_forge_atoms(limit=BATCH_SIZE, offset=offset)
            stats["checked"] += len(atoms)
            for fa in atoms:
                if fa.get("last_seen", "") > last_sync:
                    try:
                        result = self._upsert_atom(self._map_forge_atom(fa))
                        if result == "inserted": stats["new"] += 1
                        elif result == "updated": stats["updated"] += 1
                    except Exception as e:
                        stats["errors"] += 1
            offset += BATCH_SIZE
        return stats

    async def handle_forge_webhook(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        event = payload.get("event", "unknown")
        if event != "scan_complete":
            return {"accepted": False, "reason": f"Unknown event: {event}"}
        stats = {"imported": 0, "updated": 0, "errors": 0}
        for fa in payload.get("new_atoms", []) + payload.get("updated_atoms", []):
            try:
                result = self._upsert_atom(self._map_forge_atom(fa))
                if result == "inserted": stats["imported"] += 1
                elif result == "updated": stats["updated"] += 1
            except Exception as e:
                stats["errors"] += 1
        return {"accepted": True, "project": payload.get("project"), **stats}

    def get_stats(self) -> Dict[str, Any]:
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM atoms")
            atom_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM atoms WHERE json_extract(meta, '$.provenance.source') = 'forge_import'")
            forge_count = cursor.fetchone()[0]
        return {**self._import_stats, "helix_atoms_total": atom_count, "helix_atoms_from_forge": forge_count}


_membrane_service: Optional[MembraneService] = None

def get_membrane_service() -> MembraneService:
    global _membrane_service
    if _membrane_service is None:
        _membrane_service = MembraneService()
    return _membrane_service
