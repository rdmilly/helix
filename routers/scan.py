"""Scan Router — Universal Code Intake

Three hooks for the Cortex scanner:

Hook 3 (on-demand):
  POST /api/v1/scan/source    — Scan raw source code
  POST /api/v1/scan/file      — Scan a file by VPS path
  POST /api/v1/scan/repo      — Scan all files in a project directory

Hook 2 (file writes):
  POST /api/v1/scan/webhook   — Called by Forge/infra-watcher on file writes

Hook 1 (transcripts):
  POST /api/v1/scan/transcript — Accept transcript, extract code blocks, scan them

All routes feed into the same scanner service — one brain, many ears.
"""
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.scanner import get_scanner_service
from services.database import get_db
from services.chromadb import get_chromadb_service
# embeddings handled internally by chromadb.add_document

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/scan", tags=["Scan"])

# Supported languages and extensions
SUPPORTED_EXTENSIONS = {
    ".py": "python",
}
# Future: ".ts": "typescript", ".js": "javascript", ".go": "go"

IGNORE_DIRS = {
    "__pycache__", ".git", "node_modules", ".venv", "venv",
    ".mypy_cache", ".pytest_cache", "dist", "build", ".egg-info",
}

IGNORE_FILES = {
    "__init__.py", "setup.py", "conftest.py",
}

MAX_FILE_SIZE = 500_000  # 500KB — skip huge generated files


# === Request Models ===

class ScanSourceRequest(BaseModel):
    """Scan raw source code."""
    code: str = Field(..., description="Raw source code to scan")
    language: str = Field("python", description="Language of the code")
    filepath: str = Field("<manual>", description="Virtual filepath for provenance tracking")
    project: str = Field("", description="Project name for provenance")
    enrich: bool = Field(False, description="Also run Haiku concept enrichment (costs tokens)")
    embed: bool = Field(True, description="Embed atoms into ChromaDB after scanning")


class ScanFileRequest(BaseModel):
    """Scan a file by path on the VPS filesystem."""
    path: str = Field(..., description="Absolute path to file on VPS")
    enrich: bool = Field(False, description="Run Haiku concept enrichment")
    embed: bool = Field(True, description="Embed atoms into ChromaDB")


class ScanRepoRequest(BaseModel):
    """Scan all files in a project directory."""
    path: str = Field(..., description="Absolute path to project root")
    project: str = Field("", description="Project name override (auto-detected from path if empty)")
    enrich: bool = Field(False, description="Run Haiku concept enrichment")
    embed: bool = Field(True, description="Embed atoms into ChromaDB")
    extensions: List[str] = Field(default_factory=lambda: [".py"], description="File extensions to scan")
    max_files: int = Field(200, description="Max files to scan (safety limit)")


class WebhookRequest(BaseModel):
    """Webhook payload from Forge or infra-watcher."""
    path: str = Field(..., description="File path that changed")
    content: Optional[str] = Field(None, description="File content (if available, avoids re-read)")
    project: str = Field("", description="Project name")
    event: str = Field("write", description="Event type: write, create, modify")
    source: str = Field("unknown", description="Who sent this: forge, infra-watcher, manual")


class TranscriptRequest(BaseModel):
    """Accept a conversation transcript containing code blocks."""
    transcript: str = Field("", description="Raw transcript text")
    messages: Optional[List[Dict[str, Any]]] = Field(None, description="Structured messages array")
    conversation_id: str = Field("", description="Conversation ID for provenance")
    session_id: str = Field("", description="Session ID for provenance")


# === Helper Functions ===

def _extract_code_blocks(text: str) -> List[Dict[str, str]]:
    """Extract fenced code blocks from text.
    
    Returns list of {language, code, char_count}.
    """
    pattern = r'```(\w*)\n(.*?)```'
    blocks = []
    for match in re.finditer(pattern, text, re.DOTALL):
        lang = match.group(1).lower() or "unknown"
        code = match.group(2).strip()
        if len(code) > 50:  # Skip trivial snippets
            blocks.append({
                "language": lang,
                "code": code,
                "char_count": len(code),
            })
    return blocks


def _discover_files(root: str, extensions: List[str], max_files: int) -> List[str]:
    """Walk a directory tree and find scannable files."""
    files = []
    root_path = Path(root)
    
    for dirpath, dirnames, filenames in os.walk(root_path):
        # Prune ignored directories
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]
        
        rel = Path(dirpath).relative_to(root_path)
        
        for filename in sorted(filenames):
            if filename in IGNORE_FILES:
                continue
            
            filepath = Path(dirpath) / filename
            
            if filepath.suffix not in extensions:
                continue
            
            if filepath.stat().st_size > MAX_FILE_SIZE:
                logger.info(f"Skipping large file: {filepath} ({filepath.stat().st_size} bytes)")
                continue
            
            files.append(str(filepath))
            
            if len(files) >= max_files:
                logger.warning(f"Hit max_files limit ({max_files}) scanning {root}")
                return files
    
    return files


async def _embed_atoms(atoms: List[Dict[str, Any]]):
    """Embed new atoms into ChromaDB via add_document."""
    chromadb = get_chromadb_service()
    
    if not chromadb._initialized:
        logger.warning("ChromaDB not ready, skipping embed")
        return 0
    
    embedded = 0
    for atom in atoms:
        if atom.get("action") != "created":
            continue
        
        # Build text for embedding — name + category + tags
        text = f"{atom.get('name', '')} {atom.get('category', '')} {' '.join(atom.get('semantic_tags', []))}"
        
        try:
            success = await chromadb.add_document(
                collection_base="atoms",
                doc_id=atom["id"],
                text=text,
                metadata={
                    "category": atom.get("category", ""),
                    "semantic_tags": ",".join(atom.get("semantic_tags", [])),
                    "structural_fp": atom.get("structural_fp", ""),
                    "line_count": atom.get("line_count", 0),
                }
            )
            if success:
                embedded += 1
        except Exception as e:
            logger.error(f"Failed to embed atom {atom.get('id')}: {e}")
    
    return embedded


# === Hook 3: On-Demand Scan ===

@router.post("/source")
async def scan_source(req: ScanSourceRequest):
    """Scan raw source code. Direct, synchronous scan."""
    scanner = get_scanner_service()
    atoms = await scanner.extract_atoms(req.code, req.language, req.filepath)
    
    embedded = 0
    if req.embed and atoms:
        embedded = await _embed_atoms(atoms)
    
    created = [a for a in atoms if a.get("action") == "created"]
    updated = [a for a in atoms if a.get("action") == "updated"]
    
    return {
        "status": "scanned",
        "filepath": req.filepath,
        "language": req.language,
        "atoms_created": len(created),
        "atoms_updated": len(updated),
        "atoms_embedded": embedded,
        "atoms": atoms,
    }


@router.post("/file")
async def scan_file(req: ScanFileRequest):
    """Scan a file by its VPS filesystem path."""
    filepath = Path(req.path)
    
    if not filepath.exists():
        raise HTTPException(404, f"File not found: {req.path}")
    
    if filepath.suffix not in SUPPORTED_EXTENSIONS:
        raise HTTPException(400, f"Unsupported file type: {filepath.suffix}. Supported: {list(SUPPORTED_EXTENSIONS.keys())}")
    
    if filepath.stat().st_size > MAX_FILE_SIZE:
        raise HTTPException(400, f"File too large: {filepath.stat().st_size} bytes (max {MAX_FILE_SIZE})")
    
    language = SUPPORTED_EXTENSIONS[filepath.suffix]
    code = filepath.read_text(encoding="utf-8", errors="replace")
    
    scanner = get_scanner_service()
    atoms = await scanner.extract_atoms(code, language, str(filepath))
    
    embedded = 0
    if req.embed and atoms:
        embedded = await _embed_atoms(atoms)
    
    created = [a for a in atoms if a.get("action") == "created"]
    updated = [a for a in atoms if a.get("action") == "updated"]
    
    return {
        "status": "scanned",
        "filepath": str(filepath),
        "language": language,
        "lines": len(code.split("\n")),
        "atoms_created": len(created),
        "atoms_updated": len(updated),
        "atoms_embedded": embedded,
        "atoms": atoms,
    }


@router.post("/repo")
async def scan_repo(req: ScanRepoRequest):
    """Scan all supported files in a project directory."""
    root = Path(req.path)
    
    if not root.exists() or not root.is_dir():
        raise HTTPException(404, f"Directory not found: {req.path}")
    
    project = req.project or root.name
    files = _discover_files(str(root), req.extensions, req.max_files)
    
    if not files:
        return {"status": "empty", "path": req.path, "message": "No scannable files found"}
    
    scanner = get_scanner_service()
    total_created = 0
    total_updated = 0
    total_embedded = 0
    file_results = []
    all_atoms = []
    
    for filepath in files:
        try:
            ext = Path(filepath).suffix
            language = SUPPORTED_EXTENSIONS.get(ext, "unknown")
            code = Path(filepath).read_text(encoding="utf-8", errors="replace")
            atoms = await scanner.extract_atoms(code, language, filepath)
            
            created = [a for a in atoms if a.get("action") == "created"]
            updated = [a for a in atoms if a.get("action") == "updated"]
            
            embedded = 0
            if req.embed and atoms:
                embedded = await _embed_atoms(atoms)
            
            total_created += len(created)
            total_updated += len(updated)
            total_embedded += embedded
            all_atoms.extend(atoms)
            
            file_results.append({
                "file": filepath,
                "created": len(created),
                "updated": len(updated),
                "embedded": embedded,
            })
        except Exception as e:
            logger.error(f"Failed to scan {filepath}: {e}")
            file_results.append({"file": filepath, "error": str(e)})
    
    return {
        "status": "scanned",
        "project": project,
        "path": req.path,
        "files_scanned": len(files),
        "files_with_atoms": len([f for f in file_results if f.get("created", 0) + f.get("updated", 0) > 0]),
        "total_atoms_created": total_created,
        "total_atoms_updated": total_updated,
        "total_atoms_embedded": total_embedded,
        "files": file_results,
    }


# === Hook 2: Forge/Infra-Watcher Webhook ===

@router.post("/webhook")
async def scan_webhook(req: WebhookRequest):
    """Webhook endpoint for Forge workspace_write and infra-watcher events.
    
    Called automatically when files are written/modified.
    Only scans supported file types; ignores others silently.
    """
    filepath = Path(req.path)
    ext = filepath.suffix
    
    if ext not in SUPPORTED_EXTENSIONS:
        return {"status": "skipped", "reason": f"unsupported extension: {ext}", "path": req.path}
    
    if filepath.name in IGNORE_FILES:
        return {"status": "skipped", "reason": "ignored file", "path": req.path}
    
    # Get content: from payload or read from disk
    code = req.content
    if code is None:
        if not filepath.exists():
            return {"status": "skipped", "reason": "file not found and no content provided", "path": req.path}
        if filepath.stat().st_size > MAX_FILE_SIZE:
            return {"status": "skipped", "reason": f"file too large ({filepath.stat().st_size} bytes)", "path": req.path}
        code = filepath.read_text(encoding="utf-8", errors="replace")
    
    language = SUPPORTED_EXTENSIONS[ext]
    scanner = get_scanner_service()
    atoms = await scanner.extract_atoms(code, language, str(filepath))
    
    embedded = 0
    if atoms:
        embedded = await _embed_atoms(atoms)
    
    created = [a for a in atoms if a.get("action") == "created"]
    updated = [a for a in atoms if a.get("action") == "updated"]
    
    logger.info(f"Webhook scan [{req.source}:{req.event}] {req.path}: {len(created)} created, {len(updated)} updated")
    
    return {
        "status": "scanned",
        "source": req.source,
        "event": req.event,
        "filepath": req.path,
        "project": req.project,
        "atoms_created": len(created),
        "atoms_updated": len(updated),
        "atoms_embedded": embedded,
    }


# === Hook 1: Transcript Code Extraction ===

@router.post("/transcript")
async def scan_transcript(req: TranscriptRequest):
    """Extract code blocks from a conversation transcript and scan them.
    
    Accepts either raw transcript text or structured messages array.
    Extracts fenced code blocks, filters for supported languages,
    and feeds each through the scanner.
    """
    code_blocks = []
    
    # Extract from structured messages if provided
    if req.messages:
        for msg in req.messages:
            sender = msg.get("sender", "")
            # Only scan assistant-generated code (that's what we built)
            if sender != "assistant":
                continue
            
            # Check text field
            text = msg.get("text", "")
            if text:
                code_blocks.extend(_extract_code_blocks(text))
            
            # Check content blocks
            for block in msg.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text" and block.get("text"):
                    code_blocks.extend(_extract_code_blocks(block["text"]))
    
    # Also extract from raw transcript text
    if req.transcript:
        code_blocks.extend(_extract_code_blocks(req.transcript))
    
    if not code_blocks:
        return {
            "status": "no_code",
            "conversation_id": req.conversation_id,
            "message": "No substantial code blocks found in transcript",
        }
    
    # Deduplicate by content hash
    seen = set()
    unique_blocks = []
    for block in code_blocks:
        import hashlib
        h = hashlib.sha256(block["code"].encode()).hexdigest()[:16]
        if h not in seen:
            seen.add(h)
            unique_blocks.append(block)
    
    scanner = get_scanner_service()
    total_created = 0
    total_updated = 0
    total_embedded = 0
    block_results = []
    
    for i, block in enumerate(unique_blocks):
        lang = block["language"]
        if lang not in SUPPORTED_EXTENSIONS.values() and lang != "python":
            # Try to detect python
            if "def " in block["code"] or "import " in block["code"] or "class " in block["code"]:
                lang = "python"
            else:
                block_results.append({
                    "block": i,
                    "language": block["language"],
                    "status": "skipped",
                    "reason": f"unsupported language: {block['language']}",
                })
                continue
        
        filepath = f"<transcript:{req.conversation_id or 'unknown'}:block-{i}>"
        atoms = await scanner.extract_atoms(block["code"], lang, filepath)
        
        created = [a for a in atoms if a.get("action") == "created"]
        updated = [a for a in atoms if a.get("action") == "updated"]
        
        embedded = 0
        if atoms:
            embedded = await _embed_atoms(atoms)
        
        total_created += len(created)
        total_updated += len(updated)
        total_embedded += embedded
        
        block_results.append({
            "block": i,
            "language": lang,
            "chars": block["char_count"],
            "atoms_created": len(created),
            "atoms_updated": len(updated),
            "atoms_embedded": embedded,
        })
    
    return {
        "status": "scanned",
        "conversation_id": req.conversation_id,
        "code_blocks_found": len(code_blocks),
        "unique_blocks": len(unique_blocks),
        "blocks_scanned": len([b for b in block_results if b.get("atoms_created", 0) + b.get("atoms_updated", 0) > 0]),
        "total_atoms_created": total_created,
        "total_atoms_updated": total_updated,
        "total_atoms_embedded": total_embedded,
        "blocks": block_results,
    }


# === Stats ===

@router.get("/stats")
async def scan_stats():
    """Get scanner statistics."""
    db = get_db()
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM atoms")
        total_atoms = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM atoms WHERE meta LIKE '%scanner_v1%'")
        scanner_atoms = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT json_extract(meta, '$.semantic.category') as cat, COUNT(*) as cnt
            FROM atoms
            WHERE json_extract(meta, '$.semantic.category') IS NOT NULL
            GROUP BY cat ORDER BY cnt DESC
        """)
        categories = {row[0]: row[1] for row in cursor.fetchall()}
        
        cursor.execute("""
            SELECT json_extract(meta, '$.provenance.first_seen_project') as proj, COUNT(*) as cnt
            FROM atoms
            WHERE json_extract(meta, '$.provenance.first_seen_project') IS NOT NULL
            GROUP BY proj ORDER BY cnt DESC
        """)
        projects = {row[0]: row[1] for row in cursor.fetchall()}
    
    return {
        "total_atoms": total_atoms,
        "scanner_produced": scanner_atoms,
        "by_category": categories,
        "by_project": projects,
        "supported_languages": list(SUPPORTED_EXTENSIONS.values()),
        "supported_extensions": list(SUPPORTED_EXTENSIONS.keys()),
    }
