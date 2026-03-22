"""Helix Cortex - Main Entry Point

FastAPI application with health probes, graceful startup,
Phase 1 routers, Phase 2 Mitochondria worker, Phase 3 Synapse API,
Phase 4 Compression Engine, Phase 5 Editor, and Phase 6 Cockpit.
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from config import PORT, HOST, ALLOWED_ORIGINS
from routers.intake import router as intake_router
from routers.token_count import token_count_router
from routers.synapse import (
    lifecycle_router,
    search_router,
    context_router,
    synapse_tier1_router,
)
from routers.admin import admin_router
from routers.compression import compression_router
from routers.editor import router as editor_router
from routers.cockpit import router as cockpit_router
from routers.recovery import router as recovery_router
from routers.shard import router as shard_router
from routers.turn_flush import router as turn_flush_router
from routers.inject import router as inject_router
from routers.scan import router as scan_router
from routers.runbook import router as runbook_router
from routers.conversations import router as conversations_router
from routers.observer import router as observer_router
from routers.knowledge import router as knowledge_router
from routers.archive import router as archive_router
from routers.kb import router as kb_router
from routers.backup import router as backup_router
from routers.exchange import router as exchange_router
from routers.ext_ingest import router as ext_ingest_router
from routers.membrain_vector import router as membrain_vector_router
from routers.proxy import proxy_router
from routers.ops import router as ops_router
from routers.master_status import router as master_status_router
from routers.assemble import router as assemble_router
from routers.action import router as action_router
from routers.nodes import router as nodes_router
from services.exchange import ensure_tables as ensure_exchange_tables
from routers.tenants import tenants_router
from routers.init import init_router
from routers.usage import usage_router
from routers.dashboard import dashboard_router
from routers.auth import auth_router
from routers.register import register_router
from services.tenant_auth import TenantMiddleware
from routers.widget import widget_router
# MCP endpoint removed — helix-mcp (port 9096) is now the single MCP surface.
# helix-mcp proxies all tool calls to cortex REST. cortex no longer exposes /mcp.
from routers.stubs import (
    flagella_router,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("helix.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    log.info("Helix Cortex starting up...")

    # Database init
    try:
        from services.database import get_db
        db = get_db()
        log.info("Database skipping legacy initialize() - using pg_sync directly")
    except Exception as e:
        log.error(f"Database check failed: {e}")

    # Exchange tables
    try:
        ensure_exchange_tables()
        log.info("Exchange tables initialized")
    except Exception as e:
        log.warning(f"Exchange tables init failed: {e}")

    # Dictionary
    try:
        from services.dictionary import get_dictionary
        d = get_dictionary()
        count = d.load()
        log.info(f"Dictionary loaded: {count}")
    except Exception as e:
        log.warning(f"Dictionary failed to load: {e}")

    # BM25
    try:
        from services.bm25_store import get_bm25_store
        get_bm25_store()
        log.info("BM25 FTS5 store initialized")
    except Exception as e:
        log.warning(f"BM25 init failed: {e}")

    # Vector store
    try:
        from services.vector_store import get_vector_store
        vs = get_vector_store()
        await vs.initialize()
        log.info(f"VectorStore ready (pgvector + BGE-large)")
    except Exception as e:
        log.warning(f"VectorStore unavailable (circuit breaker open) -- vector search degraded: {e}")

    # Neo4j
    try:
        from services.neo4j_store import get_neo4j_store
        neo = get_neo4j_store()
        info = neo.stats()
        log.info(f"Neo4j KG ready: {info}")
    except Exception as e:
        log.warning(f"Neo4j unavailable (circuit breaker open) -- KG queries degraded: {e}")

    # Redis
    try:
        from services.redis_cache import get_redis_cache
        redis = get_redis_cache()
        depth = await redis.queue_length()
        log.info(f"Redis ready: queue_depth={depth}")
    except Exception as e:
        log.warning(f"Redis unavailable (circuit breaker open) -- queue uses PG fallback: {e}")

    # Haiku
    try:
        from services.haiku import get_haiku_service
        haiku = get_haiku_service()
        if haiku.api_key:
            log.info("Haiku API connected")
        else:
            log.warning("Haiku API unavailable (will use heuristic fallback)")
    except Exception as e:
        log.warning(f"Haiku init error: {e}")

    # LLMPort
    try:
        from services.llm_port import get_llm_port
        llm = get_llm_port()
        log.info(f"LLMPort: provider={llm.provider} model={llm.default_model}")
    except Exception as e:
        log.warning(f"LLMPort init error: {e}")

    # Scheduler
    try:
        from services.scheduler import get_scheduler, register_default_jobs
        scheduler = get_scheduler()
        register_default_jobs(scheduler)
        await scheduler.start()
        log.info("Scheduler started")
    except Exception as e:
        log.warning(f"Scheduler failed to start: {e}")

    # Mitochondria worker
    try:
        from services.worker import get_worker
        worker = get_worker()
        await worker.start()
        log.info("Mitochondria worker started")
    except Exception as e:
        log.warning(f"Worker failed to start: {e}")

    # MCP endpoint disabled — helix-mcp handles all MCP traffic

    log.info(f"Helix Cortex v0.9.0 ready on port 9050")

    yield

    # Shutdown
    log.info("Helix Cortex shutting down...")

    try:
        from services.worker import get_worker
        worker = get_worker()
        await worker.stop()
        log.info("Worker stopped")
    except Exception:
        pass

    try:
        from services.scheduler import get_scheduler
        get_scheduler().stop()
        log.info("Scheduler stopped")
    except Exception:
        pass

    # teardown_mcp removed

    try:
        from services.workbench import get_workbench
        wb = get_workbench()
        if wb._http and not wb._http.is_closed:
            await wb._http.aclose()
        log.info("HTTP clients closed")
    except Exception:
        pass

    log.info("Helix Cortex shutdown complete")


app = FastAPI(
    title="Helix Cortex",
    description="Epigenetic Data Architecture",
    version="0.9.0",
    lifespan=lifespan,
    docs_url="/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(TenantMiddleware)


@app.get("/health")
async def health():
    """Liveness probe - is the process alive?"""
    from services import pg_sync
    db_ok = False
    atom_count = 0
    session_count = 0
    queue_pending = 0
    recent_completed = 0
    recent_failed = 0

    try:
        conn = pg_sync.sqlite_conn()
        conn.execute("SELECT 1")
        db_ok = True
        atom_count = conn.execute("SELECT COUNT(*) FROM atoms").fetchone()[0]
        session_count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        queue_pending = conn.execute("SELECT COUNT(*) FROM queue WHERE status = ?", ('pending',)).fetchone()[0]
        recent_completed = conn.execute("SELECT COUNT(*) FROM queue WHERE status = ? AND completed_at > datetime('now', '-1 hour')", ('completed',)).fetchone()[0]
        recent_failed = conn.execute("SELECT COUNT(*) FROM queue WHERE status = ? AND completed_at > datetime('now', '-1 hour')", ('failed',)).fetchone()[0]
    except Exception as e:
        log.debug(f"Health check db error: {e}")

    return {
        "status": "alive",
        "version": "v0.9.0",
        "database": "ok" if db_ok else "error",
        "atoms": atom_count,
        "sessions": session_count,
        "queue": {
            "pending": queue_pending,
            "completed_1h": recent_completed,
            "failed_1h": recent_failed,
        },
    }


@app.get("/ready")
async def ready():
    """Readiness probe - are dependencies available?"""
    from services.vector_store import get_vector_store
    from services.redis_cache import get_redis_cache

    checks = {}

    # DB
    try:
        from services import pg_sync
        conn = pg_sync.sqlite_conn()
        conn.execute("SELECT 1")
        checks["database"] = "ready"
    except Exception as e:
        checks["database"] = f"not_ready: {e}"

    # Vector
    try:
        vs = get_vector_store()
        checks["embeddings"] = "ready" if vs.is_ready() else "not_ready"
    except Exception:
        checks["embeddings"] = "not_initialized"

    # Redis
    try:
        redis = get_redis_cache()
        depth = await redis.queue_length()
        checks["queue"] = "ready"
        checks["queue_depth"] = depth
        if depth > 450:
            checks["queue"] = "at_capacity"
    except Exception:
        checks["queue"] = "not_ready"

    # ChromaDB
    try:
        from services.chromadb import get_chroma_store
        cs = get_chroma_store()
        checks["chromadb"] = "ready" if cs.is_ready() else "not_initialized"
    except Exception:
        checks["chromadb"] = "not_initialized"

    all_ready = all(
        v in ("ready", "not_initialized")
        for k, v in checks.items()
        if k != "queue_depth"
    )

    return JSONResponse(
        status_code=200 if all_ready else 503,
        content={
            "status": "ready" if all_ready else "not_ready",
            "checks": checks,
        },
    )


@app.get("/")
async def root():
    """Root endpoint with system info"""
    return {
        "service": "helix-cortex",
        "version": "v0.9.0 — Migration Complete",
        "description": "Epigenetic Intelligence Platform",
        "docs": "/docs",
        "health": "/health",
        "ready": "/ready",
        "mcp": "/mcp",
    }


# ================================================================
# ROUTERS
# ================================================================

app.include_router(intake_router, tags=["Intake"])
app.include_router(token_count_router, tags=["Compression Proxy"])
app.include_router(lifecycle_router, tags=["Lifecycle - Phase 3"])
app.include_router(search_router, tags=["Search - Phase 3"])
app.include_router(context_router, tags=["Context - Phase 3"])
app.include_router(synapse_tier1_router, tags=["Lifecycle - Phase 3"])
app.include_router(compression_router, tags=["Compression - Phase 4"])
app.include_router(editor_router, tags=["Editor - Phase 5"])
app.include_router(cockpit_router, tags=["Cockpit - Phase 6"])
app.include_router(scan_router, tags=["Scan - Phase 7a"])
app.include_router(conversations_router, tags=["Conversations - RAG"])
app.include_router(observer_router, tags=["Observer - Unified Capture"])
app.include_router(knowledge_router, tags=["Knowledge Graph"])
app.include_router(archive_router, tags=["Structured Archive"])
app.include_router(kb_router, tags=["KB Unification"])
app.include_router(backup_router, tags=["Backup & Restore"])
app.include_router(ops_router, tags=["Operations"])
app.include_router(master_status_router, tags=["Master Status"])
app.include_router(assemble_router, tags=["Assembler"])
app.include_router(action_router, tags=["helix_action"])
app.include_router(nodes_router, tags=["Nodes"])
app.include_router(flagella_router, tags=["Flagella - Phase 7b"])
app.include_router(recovery_router, tags=["Recovery - Crash Protection"])
app.include_router(admin_router, tags=["Admin"])
app.include_router(inject_router, tags=["Inject - Context Pipeline"])
app.include_router(runbook_router, tags=["Runbook - Dynamic Registry"])
app.include_router(exchange_router, tags=["Exchange - Per-Exchange Observations"])
app.include_router(ext_ingest_router, tags=["Extension Ingest"])
app.include_router(membrain_vector_router, tags=["MemBrain Vector"])
app.include_router(shard_router, tags=["Shard - Diff Context"])
app.include_router(turn_flush_router, tags=["Turn Flush - Phase 1.3"])
app.include_router(proxy_router, tags=["Double Helix - Phase 7a: Conversation RAG"])
app.include_router(tenants_router, tags=["Tenants - Layer 0"])
app.include_router(init_router, tags=["Init - Layer 1"])
app.include_router(usage_router, tags=["Usage - Layer 2"])
app.include_router(dashboard_router)
app.include_router(auth_router, tags=["Auth"])
app.include_router(register_router)
app.include_router(widget_router)

# New proper auth system
from routers.login import login_router
from routers.auth_google import auth_google_router
from routers.auth_email import auth_email_router
app.include_router(login_router)
app.include_router(auth_google_router, tags=["Auth - Google OAuth"])
app.include_router(auth_email_router, tags=["Auth - Email/Password"])

# MCP mount removed — see helix-mcp service (port 9096)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
