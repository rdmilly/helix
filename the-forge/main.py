"""The Forge — Workspace + Pattern Composition Engine.

Unified service providing:
- Workspace: File CRUD backed by MinIO with versioning and FTS
- Forge: Atom/molecule/organism pattern catalog and composition
- Every write triggers pattern scanning (write = scan)

Designed to integrate with Memory's Observer module:
- Observer captures tool calls at the MCP gateway
- File content from writes gets POSTed to The Forge for scanning
- Forge catalog feeds back composition for future work
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import PORT, DEBUG, DATA_DIR, WORKSPACE_ROOT, DB_PATH
from services.database import init_db
from services.minio_client import init_minio

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("forge")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    logger.info("=" * 50)
    logger.info("The Forge v0.1.0 starting up...")
    logger.info(f"  Port: {PORT}")
    logger.info(f"  Data: {DATA_DIR}")
    logger.info(f"  Workspace: {WORKSPACE_ROOT}")
    logger.info(f"  Database: {DB_PATH}")

    # Ensure directories
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)

    # Initialize database
    init_db()
    logger.info("Database initialized")

    # Initialize MinIO
    minio_ok = init_minio()
    logger.info(f"MinIO: {'connected' if minio_ok else 'NOT AVAILABLE (filesystem-only mode)'}")

    logger.info("The Forge is ready.")
    logger.info("=" * 50)
    yield
    logger.info("The Forge shutting down.")


app = FastAPI(
    title="The Forge",
    description="Workspace + Pattern Composition Engine",
    version="0.1.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
from routers import workspace, forge
app.include_router(workspace.router)
app.include_router(forge.router)


@app.get("/health")
async def health():
    from services.database import get_stats
    stats = get_stats()
    return {
        "status": "healthy",
        "version": "0.1.0",
        "service": "the-forge",
        **stats
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
