"""
MemBrain Vector Router — /api/v1/ext/vector/*

Four endpoints for the paid tier + one provisioning endpoint.

Auth: Bearer token in Authorization header (format: mbr_*)
      Validated against SHA-256 hash in membrain_users table.
      Provisioning endpoint uses the existing X-Helix-Admin-Key.

Endpoints:
    POST   /api/v1/ext/vector/migrate   — one-time migration at upgrade
    POST   /api/v1/ext/vector/upsert    — store a single new fact
    POST   /api/v1/ext/vector/search    — semantic search
    DELETE /api/v1/ext/vector/delete    — remove a fact's vector

Admin:
    POST   /api/v1/ext/vector/provision — create user record + return token
    GET    /api/v1/ext/vector/users     — list all users
    DELETE /api/v1/ext/vector/users/{user_id} — revoke user
"""
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, status
from pydantic import BaseModel, Field

from services.membrain_auth import membrain_auth, MembrainUser
from services.membrain_vector import membrain_vector

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ext/vector", tags=["MemBrain Vector"])

# Admin key — same env var pattern used elsewhere in Helix
ADMIN_KEY = os.getenv("HELIX_ADMIN_KEY", "")


# ==================== AUTH HELPERS ====================

async def require_user(authorization: Optional[str]) -> MembrainUser:
    """Extract and verify Bearer token. Raises 401 on failure."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Bearer token")

    raw_token = authorization.removeprefix("Bearer ").strip()
    user = await membrain_auth.verify_token(raw_token)

    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or revoked token")

    return user


def require_admin(x_admin_key: Optional[str]):
    """Verify admin key. Raises 403 on failure."""
    if not ADMIN_KEY:
        raise HTTPException(status_code=500, detail="HELIX_ADMIN_KEY not configured")
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin key")


# ==================== SCHEMAS ====================

class MigrateFact(BaseModel):
    factId: str
    text: str
    category: Optional[str] = ""
    confidence: Optional[float] = None
    createdAt: Optional[str] = None


class MigrateRequest(BaseModel):
    facts: List[MigrateFact] = Field(..., description="All local facts to re-embed into Cortex")
    extensionVersion: Optional[str] = None


class MigrateResponse(BaseModel):
    migrated: int
    failed: int
    collection: str


class UpsertRequest(BaseModel):
    factId: str
    text: str
    category: Optional[str] = ""
    confidence: Optional[float] = None


class UpsertResponse(BaseModel):
    ok: bool
    factId: str


class SearchRequest(BaseModel):
    query: str
    topK: int = Field(default=8, ge=1, le=20)
    threshold: float = Field(default=0.35, ge=0.0, le=1.0)


class SearchResult(BaseModel):
    factId: str
    score: float
    text: str


class SearchResponse(BaseModel):
    results: List[SearchResult]
    model: str = "bge-large-en-v1.5"


class DeleteRequest(BaseModel):
    factId: str


class DeleteResponse(BaseModel):
    ok: bool
    factId: str


class ProvisionRequest(BaseModel):
    email: str
    tier: str = "paid"
    note: Optional[str] = None  # e.g. "Stripe sub_xyz123"


class ProvisionResponse(BaseModel):
    token: str        # Raw token — shown ONCE, not stored in plain text
    user_id: str
    collection: str
    note: str = "Store this token securely — it cannot be retrieved again."


# ==================== ENDPOINTS ====================

@router.post("/migrate", response_model=MigrateResponse)
async def migrate(
    body: MigrateRequest,
    authorization: Optional[str] = Header(None),
):
    """
    One-time migration endpoint called when a user upgrades to paid tier.

    Receives all local facts as plain text.
    Cortex re-embeds them using bge-large-en-v1.5 (1024d) and stores
    them in the user's dedicated ChromaDB collection.

    Idempotent — safe to retry if the connection drops.
    """
    user = await require_user(authorization)

    # Ensure collection exists before batch upsert
    await membrain_vector.ensure_collection(user.id)

    facts_dicts = [f.model_dump() for f in body.facts]
    result = await membrain_vector.upsert_batch(user.id, facts_dicts)

    # Audit log
    membrain_auth._log_event(user.id, "migrate", {
        "facts_received": len(body.facts),
        "migrated": result["upserted"],
        "extension_version": body.extensionVersion,
    })

    logger.info(f"[Vector/migrate] user={user.id} migrated={result['upserted']} failed={result['failed']}")

    return MigrateResponse(
        migrated=result["upserted"],
        failed=result["failed"],
        collection=user.collection_name,
    )


@router.post("/upsert", response_model=UpsertResponse)
async def upsert(
    body: UpsertRequest,
    authorization: Optional[str] = Header(None),
):
    """
    Store a single new fact after upgrade.
    Called every time the SW saves a new fact (replaces local embedder).
    """
    user = await require_user(authorization)

    ok = await membrain_vector.upsert(
        user_id=user.id,
        fact_id=body.factId,
        text=body.text,
        metadata={"category": body.category or "", "confidence": body.confidence or 0},
    )

    if not ok:
        raise HTTPException(status_code=500, detail="Upsert failed — check Cortex logs")

    membrain_auth._log_event(user.id, "upsert", {"factId": body.factId})
    return UpsertResponse(ok=True, factId=body.factId)


@router.post("/search", response_model=SearchResponse)
async def search(
    body: SearchRequest,
    authorization: Optional[str] = Header(None),
):
    """
    Semantic search across a user's fact collection.
    Called just before each outgoing message to find relevant memories.
    Returns the same shape as the local vector store search so
    CloudVectorBackend is a drop-in swap.
    """
    user = await require_user(authorization)

    raw_results = await membrain_vector.search(
        user_id=user.id,
        query=body.query,
        top_k=body.topK,
        threshold=body.threshold,
    )

    membrain_auth._log_event(user.id, "search", {"results": len(raw_results)})

    return SearchResponse(
        results=[SearchResult(**r) for r in raw_results],
    )


@router.delete("/delete", response_model=DeleteResponse)
async def delete_fact(
    body: DeleteRequest,
    authorization: Optional[str] = Header(None),
):
    """
    Remove a single fact's vector.
    Called when a user deletes a fact in the extension options page.
    """
    user = await require_user(authorization)

    ok = await membrain_vector.delete(user_id=user.id, fact_id=body.factId)
    membrain_auth._log_event(user.id, "delete", {"factId": body.factId})

    return DeleteResponse(ok=ok, factId=body.factId)


# ==================== ADMIN ENDPOINTS ====================

@router.post("/provision", response_model=ProvisionResponse)
async def provision_user(
    body: ProvisionRequest,
    x_admin_key: Optional[str] = Header(None),
):
    """
    Create a new paid user.
    Protected by X-Admin-Key header (HELIX_ADMIN_KEY env var).

    Call this from your Stripe webhook when a subscription is created.
    Returns the raw token ONCE — store it in your payment system
    and email it to the user.

    Example curl:
        curl -X POST https://helix.millyweb.com/api/v1/ext/vector/provision \\
          -H "X-Admin-Key: $HELIX_ADMIN_KEY" \\
          -H "Content-Type: application/json" \\
          -d '{"email": "customer@example.com", "note": "sub_abc123"}'
    """
    require_admin(x_admin_key)

    raw_token, user_id = membrain_auth.provision_user(email=body.email, tier=body.tier)

    # Pre-create the ChromaDB collection so first migrate is instant
    collection_name = f"membrain_{user_id}"
    await membrain_vector.ensure_collection(user_id)

    if body.note:
        membrain_auth._log_event(user_id, "provision_note", {"note": body.note})

    logger.info(f"[Vector/provision] Provisioned user {user_id} ({body.email})")

    return ProvisionResponse(
        token=raw_token,
        user_id=user_id,
        collection=collection_name,
    )


@router.get("/users")
async def list_users(x_admin_key: Optional[str] = Header(None)):
    """List all users with their stats. Admin only."""
    require_admin(x_admin_key)
    users = membrain_auth.list_users()
    # Add vector counts
    for u in users:
        stats = await membrain_vector.get_collection_stats(u["id"])
        u["vector_count"] = stats.get("count", 0)
    return {"users": users, "total": len(users)}


@router.delete("/users/{user_id}")
async def revoke_user(user_id: str, x_admin_key: Optional[str] = Header(None)):
    """Revoke a user's token. Admin only. Does NOT delete their vectors."""
    require_admin(x_admin_key)
    ok = membrain_auth.revoke_user(user_id)
    return {"ok": ok, "user_id": user_id}
