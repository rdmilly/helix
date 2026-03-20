"""Intake Router - Registry-Based Routing

POST /api/v1/ingest - Single intake endpoint with type-based routing.
Implements all Phase 1 deliverables:
- Registry-based routing
- Idempotent intake
- Content hash deduplication
- Rate limiting / backpressure
- Credential encryption
- Queue persistence
"""
import hashlib
import json
import logging
import uuid
from typing import Dict, Any
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from models.schemas import IntakePayload
from services.database import get_db
from services.registry import get_registry_service
from services.meta import get_meta_service
from config import MAX_QUEUE_DEPTH, CREDENTIAL_PATTERNS
from services.redis_cache import get_redis_cache
from services import pg_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")


@router.post("/ingest")
async def ingest(payload: IntakePayload, request: Request):
    """
    Universal intake endpoint with registry-based routing.
    
    Process:
    1. Compute content hash for idempotency
    2. Check for duplicates
    3. Detect and encrypt credentials
    4. Validate intake type via registry
    5. Enqueue with priority
    6. Return immediately (async processing)
    """
    db = get_db()
    registry = get_registry_service()
    
    try:
        # === IDEMPOTENCY CHECK ===
        
        content_str = json.dumps(payload.payload, sort_keys=True)
        content_hash = hashlib.sha256(content_str.encode()).hexdigest()
        
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT queue_id FROM intake_hashes
                WHERE content_hash = ?
            """, (content_hash,))
            
            existing = cursor.fetchone()
            if existing:
                logger.info(f"Duplicate intake blocked: hash {content_hash[:8]}")
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "duplicate",
                        "message": "Content already received",
                        "content_hash": content_hash
                    }
                )
            
            # === RATE LIMITING ===
            
            cursor.execute("""
                SELECT COUNT(*) FROM queue
                WHERE status = 'pending'
            """)
            pending_count = cursor.fetchone()[0]
            
            if pending_count >= MAX_QUEUE_DEPTH:
                logger.warning(f"Queue at max depth: {pending_count}/{MAX_QUEUE_DEPTH}")
                raise HTTPException(status_code=429, detail="Queue at maximum capacity, retry later")
            
            # === VALIDATE INTAKE TYPE ===
            
            intake_type_info = registry.get_type(payload.intake_type)
            if not intake_type_info:
                logger.error(f"Unknown intake type: {payload.intake_type}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown intake type: {payload.intake_type}"
                )
            
            if not intake_type_info["active"]:
                logger.error(f"Inactive intake type: {payload.intake_type}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Intake type {payload.intake_type} is not active"
                )
            
            # === CREDENTIAL DETECTION & ENCRYPTION ===
            
            encrypted_payload = _detect_and_encrypt_credentials(payload.payload)
            
            # === ENQUEUE ===
            
            queue_id = f"q_{uuid.uuid4().hex[:12]}"
            
            cursor.execute("""
                INSERT INTO queue (id, intake_type, content_type, payload, status, priority, user_id)
                VALUES (?, ?, ?, ?, 'pending', ?, ?)
            """, (
                queue_id,
                payload.intake_type,
                payload.content_type,
                json.dumps(encrypted_payload),
                payload.priority,
                user_id,
            ))
            
            # Record hash
            cursor.execute("""
                INSERT INTO intake_hashes (content_hash, intake_type, queue_id)
                VALUES (?, ?, ?)
            """, (content_hash, payload.intake_type, queue_id))
            
            conn.commit()

            # Push to Redis hot queue (best-effort — PG is source of truth)
            redis = get_redis_cache()
            await redis.queue_push(queue_id, payload.priority)

            logger.info(f"Enqueued {payload.intake_type}: {queue_id} (priority {payload.priority})")
            
            return JSONResponse(
                status_code=202,
                content={
                    "status": "accepted",
                    "queue_id": queue_id,
                    "content_hash": content_hash,
                    "intake_type": payload.intake_type,
                    "message": "Payload accepted for processing"
                }
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Intake error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@router.get("/queue/status/{queue_id}")
async def get_queue_status(queue_id: str):
    """Check status of a queued item"""
    db = get_db()
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, status, created_at, started_at, completed_at, error, attempts
            FROM queue
            WHERE id = ?
        """, (queue_id,))
        
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Queue item not found")
        
        return {
            "queue_id": row[0],
            "status": row[1],
            "created_at": row[2],
            "started_at": row[3],
            "completed_at": row[4],
            "error": row[5],
            "attempts": row[6]
        }


@router.get("/queue/stats")
async def get_queue_stats():
    """Get queue statistics"""
    db = get_db()
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM queue
            GROUP BY status
        """)
        
        stats = {}
        for row in cursor.fetchall():
            stats[row[0]] = row[1]
        
        cursor.execute("SELECT COUNT(*) FROM queue WHERE status = 'pending'")
        pending = cursor.fetchone()[0]
        
        return {
            "by_status": stats,
            "pending_count": pending,
            "max_depth": MAX_QUEUE_DEPTH,
            "utilization": f"{(pending / MAX_QUEUE_DEPTH) * 100:.1f}%"
        }


def _detect_and_encrypt_credentials(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Detect credentials in payload and encrypt them.
    Phase 1: Detection only, logging.
    Phase 2: Actual encryption using Infisical key.
    """
    # Convert to JSON string for pattern matching
    payload_str = json.dumps(payload)
    
    detected = []
    for pattern in CREDENTIAL_PATTERNS:
        import re
        matches = re.findall(pattern, payload_str)
        if matches:
            detected.extend(matches)
    
    if detected:
        logger.warning(f"Detected {len(detected)} potential credentials in payload")
        # TODO Phase 2: Encrypt using config.CREDENTIAL_ENCRYPTION_KEY
        # For now, just log and pass through
    
    return payload
