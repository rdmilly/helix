"""Cockpit Router — Phase 6: Nervous System Dashboard API

Endpoints for system monitoring, DNA metrics, pipeline health,
anomaly/nudge feeds, and activity timeline.

All endpoints are read-only (GET) and support optional filtering.
"""
from fastapi import APIRouter, Query
from typing import Optional

from services.cockpit import get_cockpit_service

router = APIRouter(prefix="/api/v1/cockpit", tags=["Cockpit - Phase 6"])


# ── System Overview ─────────────────────────────────────────

@router.get("/overview")
async def get_overview():
    """Full system overview: DNA counts, pipeline health, infrastructure"""
    cockpit = get_cockpit_service()
    return cockpit.get_overview()


# ── Anomalies ───────────────────────────────────────────────

@router.get("/anomalies")
async def get_anomalies(
    severity: Optional[str] = Query(None, description="Filter by severity (critical, high, medium, low)"),
    state: Optional[str] = Query(None, description="Filter by state (active, resolved, dismissed)"),
    limit: int = Query(50, ge=1, le=500, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """List anomalies with optional severity/state filtering"""
    cockpit = get_cockpit_service()
    return cockpit.get_anomalies(
        severity=severity,
        state=state,
        limit=limit,
        offset=offset,
    )


# ── Nudges ──────────────────────────────────────────────────

@router.get("/nudges")
async def get_nudges(
    state: Optional[str] = Query(None, description="Filter by state (pending, acted, dismissed)"),
    category: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(50, ge=1, le=500, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """List nudges with optional state/category filtering"""
    cockpit = get_cockpit_service()
    return cockpit.get_nudges(
        state=state,
        category=category,
        limit=limit,
        offset=offset,
    )


# ── Activity Timeline ───────────────────────────────────────

@router.get("/timeline")
async def get_timeline(
    action: Optional[str] = Query(None, description="Filter by action (meta_set, meta_create, etc.)"),
    target_table: Optional[str] = Query(None, description="Filter by target table (atoms, molecules, etc.)"),
    hours: int = Query(24, ge=1, le=720, description="Time window in hours"),
    limit: int = Query(100, ge=1, le=1000, description="Results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """Activity timeline from meta_events"""
    cockpit = get_cockpit_service()
    return cockpit.get_timeline(
        action=action,
        target_table=target_table,
        hours=hours,
        limit=limit,
        offset=offset,
    )


# ── DNA Stats ───────────────────────────────────────────────

@router.get("/dna")
async def get_dna_stats():
    """Detailed DNA library stats: atoms, molecules, organisms with metadata"""
    cockpit = get_cockpit_service()
    return cockpit.get_dna_stats()


# ── Pipeline Stats ──────────────────────────────────────────

@router.get("/pipeline")
async def get_pipeline_stats(
    hours: int = Query(24, ge=1, le=720, description="Time window in hours"),
):
    """Pipeline throughput: queue, compression, sessions, dictionary"""
    cockpit = get_cockpit_service()
    return cockpit.get_pipeline_stats(hours=hours)
