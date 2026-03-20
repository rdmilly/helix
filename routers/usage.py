"""
usage.py — Layer 2: per-tenant usage metering

GET /api/v1/usage          — current tenant summary
GET /api/v1/usage/history  — daily breakdown (last 30 days)
GET /api/v1/admin/usage    — all tenants (admin)
"""
import logging
from fastapi import APIRouter, Request, HTTPException
from typing import Optional
from services import pg_sync

logger = logging.getLogger(__name__)
usage_router = APIRouter(prefix="/api/v1")


@usage_router.get("/usage")
def get_usage(request: Request, days: int = 30):
    """Current tenant usage summary. Scoped by API key / subdomain."""
    tenant_id = getattr(request.state, "tenant_id", "system")
    days = max(1, min(days, 365))

    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()

            # Token totals
            cur.execute("""
                SELECT
                    COUNT(DISTINCT session_id)  AS sessions,
                    COUNT(*)                    AS exchanges,
                    COALESCE(SUM(tokens_in), 0) AS tokens_in,
                    COALESCE(SUM(tokens_out),0) AS tokens_out,
                    COALESCE(SUM(tool_calls), 0) AS tool_calls,
                    MIN(timestamp)              AS first_seen,
                    MAX(timestamp)              AS last_seen
                FROM observer_session_tokens
                WHERE tenant_id = %s
                  AND timestamp >= NOW() - (%s || ' days')::interval
            """, (tenant_id, str(days)))
            row = cur.fetchone()

            # Observer actions count
            cur.execute("""
                SELECT COUNT(*) FROM observer_actions
                WHERE tenant_id = %s
                  AND timestamp >= NOW() - (%s || ' days')::interval
            """, (tenant_id, str(days)))
            action_row = cur.fetchone()

            # Tenant plan
            cur.execute("SELECT slug, name, plan FROM tenants WHERE id = %s", (tenant_id,))
            tenant_row = cur.fetchone()

    except Exception as e:
        raise HTTPException(500, str(e))

    total_in  = int(row[2]) if row else 0
    total_out = int(row[3]) if row else 0

    return {
        "tenant_id": tenant_id,
        "tenant": tenant_row[0] if tenant_row else tenant_id,
        "tenant_name": tenant_row[1] if tenant_row else "",
        "plan": tenant_row[2] if tenant_row else "unknown",
        "period_days": days,
        "sessions": int(row[0]) if row else 0,
        "exchanges": int(row[1]) if row else 0,
        "tokens_in": total_in,
        "tokens_out": total_out,
        "tokens_total": total_in + total_out,
        "tool_calls": int(row[4]) if row else 0,
        "observer_actions": int(action_row[0]) if action_row else 0,
        "first_seen": str(row[5]) if row and row[5] else None,
        "last_seen": str(row[6]) if row and row[6] else None,
    }


@usage_router.get("/usage/history")
def get_usage_history(request: Request, days: int = 30):
    """Daily token breakdown for current tenant."""
    tenant_id = getattr(request.state, "tenant_id", "system")
    days = max(1, min(days, 365))

    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    DATE(timestamp AT TIME ZONE 'UTC') AS day,
                    COUNT(DISTINCT session_id)          AS sessions,
                    COUNT(*)                            AS exchanges,
                    COALESCE(SUM(tokens_in), 0)         AS tokens_in,
                    COALESCE(SUM(tokens_out), 0)        AS tokens_out,
                    COALESCE(SUM(tool_calls), 0)        AS tool_calls
                FROM observer_session_tokens
                WHERE tenant_id = %s
                  AND timestamp >= NOW() - (%s || ' days')::interval
                GROUP BY DATE(timestamp AT TIME ZONE 'UTC')
                ORDER BY day DESC
            """, (tenant_id, str(days)))
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(500, str(e))

    return [
        {
            "day": str(r[0]),
            "sessions": int(r[1]),
            "exchanges": int(r[2]),
            "tokens_in": int(r[3]),
            "tokens_out": int(r[4]),
            "tokens_total": int(r[3]) + int(r[4]),
            "tool_calls": int(r[5]),
        }
        for r in rows
    ]


@usage_router.get("/admin/usage")
def get_all_usage(days: int = 30):
    """All tenants usage summary (admin endpoint, no RLS)."""
    days = max(1, min(days, 365))

    try:
        with pg_sync.get_pg_conn(admin=True) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    t.slug,
                    t.name,
                    t.plan,
                    COUNT(DISTINCT ost.session_id) AS sessions,
                    COUNT(ost.id)                  AS exchanges,
                    COALESCE(SUM(ost.tokens_in),0) AS tokens_in,
                    COALESCE(SUM(ost.tokens_out),0) AS tokens_out,
                    COALESCE(SUM(ost.tool_calls),0) AS tool_calls
                FROM tenants t
                LEFT JOIN observer_session_tokens ost
                    ON ost.tenant_id = t.id
                   AND ost.timestamp >= NOW() - (%s || ' days')::interval
                GROUP BY t.id, t.slug, t.name, t.plan
                ORDER BY SUM(ost.tokens_in) + SUM(ost.tokens_out) DESC NULLS LAST
            """, (str(days),))
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(500, str(e))

    return [
        {
            "tenant": r[0], "name": r[1], "plan": r[2],
            "sessions": int(r[3]), "exchanges": int(r[4]),
            "tokens_in": int(r[5]), "tokens_out": int(r[6]),
            "tokens_total": int(r[5]) + int(r[6]),
            "tool_calls": int(r[7]),
        }
        for r in rows
    ]
