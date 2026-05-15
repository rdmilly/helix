"""Decay Job - writes decay meta namespace. PAUSED until full scan complete."""
DECAY_ENABLED = False
DECAY_FACTOR = 0.95
DECAY_AFTER_DAYS = 30
async def run_decay():
    if not DECAY_ENABLED:
        return {"status": "disabled", "reason": "Set DECAY_ENABLED=True in decay_job.py after full codebase scan", "decayed": 0}
    from services.database import get_db
    from services.meta import get_meta_service
    from datetime import datetime, timedelta
    db = get_db(); meta = get_meta_service()
    cutoff = (datetime.utcnow() - timedelta(days=DECAY_AFTER_DAYS)).isoformat()
    with db.get_connection() as conn:
        stale = conn.execute("SELECT id, name, COALESCE(weight, 1.0) FROM atoms WHERE last_seen < %s", (cutoff,)).fetchall()
    decayed = 0
    for atom_id, name, weight in stale:
        new_weight = round(weight * DECAY_FACTOR, 4)
        with db.get_connection() as conn:
            conn.execute("UPDATE atoms SET weight = %s WHERE id = %s", (new_weight, atom_id)); conn.commit()
        meta.write_meta("atoms", atom_id, "decay", {"previous_weight": weight, "new_weight": new_weight, "decayed_at": datetime.utcnow().isoformat()}, written_by="decay_job_v1")
        decayed += 1
    return {"status": "ok", "decayed": decayed}
