from services import pg_sync
"""Scheduler Service — Built-in Periodic Task Runner

Lightweight asyncio scheduler that runs inside Helix.
No external deps, no system cron needed. Users get automatic
periodic tasks just by running the Helix container.

Registered jobs:
  - compression_profiles: Rebuild personal compression profiles (daily)
  - db_backup: Backup SQLite databases (every 6 hours)
  - pattern_decay: Decay stale compression patterns (weekly)

All jobs are fire-and-forget — failures logged but never crash the app.
Jobs run on startup after initial_delay, then repeat at interval.
"""
import asyncio
import logging
import time
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable, Awaitable, List
from dataclasses import dataclass, field

log = logging.getLogger("helix.scheduler")


@dataclass
class ScheduledJob:
    """A periodic job definition."""
    name: str
    fn: Callable[[], Awaitable[Any]]  # async function to call
    interval_seconds: int             # how often to run
    initial_delay: int = 60           # wait after startup before first run
    enabled: bool = True
    last_run: Optional[str] = None
    last_result: Optional[str] = None
    last_error: Optional[str] = None
    run_count: int = 0
    error_count: int = 0
    _task: Optional[asyncio.Task] = field(default=None, repr=False)


class SchedulerService:
    """Manages periodic background tasks inside Helix."""

    def __init__(self):
        self.jobs: Dict[str, ScheduledJob] = {}
        self._running = False

    def register(self, name: str, fn: Callable, interval_seconds: int,
                 initial_delay: int = 60, enabled: bool = True):
        """Register a periodic job."""
        self.jobs[name] = ScheduledJob(
            name=name, fn=fn,
            interval_seconds=interval_seconds,
            initial_delay=initial_delay,
            enabled=enabled,
        )
        log.info(f"Registered job: {name} (every {interval_seconds}s, delay {initial_delay}s)")

    async def start(self):
        """Start all registered jobs."""
        if self._running:
            return
        self._running = True
        for name, job in self.jobs.items():
            if job.enabled:
                job._task = asyncio.create_task(self._run_loop(job))
                log.info(f"Started job: {name}")
        log.info(f"Scheduler started with {len(self.jobs)} jobs")

    async def stop(self):
        """Stop all running jobs gracefully."""
        self._running = False
        for name, job in self.jobs.items():
            if job._task and not job._task.done():
                job._task.cancel()
                try:
                    await job._task
                except asyncio.CancelledError:
                    pass
        log.info("Scheduler stopped")

    async def _run_loop(self, job: ScheduledJob):
        """Run a single job on its schedule."""
        try:
            # Initial delay
            await asyncio.sleep(job.initial_delay)

            while self._running:
                now = datetime.now(timezone.utc).isoformat()
                try:
                    log.info(f"Running job: {job.name}")
                    result = await job.fn()
                    job.last_run = now
                    job.run_count += 1
                    job.last_result = str(result)[:500] if result else "ok"
                    job.last_error = None
                    log.info(f"Job {job.name} completed (run #{job.run_count})")
                except Exception as e:
                    job.last_run = now
                    job.error_count += 1
                    job.last_error = f"{type(e).__name__}: {e}"
                    log.error(f"Job {job.name} failed: {e}\n{traceback.format_exc()}")

                # Sleep until next run
                await asyncio.sleep(job.interval_seconds)

        except asyncio.CancelledError:
            log.info(f"Job {job.name} cancelled")

    async def run_now(self, name: str) -> Dict[str, Any]:
        """Manually trigger a job immediately."""
        job = self.jobs.get(name)
        if not job:
            return {"error": f"Job '{name}' not found"}

        now = datetime.now(timezone.utc).isoformat()
        try:
            result = await job.fn()
            job.last_run = now
            job.run_count += 1
            job.last_result = str(result)[:500] if result else "ok"
            job.last_error = None
            return {"status": "ok", "job": name, "result": job.last_result}
        except Exception as e:
            job.last_run = now
            job.error_count += 1
            job.last_error = f"{type(e).__name__}: {e}"
            return {"status": "error", "job": name, "error": job.last_error}

    def get_status(self) -> Dict[str, Any]:
        """Get status of all registered jobs."""
        return {
            "running": self._running,
            "job_count": len(self.jobs),
            "jobs": {
                name: {
                    "enabled": job.enabled,
                    "interval_seconds": job.interval_seconds,
                    "last_run": job.last_run,
                    "last_result": job.last_result[:100] if job.last_result else None,
                    "last_error": job.last_error,
                    "run_count": job.run_count,
                    "error_count": job.error_count,
                }
                for name, job in self.jobs.items()
            }
        }


# Singleton
_instance: Optional[SchedulerService] = None

def get_scheduler() -> SchedulerService:
    global _instance
    if _instance is None:
        _instance = SchedulerService()
    return _instance


# ============================================================
# JOB DEFINITIONS — the actual tasks
# ============================================================

async def job_build_compression_profiles():
    """Rebuild compression profiles from latest transcript data."""
    from services.compression_profiles import get_profile_service
    svc = get_profile_service()
    result = svc.build_profiles(rebuild=False)
    stats = result.get("stats", {})
    return f"patterns={stats.get('total',0)} active={stats.get('active',0)} proven={stats.get('proven',0)} saved={stats.get('active_tokens_saved',0)}"


async def job_decay_stale_patterns():
    """Decay compression patterns not seen recently."""
    from services.compression_profiles import get_profile_service
    svc = get_profile_service()
    # Just run build which includes decay step
    result = svc.build_profiles(rebuild=False)
    return f"decayed={result.get('decays', 0)}"


async def job_db_backup():
    """Backup SQLite databases (cortex.db + conversations_fts.db) to the backups directory.

    Uses sqlite3 directly — pg_sync wraps psycopg2 which does not support
    the SQLite-only VACUUM INTO syntax.
    """
    import sqlite3
    import os
    from pathlib import Path
    from config import DB_PATH, FTS_DB_PATH

    backup_dir = DB_PATH.parent / "backups"
    backup_dir.mkdir(exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    results = []

    for db_path, label in [(DB_PATH, "cortex"), (FTS_DB_PATH, "fts")]:
        if not db_path.exists():
            log.warning(f"db_backup: {db_path} not found, skipping")
            continue
        backup_path = backup_dir / f"{label}-{timestamp}.db"
        try:
            # Use sqlite3 directly — NOT pg_sync which routes through psycopg2
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute(f"VACUUM INTO '{backup_path}'")
                conn.commit()
            finally:
                conn.close()
            size_mb = os.path.getsize(str(backup_path)) / 1024 / 1024
            results.append(f"{label}={size_mb:.1f}MB")
            log.info(f"db_backup: {label} -> {backup_path.name} ({size_mb:.1f}MB)")
        except Exception as e:
            log.error(f"db_backup: failed to back up {label}: {e}")
            results.append(f"{label}=ERROR({e})")

    # Prune old backups — keep last 10 of each type
    for label in ["cortex", "fts"]:
        old = sorted(backup_dir.glob(f"{label}-*.db"))
        while len(old) > 10:
            oldest = old.pop(0)
            oldest.unlink()
            log.info(f"db_backup: pruned {oldest.name}")

    return "backed up: " + ", ".join(results)


async def job_promote_phrases():
    """Promote proven phrases to shorthand symbol dictionary (epigenetic schema)."""
    try:
        from services.phrase_promoter import get_phrase_promoter
        p = get_phrase_promoter()
        result = p.promote()
        return f"Phrase promotion: {result['promoted']} new symbols, {result.get('total_symbols', 0)} total"
    except Exception as e:
        log.error(f"Phrase promotion failed: {e}")
        return f"error: {e}"


async def job_file_scanner():
    """Walk /opt/projects, hash-index files, send new/changed to Forge for atom extraction."""
    from services.file_scanner import trigger_scan
    result = await trigger_scan()
    return (
        f"scanned={result.get('scanned',0)} "
        f"new={result.get('new',0)} "
        f"changed={result.get('changed',0)} "
        f"unchanged={result.get('skipped_unchanged',0)} "
        f"atoms={result.get('atoms_created',0)} "
        f"duration={result.get('duration_s',0)}s"
    )


def register_default_jobs(scheduler: SchedulerService):
    """Register all default periodic jobs."""
    # Compression profiles: daily (86400s), first run after 5 min
    scheduler.register(
        "compression_profiles",
        job_build_compression_profiles,
        interval_seconds=86400,  # 24 hours
        initial_delay=300,       # 5 min after startup
    )

    # Phrase promoter: daily (runs after compression_profiles), first run after 10 min
    scheduler.register(
        "phrase_promotion",
        job_promote_phrases,
        interval_seconds=86400,  # 24 hours
        initial_delay=600,       # 10 min after startup (after compression_profiles)
    )

    # Database backup: every 6 hours, first run after 10 min
    scheduler.register(
        "db_backup",
        job_db_backup,
        interval_seconds=21600,  # 6 hours
        initial_delay=600,       # 10 min after startup
    )

    # Pattern decay check: weekly (run daily, decay logic has 30-day window)
    scheduler.register(
        "pattern_decay",
        job_decay_stale_patterns,
        interval_seconds=604800,  # 7 days
        initial_delay=900,        # 15 min after startup
    )

    # File scanner: every 4 hours, first run after 2 min
    # Walks /opt/projects, hash-indexes files, sends new/changed to Forge
    scheduler.register(
        "file_scanner",
        job_file_scanner,
        interval_seconds=14400,  # 4 hours
        initial_delay=120,       # 2 min after startup — run early to populate atoms
    )

    # Haiku reconciler: hourly, first run after 3 min
    scheduler.register(
        "haiku_reconciler",
        job_haiku_reconciler,
        interval_seconds=3600,  # 1 hour
        initial_delay=180,      # 3 min after startup
    )

    scheduler.register(
        "conversation_ingester",
        job_conversation_ingester,
        interval_seconds=300,   # 5 min
        initial_delay=60,       # 1 min after startup
    )

    log.info(f"Registered {len(scheduler.jobs)} default jobs")


async def job_haiku_reconciler():
    """Run Haiku reconciler: extract decisions/patterns/failures from recent archive entries."""
    from services.reconciler import run_reconciler
    result = await run_reconciler()
    return (f"processed={result['processed']} decisions={result['decisions']} "
            f"patterns={result['patterns']} failures={result['failures']} "
            f"errors={result['errors']} skipped={result['skipped']}")


async def job_conversation_ingester():
    """Sync new FTS conversation chunks into Postgres sessions (5min)."""
    from services.ingester import run_ingester
    import asyncio
    result = await asyncio.get_event_loop().run_in_executor(None, run_ingester)
    return f"new={result['new_sessions']} skipped={result['skipped']} errors={result['errors']}"
