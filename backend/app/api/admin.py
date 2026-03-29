"""
Admin & Status API

GET  /api/admin/status          — engine status, latest date, run history
POST /api/admin/run             — trigger full engine run (async)
POST /api/admin/backfill        — trigger backfill of all bhav files (async)
GET  /api/admin/runs            — engine run history (last 30)
GET  /api/admin/calendar        — market calendar (last 30 days)

All POST endpoints require header:  X-Admin-Secret: <ADMIN_SECRET>
"""
import asyncio
import logging
import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Query
from app.core.config import settings
from app.core.database import get_pool

logger = logging.getLogger(__name__)
router = APIRouter()


def _verify_secret(x_admin_secret: str = Header(..., alias="X-Admin-Secret")) -> None:
    """Dependency — validates admin secret header."""
    if x_admin_secret != settings.ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret")


# ── STATUS ────────────────────────────────────────────────────────────
@router.get("/status", summary="Engine status and system info")
async def get_status(pool=Depends(get_pool)):
    """
    Returns overall system health:
    - last engine run (status, duration, symbol count)
    - latest trading date with data
    - total trading days processed
    - total symbols in DB
    """
    async with pool.acquire() as conn:
        # Latest completed run
        last_run = await conn.fetchrow(
            """select id, run_date, trigger, started_at, finished_at,
                      status, symbols_processed, bhav_files_loaded,
                      duration_secs, error_message
               from engine_runs
               order by started_at desc
               limit 1"""
        )
        # Latest trading date
        latest_date_row = await conn.fetchrow(
            "select trade_date from v_latest_date"
        )
        # Counts
        stats = await conn.fetchrow(
            """select
                (select count(*) from symbols where is_active = true)           as total_symbols,
                (select count(*) from market_calendar where engine_status='done') as total_days,
                (select count(*) from engine_runs where status='success')         as successful_runs,
                (select max(trade_date) from market_calendar
                 where engine_status = 'running')                                 as currently_running_for
            """
        )

    return {
        "latest_date":          str(latest_date_row["trade_date"]) if latest_date_row and latest_date_row["trade_date"] else None,
        "total_symbols":        stats["total_symbols"],
        "total_trading_days":   stats["total_days"],
        "successful_runs":      stats["successful_runs"],
        "currently_running":    stats["currently_running_for"] is not None,
        "last_run":             dict(last_run) if last_run else None,
    }


# ── RUN HISTORY ───────────────────────────────────────────────────────
@router.get("/runs", summary="Engine run history")
async def get_run_history(
    limit: int = Query(30, ge=1, le=100),
    pool=Depends(get_pool),
):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """select id, run_date, trigger, started_at, finished_at,
                      status, symbols_processed, bhav_files_loaded,
                      duration_secs, error_message
               from engine_runs
               order by started_at desc
               limit $1""",
            limit,
        )
    return {"count": len(rows), "runs": [dict(r) for r in rows]}


# ── CALENDAR ─────────────────────────────────────────────────────────
@router.get("/calendar", summary="Market calendar")
async def get_calendar(
    limit: int = Query(30, ge=1, le=252),
    pool=Depends(get_pool),
):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """select trade_date, is_trading_day, holiday_name,
                      bhav_downloaded, engine_status, symbol_count,
                      engine_duration_secs, error_message, processed_at
               from market_calendar
               order by trade_date desc
               limit $1""",
            limit,
        )
    return {"count": len(rows), "calendar": [dict(r) for r in rows]}


# ── TRIGGER DAILY RUN ────────────────────────────────────────────────
@router.post("/run", summary="Trigger engine run",
             dependencies=[Depends(_verify_secret)])
async def trigger_run(pool=Depends(get_pool)):
    """
    Triggers a full engine run in the background.
    The run loads all bhav files, computes metrics, and upserts to DB.
    Returns immediately — check /api/admin/status for progress.
    """
    # Check if a run is already in progress
    async with pool.acquire() as conn:
        running = await conn.fetchrow(
            "select id from engine_runs where status='running' limit 1"
        )
    if running:
        raise HTTPException(status_code=409,
            detail="An engine run is already in progress")

    asyncio.create_task(_run_engine_task(pool, trigger="manual"))
    return {"status": "triggered", "message": "Engine run started in background"}


# ── TRIGGER BACKFILL ──────────────────────────────────────────────────
@router.post("/backfill", summary="Trigger full historical backfill",
             dependencies=[Depends(_verify_secret)])
async def trigger_backfill(pool=Depends(get_pool)):
    """
    Re-processes ALL bhav files in DATA_FOLDER.
    Use this on first setup or to re-compute all historical data.
    Takes several minutes for 252 files × 2143 symbols.
    """
    async with pool.acquire() as conn:
        running = await conn.fetchrow(
            "select id from engine_runs where status='running' limit 1"
        )
    if running:
        raise HTTPException(status_code=409,
            detail="An engine run is already in progress")

    asyncio.create_task(_run_engine_task(pool, trigger="backfill"))
    return {"status": "triggered", "message": "Backfill started in background"}


# ── INTERNAL TASK ─────────────────────────────────────────────────────
async def _run_engine_task(pool, trigger: str = "manual"):
    """Background task — wraps the engine with DB logging."""
    from app.services.scheduler import run_daily_pipeline
    try:
        await run_daily_pipeline(pool, trigger=trigger)
    except Exception as e:
        logger.exception("Engine task failed: %s", e)
