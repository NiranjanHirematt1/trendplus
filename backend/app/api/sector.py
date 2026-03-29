"""
GET /api/sector           — all sectors for a date
GET /api/sector/list      — sector names list
GET /api/sector/{name}    — sector detail + all stocks
"""
import datetime
import logging
from typing import Optional
from urllib.parse import unquote

from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.database import get_pool

logger = logging.getLogger(__name__)
router = APIRouter()

VALID_SECTOR_SORT = frozenset({
    "avg_momentum", "avg_rs_score", "avg_trending_days",
    "avg_chg_12d", "avg_chg_5d", "avg_chg_today",
    "pct_stocks_up", "stock_count", "stocks_near_high",
    "avg_rsi_14", "avg_adx_14",
})


def _resolve_date(raw) -> Optional[datetime.date]:
    """Return datetime.date regardless of whether input is str or date."""
    if raw is None:
        return None
    if isinstance(raw, datetime.date):
        return raw
    try:
        return datetime.date.fromisoformat(str(raw))
    except ValueError:
        return None


@router.get("", summary="Sector momentum summary")
async def get_sectors(
    date:    Optional[str] = Query(None),
    sort_by: str           = Query("avg_momentum"),
    order:   str           = Query("desc", pattern="^(asc|desc)$"),
    pool=Depends(get_pool),
):
    if sort_by not in VALID_SECTOR_SORT:
        sort_by = "avg_momentum"

    async with pool.acquire() as conn:
        if not date:
            row = await conn.fetchrow("select trade_date from v_latest_date")
            trade_date = _resolve_date(row["trade_date"] if row else None)
        else:
            trade_date = _resolve_date(date)

        if not trade_date:
            return {"date": None, "total": 0, "data": []}

        sort_dir = "asc" if order == "asc" else "desc"
        rows = await conn.fetch(
            f"""select
                    sector, stock_count, stocks_up, stocks_down,
                    pct_stocks_up, avg_trending_days, avg_chg_12d,
                    avg_chg_5d, avg_chg_today, avg_rsi_14, avg_adx_14,
                    avg_rs_score, avg_momentum,
                    stocks_near_high, pct_near_high
                from sector_daily
                where trade_date = $1
                order by {sort_by} {sort_dir} nulls last""",
            trade_date,
        )
    return {
        "date":  str(trade_date),
        "total": len(rows),
        "data":  [dict(r) for r in rows],
    }


@router.get("/list", summary="Sector name list")
async def get_sector_list(pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """select distinct sector, count(*) as symbol_count
               from symbols
               where sector != '' and is_active = true
               group by sector order by sector"""
        )
    return {"sectors": [dict(r) for r in rows]}


@router.get("/{sector_name}", summary="Sector detail with all stocks")
async def get_sector_detail(
    sector_name: str,
    date:    Optional[str] = Query(None),
    sort_by: str           = Query("momentum_score"),
    pool=Depends(get_pool),
):
    sector_name = unquote(sector_name)

    valid_stock_sort = frozenset({
        "momentum_score", "rs_score", "trending_days",
        "chg_12d", "chg_5d", "chg_1d", "rsi_14", "adx_14",
        "rank_52w", "close_price", "total_trades", "macd_hist",
    })
    if sort_by not in valid_stock_sort:
        sort_by = "momentum_score"

    async with pool.acquire() as conn:
        if not date:
            row = await conn.fetchrow("select trade_date from v_latest_date")
            trade_date = _resolve_date(row["trade_date"] if row else None)
        else:
            trade_date = _resolve_date(date)

        if not trade_date:
            raise HTTPException(404, "No trading data available")

        summary = await conn.fetchrow(
            "select * from sector_daily where trade_date = $1 and sector = $2",
            trade_date, sector_name,
        )
        if not summary:
            raise HTTPException(404, f"Sector '{sector_name}' not found for {trade_date}")

        stocks = await conn.fetch(
            f"""select
                    s.symbol, s.company_name, s.cap_category,
                    tr.trending_days, tr.chg_1d, tr.chg_5d, tr.chg_12d,
                    tr.rsi_14, tr.adx_14, tr.rs_score, tr.momentum_score,
                    tr.pct_from_high, tr.near_52w_high, tr.rank_52w,
                    tr.high_52w, tr.close_price, tr.total_trades,
                    tr.macd_hist, tr.ema_signal,
                    tr.ema_50, tr.ema_200
                from trend_results tr
                join symbols s on s.symbol = tr.symbol
                where tr.trade_date = $1 and s.sector = $2
                order by tr.{sort_by} desc nulls last""",
            trade_date, sector_name,
        )

    return {
        "sector":  sector_name,
        "date":    str(trade_date),
        "summary": dict(summary),
        "stocks":  [dict(r) for r in stocks],
    }