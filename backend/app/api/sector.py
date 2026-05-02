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
from backend.app.core.sector_mapping import normalize_sector_name

logger = logging.getLogger(__name__)
router = APIRouter()

VALID_SECTOR_SORT = frozenset({
    "avg_momentum", "avg_rs_score", "avg_trending_days",
    "avg_chg_12d", "avg_chg_5d", "avg_chg_today",
    "pct_stocks_up", "stock_count", "stocks_near_high",
    "avg_rsi_14", "avg_adx_14",
})


def _aggregate_sector_rows(rows):
    grouped = {}
    avg_fields = ["avg_trending_days","avg_chg_12d","avg_chg_5d","avg_chg_today","avg_rsi_14","avg_adx_14","avg_rs_score","avg_momentum","pct_near_high"]
    for r in rows:
        d = dict(r)
        canonical = normalize_sector_name(d.get("sector"))
        stock_count = int(d.get("stock_count") or 0)
        g = grouped.setdefault(canonical, {"sector": canonical, "stock_count": 0, "stocks_up": 0, "stocks_down": 0, "stocks_near_high": 0, "_w": 0})
        g["stock_count"] += stock_count
        g["stocks_up"] += int(d.get("stocks_up") or 0)
        g["stocks_down"] += int(d.get("stocks_down") or 0)
        g["stocks_near_high"] += int(d.get("stocks_near_high") or 0)
        g["_w"] += stock_count
        for f in avg_fields:
            v = d.get(f)
            if v is not None:
                g[f] = g.get(f, 0.0) + float(v) * stock_count
    out=[]
    for g in grouped.values():
        w=max(g.pop("_w"),1)
        for f in avg_fields:
            if f in g:
                g[f]=g[f]/w
            else:
                g[f]=None
        g["pct_stocks_up"]=(g["stocks_up"]*100.0/g["stock_count"]) if g["stock_count"] else 0.0
        out.append(g)
    return out

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
            """select
                    sector, stock_count, stocks_up, stocks_down,
                    pct_stocks_up, avg_trending_days, avg_chg_12d,
                    avg_chg_5d, avg_chg_today, avg_rsi_14, avg_adx_14,
                    avg_rs_score, avg_momentum,
                    stocks_near_high, pct_near_high
                from sector_daily
                where trade_date = $1""",

                
            trade_date,
        )
    data = _aggregate_sector_rows(rows)
    data.sort(key=lambda x: (x.get(sort_by) is None, x.get(sort_by)), reverse=(sort_dir=="desc"))
    return {"date": str(trade_date), "total": len(data), "data": data}


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

        raw_summary = await conn.fetch(
            "select * from sector_daily where trade_date = $1 and sector = $2",
            trade_date,
        )
        summary_rows = [r for r in raw_summary if normalize_sector_name(r["sector"]) == sector_name]
        if not summary_rows:
                raise HTTPException(404, f"Sector '{sector_name}' not found for {trade_date}")
        summary = _aggregate_sector_rows(summary_rows)[0]

        all_stocks = await conn.fetch(
            f"""select
                    s.symbol, s.company_name, s.cap_category,s.sector,
                    tr.trending_days, tr.chg_1d, tr.chg_5d, tr.chg_12d,
                    tr.rsi_14, tr.adx_14, tr.rs_score, tr.momentum_score,
                    tr.pct_from_high, tr.near_52w_high, tr.rank_52w,
                    tr.high_52w, tr.close_price, tr.total_trades,
                    tr.macd_hist, tr.ema_signal,
                    tr.ema_50, tr.ema_200
                from trend_results tr
                join symbols s on s.symbol = tr.symbol
                where tr.trade_date = $1
                order by tr.{sort_by} desc nulls last""",
            trade_date,
        )
        stocks = [dict(r) for r in all_stocks if normalize_sector_name(r["sector"]) == sector_name]
        for r in stocks:
            r.pop("sector", None)

    return {
        "sector":  sector_name,
        "date":    str(trade_date),
        "summary": summary,
        "stocks":  stocks,
    }