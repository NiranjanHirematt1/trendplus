"""
GET /api/search?q=reliance
GET /api/search?q=INFY

Fuzzy search across symbol and company name.
Uses pg_trgm for similarity matching — very fast with GIN index.

Response
────────
{
  "query": "reliance",
  "count": 5,
  "results": [
    { "symbol": "RELIANCE", "company_name": "Reliance Industries...",
      "sector": "Oil Gas...", "cap_category": "Large Cap",
      "momentum_score": 72.3, "rs_score": 88.1, "close_price": 1234.50 }
  ]
}
"""
import logging
from fastapi import APIRouter, Depends, Query, HTTPException
from app.core.database import get_pool

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("", summary="Search symbols and company names")
async def search(
    q:    str = Query(..., min_length=1, max_length=100, description="Symbol or company name"),
    pool=Depends(get_pool),
):
    q = q.strip()
    if not q:
        raise HTTPException(status_code=422, detail="Query cannot be empty")

    async with pool.acquire() as conn:
        # Latest trade date for joining momentum data
        date_row = await conn.fetchrow("select trade_date from v_latest_date")
        latest_date = date_row["trade_date"] if date_row else None

        if latest_date:
            # Join with latest trend data so results include live metrics
            rows = await conn.fetch(
                """
                select
                    s.symbol,
                    s.company_name,
                    s.sector,
                    s.cap_category,
                    s.isin,
                    tr.momentum_score,
                    tr.rs_score,
                    tr.trending_days,
                    tr.chg_1d,
                    tr.chg_12d,
                    tr.rsi_14,
                    tr.close_price
                from symbols s
                left join trend_results tr
                    on tr.symbol = s.symbol
                    and tr.trade_date = $3
                where s.is_active = true
                  and (
                      s.symbol       ilike $1
                   or s.company_name ilike $1
                   or s.symbol       %     $2
                   or s.company_name %     $2
                  )
                order by
                    case when s.symbol ilike $2 then 0
                         when s.symbol ilike $1 then 1
                         else 2
                    end,
                    similarity(s.company_name, $2) desc
                limit 20
                """,
                f"%{q}%", q, latest_date,
            )
        else:
            # No trend data yet — return symbol info only
            rows = await conn.fetch(
                """
                select
                    s.symbol, s.company_name, s.sector, s.cap_category, s.isin,
                    null::numeric as momentum_score,
                    null::numeric as rs_score,
                    null::smallint as trending_days,
                    null::numeric as chg_1d,
                    null::numeric as chg_12d,
                    null::numeric as rsi_14,
                    null::numeric as close_price
                from symbols s
                where s.is_active = true
                  and (s.symbol ilike $1 or s.company_name ilike $1)
                order by
                    case when s.symbol ilike $2 then 0 else 1 end,
                    s.symbol
                limit 20
                """,
                f"%{q}%", q,
            )

    return {
        "query":   q,
        "count":   len(rows),
        "results": [dict(r) for r in rows],
    }
