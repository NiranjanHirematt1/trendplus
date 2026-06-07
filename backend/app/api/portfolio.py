from __future__ import annotations

from datetime import date
from decimal import Decimal
import math

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from app.api.deps import current_user
from app.core.database import get_pool
from app.models.portfolio import HoldingCreate, HoldingSold, HoldingUpdate, PortfolioCreate
from app.services.portfolio_import import parse_portfolio_file

router = APIRouter()


def num(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return value


def as_dict(row):
    data = dict(row)
    return {k: num(v) for k, v in data.items()}


def recommendation(row) -> str:
    gain = float(row.get("gain_pct") or 0)
    momentum = float(row.get("momentum_score") or 0)
    rs = float(row.get("rs_score") or 0)
    trend = float(row.get("trending_days") or 0) / 12 * 100
    vol_penalty = min(abs(float(row.get("chg_5d") or 0)) * 2, 25)
    score = gain * 0.25 + momentum * 0.30 + rs * 0.20 + trend * 0.15 - vol_penalty * 0.10
    if score >= 65 and gain >= 0:
        return "ADD MORE"
    if score >= 45:
        return "HOLD"
    if score >= 25 or gain > 10:
        return "TRIM"
    return "EXIT"


async def ensure_portfolio(conn, user_id: int) -> int:
    pid = await conn.fetchval("select id from portfolios where user_id = $1 order by created_at limit 1", user_id)
    if pid:
        return pid
    return await conn.fetchval("insert into portfolios (user_id, portfolio_name) values ($1, 'My Portfolio') returning id", user_id)


async def assert_symbol(conn, symbol: str):
    exists = await conn.fetchval("select true from symbols where symbol = $1 and is_active = true", symbol)
    if not exists:
        raise HTTPException(422, f"Unknown symbol: {symbol}")


@router.get("", summary="List user portfolios")
async def list_portfolios(user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        await ensure_portfolio(conn, user["id"])
        rows = await conn.fetch("select id, portfolio_name, created_at, updated_at from portfolios where user_id = $1 order by created_at", user["id"])
    return {"data": [as_dict(r) for r in rows]}


@router.post("", summary="Create portfolio")
async def create_portfolio(payload: PortfolioCreate, user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "insert into portfolios (user_id, portfolio_name) values ($1, $2) returning id, portfolio_name, created_at, updated_at",
            user["id"], payload.portfolio_name.strip(),
        )
    return as_dict(row)


@router.get("/summary", summary="Portfolio dashboard and analytics")
async def portfolio_summary(user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        pid = await ensure_portfolio(conn, user["id"])
        rows = await enriched_holdings(conn, pid, active_only=False)
    active = [r for r in rows if r["status"] == "ACTIVE"]
    total_investment = sum(float(r["investment_amount"] or 0) for r in active)
    current_value = sum(float(r["current_value"] or 0) for r in active)
    pnl = current_value - total_investment
    winning = [r for r in active if float(r["profit_loss"] or 0) > 0]
    losing = [r for r in active if float(r["profit_loss"] or 0) < 0]
    sector_totals = {}
    for r in active:
        sector = r.get("sector") or "Unclassified"
        sector_totals[sector] = sector_totals.get(sector, 0) + float(r["current_value"] or 0)
    dead = [r for r in active if (r.get("days_held") or 0) > 180 and -5 <= float(r.get("gain_pct") or 0) <= 5]
    return {
        "portfolio_id": pid,
        "cards": {
            "total_investment": total_investment,
            "current_value": current_value,
            "profit_loss": pnl,
            "return_pct": (pnl / total_investment * 100) if total_investment else 0,
            "number_of_holdings": len(active),
            "winning_holdings": len(winning),
            "losing_holdings": len(losing),
        },
        "top_gainers": sorted(active, key=lambda r: float(r.get("gain_pct") or 0), reverse=True)[:5],
        "top_losers": sorted(active, key=lambda r: float(r.get("gain_pct") or 0))[:5],
        "winners_vs_losers": {
            "winning_capital_pct": (sum(float(r["current_value"] or 0) for r in winning) / current_value * 100) if current_value else 0,
            "losing_capital_pct": (sum(float(r["current_value"] or 0) for r in losing) / current_value * 100) if current_value else 0,
        },
        "sector_allocation": [{"sector": k, "value": v, "pct": (v / current_value * 100) if current_value else 0} for k, v in sorted(sector_totals.items())],
        "dead_money": dead,
    }


async def enriched_holdings(conn, portfolio_id: int, active_only: bool = True):
    status_filter = "and h.status = 'ACTIVE'" if active_only else ""
    rows = await conn.fetch(
        f"""
        with latest as (select trade_date from v_latest_date)
        select h.*, s.company_name, s.sector, s.cap_category,
               tr.close_price as current_price, tr.momentum_score, tr.rs_score, tr.trending_days,
               tr.chg_1d, tr.chg_5d, tr.chg_12d, tr.rsi_14,
               greatest(coalesce(h.sell_date, current_date) - h.buy_date, 0) as days_held,
               (h.quantity * h.avg_buy_price) as investment_amount,
               case when h.status = 'ACTIVE' then (h.quantity * tr.close_price) else (h.quantity * h.sell_price) end as current_value,
               case when h.status = 'ACTIVE' then (h.quantity * (tr.close_price - h.avg_buy_price)) else (h.quantity * (h.sell_price - h.avg_buy_price)) end as profit_loss,
               case when h.avg_buy_price > 0 then ((case when h.status = 'ACTIVE' then coalesce(tr.close_price, h.avg_buy_price) else h.sell_price end - h.avg_buy_price) / h.avg_buy_price * 100) end as gain_pct
        from holdings h
        join symbols s on s.symbol = h.symbol
        left join latest l on true
        left join trend_results tr on tr.symbol = h.symbol and tr.trade_date = l.trade_date
        where h.portfolio_id = $1 {status_filter}
        order by h.status, h.created_at desc
        """,
        portfolio_id,
    )
    result = []
    for row in rows:
        d = as_dict(row)
        if d["status"] == "ACTIVE":
            d["recommendation"] = recommendation(d)
            d["portfolio_contribution"] = None
            if d.get("days_held") and d.get("gain_pct") is not None:
                d["annualized_return"] = ((1 + d["gain_pct"] / 100) ** (365 / max(d["days_held"], 1)) - 1) * 100
            else:
                d["annualized_return"] = None
        result.append(d)
    return result


@router.get("/holdings", summary="List continuously tracked holdings")
async def list_holdings(user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        pid = await ensure_portfolio(conn, user["id"])
        rows = await enriched_holdings(conn, pid, active_only=False)
    total_value = sum(float(r.get("current_value") or 0) for r in rows if r["status"] == "ACTIVE")
    for r in rows:
        if r["status"] == "ACTIVE":
            r["portfolio_contribution"] = (float(r.get("current_value") or 0) / total_value * 100) if total_value else 0
    return {"data": rows}


@router.post("/holdings", summary="Add holding manually")
async def add_holding(payload: HoldingCreate, user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        pid = await ensure_portfolio(conn, user["id"])
        await assert_symbol(conn, payload.symbol)
        duplicate = await conn.fetchval("select true from holdings where portfolio_id = $1 and symbol = $2 and status = 'ACTIVE'", pid, payload.symbol)
        if duplicate:
            raise HTTPException(409, "This symbol already exists as an active holding. Edit the existing holding instead.")
        row = await conn.fetchrow(
            """
            insert into holdings (portfolio_id, symbol, quantity, avg_buy_price, buy_date, requires_confirmation)
            values ($1, $2, $3, $4, $5, false) returning *
            """,
            pid, payload.symbol, payload.quantity, payload.avg_buy_price, payload.buy_date,
        )
    return as_dict(row)


@router.patch("/holdings/{holding_id}", summary="Edit holding")
async def edit_holding(holding_id: int, payload: HoldingUpdate, user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        pid = await ensure_portfolio(conn, user["id"])
        current = await conn.fetchrow("select * from holdings where id = $1 and portfolio_id = $2", holding_id, pid)
        if not current:
            raise HTTPException(404, "Holding not found")
        row = await conn.fetchrow(
            """
            update holdings set
                quantity = coalesce($3, quantity),
                avg_buy_price = coalesce($4, avg_buy_price),
                buy_date = coalesce($5, buy_date),
                requires_confirmation = coalesce($6, requires_confirmation),
                updated_at = now()
            where id = $1 and portfolio_id = $2
            returning *
            """,
            holding_id, pid, payload.quantity, payload.avg_buy_price, payload.buy_date, payload.requires_confirmation,
        )
    return as_dict(row)


@router.delete("/holdings/{holding_id}", summary="Delete holding")
async def delete_holding(holding_id: int, user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        pid = await ensure_portfolio(conn, user["id"])
        deleted = await conn.fetchval("delete from holdings where id = $1 and portfolio_id = $2 returning id", holding_id, pid)
    if not deleted:
        raise HTTPException(404, "Holding not found")
    return {"message": "Holding deleted"}


@router.post("/holdings/{holding_id}/sell", summary="Mark holding as sold")
async def mark_sold(holding_id: int, payload: HoldingSold, user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        pid = await ensure_portfolio(conn, user["id"])
        row = await conn.fetchrow(
            """
            update holdings set status = 'SOLD', sell_date = $3, sell_price = $4, updated_at = now()
            where id = $1 and portfolio_id = $2 and status = 'ACTIVE'
            returning *
            """,
            holding_id, pid, payload.sell_date, payload.sell_price,
        )
    if not row:
        raise HTTPException(404, "Active holding not found")
    return as_dict(row)


@router.post("/import", summary="Import portfolio holdings from broker CSV/XLS/XLSX")
async def import_holdings(file: UploadFile = File(...), user=Depends(current_user), pool=Depends(get_pool)):
    content = await file.read()
    try:
        parsed, warnings = parse_portfolio_file(file.filename or "portfolio.csv", content)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    imported, skipped = [], []
    async with pool.acquire() as conn:
        pid = await ensure_portfolio(conn, user["id"])
        valid_symbols = {r["symbol"] for r in await conn.fetch("select symbol from symbols where is_active = true")}
        for item in parsed:
            if item.symbol not in valid_symbols:
                skipped.append({"symbol": item.symbol, "reason": "Unknown symbol", "row": item.source_row})
                continue
            exists = await conn.fetchval("select true from holdings where portfolio_id = $1 and symbol = $2 and status = 'ACTIVE'", pid, item.symbol)
            if exists:
                skipped.append({"symbol": item.symbol, "reason": "Duplicate active holding", "row": item.source_row})
                continue
            row = await conn.fetchrow(
                """
                insert into holdings (portfolio_id, symbol, quantity, avg_buy_price, buy_date, requires_confirmation, import_source)
                values ($1, $2, $3, $4, $5, $6, $7) returning *
                """,
                pid, item.symbol, item.quantity, item.avg_buy_price, item.buy_date, item.requires_confirmation, file.filename,
            )
            imported.append(as_dict(row))
    return {"imported": imported, "skipped": skipped, "warnings": warnings, "requires_confirmation": [r for r in imported if r.get("requires_confirmation")]}


@router.get("/holdings/{holding_id}/trendline", summary="Holding trendline from buy date to current/sell date")
async def holding_trendline(holding_id: int, user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        pid = await ensure_portfolio(conn, user["id"])
        h = await conn.fetchrow("select * from holdings where id = $1 and portfolio_id = $2", holding_id, pid)
        if not h:
            raise HTTPException(404, "Holding not found")
        end_date = h["sell_date"] or date.today()
        rows = await conn.fetch(
            """
            select trade_date, close_price
            from price_history
            where symbol = $1 and trade_date between $2 and $3
            order by trade_date
            """,
            h["symbol"], h["buy_date"], end_date,
        )
    return {"symbol": h["symbol"], "start_date": h["buy_date"], "end_date": end_date, "data": [as_dict(r) for r in rows]}
