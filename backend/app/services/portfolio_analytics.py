"""
Trader-grade portfolio analytics: XIRR, concentration/diversification risk,
and an approximate Indian equity capital-gains tax estimate.

These are computed from data already in `holdings`/`trend_results` — no new
tables required.
"""
from __future__ import annotations

from datetime import date
from typing import Any

# ── XIRR (money-weighted return) ────────────────────────────────────────
# Accounts for *when* each buy happened, unlike simple gain% which treats
# every rupee as invested for the same length of time.

def _npv(rate: float, cashflows: list[tuple[date, float]], today: date) -> float:
    total = 0.0
    for when, amount in cashflows:
        days = (when - today).days
        total += amount / ((1 + rate) ** (days / 365.0))
    return total


def xirr(cashflows: list[tuple[date, float]]) -> float | None:
    """cashflows: list of (date, amount). Negative = money out (a buy), positive = money in
    (current value, treated as a hypothetical sale today). Returns annualized % or None
    if it can't be solved (e.g. all flows same sign, or no data)."""
    if len(cashflows) < 2:
        return None
    if not any(a < 0 for _, a in cashflows) or not any(a > 0 for _, a in cashflows):
        return None
    today = max(d for d, _ in cashflows)

    rate = 0.15  # initial guess: 15%
    for _ in range(100):
        npv = _npv(rate, cashflows, today)
        # numerical derivative
        h = 1e-5
        d_npv = (_npv(rate + h, cashflows, today) - npv) / h
        if abs(d_npv) < 1e-12:
            break
        new_rate = rate - npv / d_npv
        if new_rate <= -0.99:
            new_rate = -0.5
        if abs(new_rate - rate) < 1e-7:
            rate = new_rate
            break
        rate = new_rate
    if rate <= -0.99 or rate > 100 or rate != rate:  # NaN guard
        return None
    return round(rate * 100, 2)


def portfolio_xirr(active_holdings: list[dict[str, Any]]) -> float | None:
    cashflows: list[tuple[date, float]] = []
    today = date.today()
    for h in active_holdings:
        buy_date = h.get("buy_date")
        investment = h.get("investment_amount")
        current_value = h.get("current_value")
        if not buy_date or investment is None or current_value is None:
            continue
        cashflows.append((buy_date, -float(investment)))
    total_current_value = sum(float(h.get("current_value") or 0) for h in active_holdings)
    if total_current_value > 0:
        cashflows.append((today, total_current_value))
    return xirr(cashflows)


# ── Concentration / diversification risk ────────────────────────────────

def concentration_risk(active_holdings: list[dict[str, Any]]) -> dict[str, Any]:
    """Herfindahl-Hirschman Index on position weights, plus a friendlier 0-100 score
    and the single biggest position/sector for a quick risk flag."""
    values = [float(h.get("current_value") or 0) for h in active_holdings]
    total = sum(values)
    if total <= 0 or not values:
        return {"hhi": None, "diversification_score": None, "top_position_pct": None, "top_sector_pct": None}

    weights = [v / total for v in values]
    hhi = sum(w * w for w in weights)
    diversification_score = round((1 - hhi) * 100, 1)
    top_position_pct = round(max(weights) * 100, 1)

    sector_totals: dict[str, float] = {}
    for h in active_holdings:
        sector = h.get("sector") or "Unclassified"
        sector_totals[sector] = sector_totals.get(sector, 0) + float(h.get("current_value") or 0)
    top_sector_pct = round(max(sector_totals.values()) / total * 100, 1) if sector_totals else None

    return {
        "hhi": round(hhi, 4),
        "diversification_score": diversification_score,
        "top_position_pct": top_position_pct,
        "top_sector_pct": top_sector_pct,
        "flag": "concentrated" if top_position_pct > 25 or (top_sector_pct or 0) > 40 else "diversified",
    }


# ── Approximate Indian equity capital-gains tax estimate ─────────────────
# Rules used (post July-2024 budget, STT-paid listed equity):
#   Short-term (< 365 days held):  20% flat, no exemption
#   Long-term  (>= 365 days held): 12.5%, first ₹1,25,000 of aggregate LTCG per year exempt
# This is an approximation for planning purposes only, not tax advice — rates,
# exemptions, and holding-period rules can change and don't account for other
# income, prior losses, or non-equity holdings.
LTCG_EXEMPTION = 125_000
LTCG_RATE = 0.125
STCG_RATE = 0.20


def tax_estimate(active_holdings: list[dict[str, Any]]) -> dict[str, Any]:
    stcg_gain = 0.0
    ltcg_gain = 0.0
    for h in active_holdings:
        pnl = h.get("profit_loss")
        days_held = h.get("days_held")
        if pnl is None or pnl <= 0 or days_held is None:
            continue  # tax only applies to gains; losses can offset but that's beyond this estimate
        if days_held >= 365:
            ltcg_gain += float(pnl)
        else:
            stcg_gain += float(pnl)

    taxable_ltcg = max(0.0, ltcg_gain - LTCG_EXEMPTION)
    ltcg_tax = taxable_ltcg * LTCG_RATE
    stcg_tax = stcg_gain * STCG_RATE

    return {
        "stcg_unrealized_gain": round(stcg_gain, 2),
        "ltcg_unrealized_gain": round(ltcg_gain, 2),
        "ltcg_exemption_used": round(min(ltcg_gain, LTCG_EXEMPTION), 2),
        "estimated_stcg_tax": round(stcg_tax, 2),
        "estimated_ltcg_tax": round(ltcg_tax, 2),
        "estimated_total_tax_if_sold_today": round(stcg_tax + ltcg_tax, 2),
        "disclaimer": "Approximate, unrealized-gain estimate only — not tax advice. "
                      "Ignores losses on other positions, other income, and rule changes.",
    }
