from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from io import BytesIO
import re
from typing import Any

import pandas as pd

SYMBOL_KEYS = ("symbol", "ticker", "scrip", "security", "instrument", "stock")
QTY_KEYS = ("quantity", "qty", "holding qty", "net qty", "available qty")
PRICE_KEYS = ("average buy price", "avg buy price", "avg price", "average price", "buy price", "cost price")
DATE_KEYS = ("buy date", "purchase date", "acquisition date", "trade date", "date")


@dataclass
class ImportRow:
    symbol: str
    quantity: Decimal
    avg_buy_price: Decimal
    buy_date: date | None
    requires_confirmation: bool
    source_row: int


def _norm_col(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _find_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    normalized = {_norm_col(c): c for c in columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    for col in columns:
        ncol = _norm_col(col)
        if any(key in ncol for key in candidates):
            return col
    return None


def _decimal(value: Any, field: str, row_num: int) -> Decimal:
    if pd.isna(value):
        raise ValueError(f"Row {row_num}: {field} is required")
    cleaned = re.sub(r"[^0-9.\-]", "", str(value))
    try:
        parsed = Decimal(cleaned)
    except (InvalidOperation, ValueError):
        raise ValueError(f"Row {row_num}: invalid {field}")
    if parsed <= 0:
        raise ValueError(f"Row {row_num}: {field} must be greater than zero")
    return parsed


def _date(value: Any) -> date | None:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        raise ValueError(f"Invalid date: {value}")
    return parsed.date()


def parse_portfolio_file(filename: str, content: bytes) -> tuple[list[ImportRow], list[str]]:
    if not content:
        raise ValueError("Uploaded file is empty")
    lower = filename.lower()
    try:
        if lower.endswith(".csv"):
            df = pd.read_csv(BytesIO(content))
        elif lower.endswith((".xlsx", ".xls")):
            df = pd.read_excel(BytesIO(content))
        else:
            raise ValueError("Unsupported file type. Upload CSV, XLS, or XLSX.")
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Could not read the uploaded file. Check that it is a valid broker export.") from exc

    if df.empty:
        raise ValueError("Uploaded file does not contain holdings")
    df = df.dropna(how="all")
    columns = list(df.columns)
    symbol_col = _find_column(columns, SYMBOL_KEYS)
    qty_col = _find_column(columns, QTY_KEYS)
    price_col = _find_column(columns, PRICE_KEYS)
    date_col = _find_column(columns, DATE_KEYS)
    missing = []
    if not symbol_col:
        missing.append("Symbol")
    if not qty_col:
        missing.append("Quantity")
    if not price_col:
        missing.append("Average Buy Price")
    if missing:
        raise ValueError("Missing required columns: " + ", ".join(missing))

    rows: list[ImportRow] = []
    warnings: list[str] = []
    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        raw_symbol = row.get(symbol_col)
        if pd.isna(raw_symbol) or str(raw_symbol).strip() == "":
            continue
        symbol = re.sub(r"[^A-Za-z0-9&\-]", "", str(raw_symbol).strip().upper())
        quantity = _decimal(row.get(qty_col), "quantity", row_num)
        price = _decimal(row.get(price_col), "average buy price", row_num)
        buy_date = None
        if date_col:
            try:
                buy_date = _date(row.get(date_col))
            except ValueError as exc:
                warnings.append(f"Row {row_num}: {exc}")
        requires_confirmation = buy_date is None
        if requires_confirmation:
            warnings.append(f"Row {row_num}: purchase date missing for {symbol}")
        rows.append(ImportRow(symbol, quantity, price, buy_date, requires_confirmation, row_num))
    if not rows:
        raise ValueError("No valid holding rows were found")
    return rows, warnings
