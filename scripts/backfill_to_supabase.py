#!/usr/bin/env python3
"""
scripts/backfill_to_supabase.py
────────────────────────────────
ONE-TIME script. Run this ONCE to load all your local bhav CSV files
into Supabase price_history. After it completes successfully you can
delete your local CSV folder — the engine never needs local files again.

What it does
────────────
  1. Reads every YYYYMMDD_NSE.csv from LOCAL_BHAV_FOLDER
  2. Filters EQ + BE series
  3. Bulk-upserts every OHLCV row into price_history
  4. Also populates/updates the symbols table with ISIN

Also reads EQUITY_L.csv and nse_sector_master.csv and stores them in
the master_data table in Supabase so the engine can read them from DB
instead of from disk.

Usage
─────
  1. Fill in the three paths at the top of this file
  2. Make sure DATABASE_URL is in backend/.env (or set as env var)
  3. Run from repo root:

       python scripts/backfill_to_supabase.py

  4. Wait ~5-10 minutes for 252 files × 2143 symbols
  5. Delete LOCAL_BHAV_FOLDER when done

Environment
───────────
  DATABASE_URL   — Supabase PostgreSQL connection string
                   (from backend/.env or set in shell)
"""

import asyncio
import logging
import os
import re
import sys
import time
from pathlib import Path

import asyncpg
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────
#  CONFIGURATION — set these three paths before running
# ─────────────────────────────────────────────────────────────────────
LOCAL_BHAV_FOLDER  = r"C:\Users\user-pc\Downloads\200_Bhav"   # folder with YYYYMMDD_NSE.csv
NSE_MASTER_CSV     = r"C:\Users\user-pc\Downloads\EQUITY_L.csv"
NSE_SECTOR_MASTER  = r"C:\Users\user-pc\Downloads\nse_sector_master.csv"
# ─────────────────────────────────────────────────────────────────────

ALLOWED_SERIES = {"EQ", "BE"}
BATCH_SIZE     = 2000   # rows per executemany call

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill")

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "backend" / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "")


# ─────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────

def _sf(val):
    """Safe float — returns None instead of NaN."""
    try:
        v = float(val)
        return None if v != v else v
    except (TypeError, ValueError):
        return None


def _si(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def load_csv_file(fpath: str, trade_date: pd.Timestamp) -> pd.DataFrame:
    """Read one bhav CSV, filter series, return clean DataFrame."""
    try:
        df = pd.read_csv(fpath, dtype=str)
    except Exception as e:
        log.warning("Cannot read %s: %s", fpath, e)
        return pd.DataFrame()

    df.columns = df.columns.str.strip().str.upper()
    required = {"SYMBOL", "SERIES", "HIGH", "LOW", "CLOSE"}
    if not required.issubset(df.columns):
        log.warning("%s missing required columns — skipped", fpath)
        return pd.DataFrame()

    df["SERIES"] = df["SERIES"].str.strip().str.upper()
    df = df[df["SERIES"].isin(ALLOWED_SERIES)].copy()
    if df.empty:
        return df

    for col in ["HIGH", "LOW", "CLOSE"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "TOTTRDQTY" in df.columns:
        df["TOTTRDQTY"] = pd.to_numeric(df["TOTTRDQTY"], errors="coerce")
    if "TOTALTRADES" in df.columns:
        df["TOTALTRADES"] = pd.to_numeric(df["TOTALTRADES"], errors="coerce")

    df["DATE"] = trade_date
    return df


def build_price_rows(df: pd.DataFrame, trade_date) -> list[tuple]:
    """
    Convert a single day's DataFrame into rows ready for
    price_history upsert. Computes prev_close from the previous
    day's CLOSE value within the passed-in full combined DataFrame.
    (We pass prev_close=None here; the engine will fill it from DB
    on subsequent runs. For backfill it's acceptable to leave it NULL
    since the value is only used for display, not for any metric.)
    """
    rows = []
    for _, r in df.iterrows():
        rows.append((
            str(r["SYMBOL"]),
            trade_date,
            _sf(r.get("CLOSE")),   # open_price proxy (bhav has no OPEN)
            _sf(r.get("HIGH")),
            _sf(r.get("LOW")),
            _sf(r.get("CLOSE")),
            _si(r.get("TOTTRDQTY"))   if "TOTTRDQTY"   in r.index else None,
            _si(r.get("TOTALTRADES")) if "TOTALTRADES" in r.index else None,
            None,                  # prev_close — left NULL for backfill
        ))
    return rows


async def upsert_price_batch(conn, rows: list[tuple]) -> None:
    await conn.executemany(
        """
        insert into price_history
            (symbol, trade_date, open_price, high_price, low_price,
             close_price, volume, total_trades, prev_close)
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        on conflict (symbol, trade_date) do update set
            open_price   = excluded.open_price,
            high_price   = excluded.high_price,
            low_price    = excluded.low_price,
            close_price  = excluded.close_price,
            volume       = coalesce(excluded.volume,       price_history.volume),
            total_trades = coalesce(excluded.total_trades, price_history.total_trades)
        """,
        rows,
    )


async def upsert_symbols(conn, isin_map: pd.Series,
                         master_csv: str, sector_csv: str) -> None:
    """Populate symbols table from EQUITY_L + sector master."""
    sym_rows = []

    # Load master CSVs if available
    master_df = pd.DataFrame()
    sector_df = pd.DataFrame()

    if master_csv and Path(master_csv).exists():
        m = pd.read_csv(master_csv, dtype=str)
        m.columns = m.columns.str.strip().str.upper()
        if {"SYMBOL", "NAME OF COMPANY"}.issubset(m.columns):
            m = m.rename(columns={"NAME OF COMPANY": "COMPANY_NAME"})
            if "ISIN NUMBER" in m.columns:
                m = m.rename(columns={"ISIN NUMBER": "ISIN"})
            master_df = m[["SYMBOL"] +
                          [c for c in ["COMPANY_NAME", "ISIN"] if c in m.columns]]
            master_df = master_df.set_index("SYMBOL")
        log.info("NSE master loaded: %d symbols", len(master_df))

    if sector_csv and Path(sector_csv).exists():
        s = pd.read_csv(sector_csv, dtype=str)
        s.columns = s.columns.str.strip().str.upper()
        if "SYMBOL" in s.columns:
            sector_df = s.set_index("SYMBOL")
        log.info("Sector master loaded: %d symbols", len(sector_df))

    VALID_CAP = {"Large Cap", "Mid Cap", "Small Cap"}

    def _clean(val, fallback="") -> str:
        """Coerce any value to str, turning NaN/None/'nan' into fallback."""
        if val is None:
            return fallback
        s = str(val).strip()
        return fallback if s.lower() in ("nan", "none", "") else s

    all_symbols = isin_map.index.tolist()
    for sym in all_symbols:
        company = sym
        isin    = _clean(isin_map.get(sym, ""))
        sector  = ""
        cap     = ""

        if sym in master_df.index:
            row = master_df.loc[sym]
            company = _clean(row.get("COMPANY_NAME"), sym)
            isin    = _clean(row.get("ISIN"), isin)

        if sym in sector_df.index:
            row = sector_df.loc[sym]
            sector = _clean(
                row.get("SECTOR") if "SECTOR" in row.index else row.iloc[0]
            )
            for col in ["CAPCATEGORY", "CAP_CATEGORY", "CAP CATEGORY", "CAP"]:
                if col in sector_df.columns:
                    cap = _clean(sector_df.at[sym, col])
                    break

        # Enforce DB check constraint — only these three values or empty string
        if cap not in VALID_CAP:
            cap = ""

        sym_rows.append((sym, company, isin, sector, cap))

    await conn.executemany(
        """
        insert into symbols (symbol, company_name, isin, sector, cap_category)
        values ($1, $2, $3, $4, $5)
        on conflict (symbol) do update set
            company_name = excluded.company_name,
            isin         = excluded.isin,
            sector       = excluded.sector,
            cap_category = excluded.cap_category,
            is_active    = true,
            updated_at   = now()
        """,
        sym_rows,
    )
    log.info("Symbols upserted: %d", len(sym_rows))


async def store_master_files_in_db(conn,
                                   master_csv: str,
                                   sector_csv: str) -> None:
    """
    Store EQUITY_L.csv and nse_sector_master.csv content in the
    master_data table so the engine never needs local files again.
    """
    # Create table if it doesn't exist
    await conn.execute("""
        create table if not exists master_data (
            key        text primary key,
            content    text not null,
            updated_at timestamptz not null default now()
        )
    """)

    for key, path in [("EQUITY_L", master_csv),
                      ("SECTOR_MASTER", sector_csv)]:
        if path and Path(path).exists():
            content = Path(path).read_text(encoding="utf-8", errors="replace")
            await conn.execute(
                """
                insert into master_data (key, content)
                values ($1, $2)
                on conflict (key) do update set
                    content    = excluded.content,
                    updated_at = now()
                """,
                key, content,
            )
            log.info("Stored %s in master_data (%d chars)",
                     key, len(content))
        else:
            log.warning("File not found, skipping master_data for %s: %s",
                        key, path)


# ─────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────

async def main():
    if not DATABASE_URL:
        log.error("DATABASE_URL not set. Add it to backend/.env")
        sys.exit(1)

    bhav_folder = Path(LOCAL_BHAV_FOLDER)
    if not bhav_folder.exists():
        log.error("LOCAL_BHAV_FOLDER not found: %s", bhav_folder)
        sys.exit(1)

    # Discover all bhav files
    pattern = re.compile(r"^(\d{8})_NSE\.csv$", re.IGNORECASE)
    bhav_files = sorted(
        (f for f in bhav_folder.iterdir() if pattern.match(f.name)),
        key=lambda f: f.name,
    )
    if not bhav_files:
        log.error("No YYYYMMDD_NSE.csv files found in %s", bhav_folder)
        sys.exit(1)

    log.info("=" * 60)
    log.info("  TrendPulse — Backfill to Supabase")
    log.info("=" * 60)
    log.info("  Bhav folder : %s", bhav_folder)
    log.info("  Files found : %d", len(bhav_files))
    log.info("  First       : %s", bhav_files[0].name)
    log.info("  Last        : %s", bhav_files[-1].name)
    log.info("=" * 60)

    # Connect
    log.info("Connecting to Supabase...")
    pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=8, command_timeout=120,
    )

    t0 = time.monotonic()
    total_rows = 0

    async with pool.acquire() as conn:

        # ── 1. Store master files in DB ───────────────────────────────
        log.info("Storing master files in Supabase master_data table...")
        await store_master_files_in_db(conn, NSE_MASTER_CSV, NSE_SECTOR_MASTER)

        # ── 2. Scan ALL bhav files first to collect every symbol+ISIN ─
        # price_history has a FK → symbols, so symbols MUST exist first.
        log.info("Scanning all bhav files to collect symbols (pass 1/2)...")
        isin_map = {}
        for fpath in bhav_files:
            try:
                df_scan = pd.read_csv(str(fpath), dtype=str,
                                      usecols=lambda c: c.strip().upper()
                                                         in {"SYMBOL","SERIES","ISIN"})
            except Exception:
                continue
            df_scan.columns = df_scan.columns.str.strip().str.upper()
            if "SERIES" in df_scan.columns:
                df_scan = df_scan[df_scan["SERIES"].str.strip().str.upper()
                                  .isin(ALLOWED_SERIES)]
            if "ISIN" in df_scan.columns:
                for _, row in df_scan.dropna(subset=["ISIN"]).iterrows():
                    isin_map[str(row["SYMBOL"]).strip()] = str(row["ISIN"]).strip()
            else:
                for sym in df_scan["SYMBOL"].dropna().unique():
                    isin_map.setdefault(str(sym).strip(), "")

        log.info("Symbols found across all files: %d", len(isin_map))

        # ── 3. Upsert ALL symbols before touching price_history ───────
        log.info("Upserting symbols (must happen before price_history)...")
        isin_series = pd.Series(isin_map)
        await upsert_symbols(
            conn, isin_series, NSE_MASTER_CSV, NSE_SECTOR_MASTER
        )
        
        # ── 4. Get dates already in Supabase — skip them ──────────────
        log.info("Fetching dates already in Supabase...")
        existing_dates = set()
        rows_existing = await conn.fetch(
            "SELECT DISTINCT trade_date FROM price_history"
        )
        for r in rows_existing:
            existing_dates.add(r["trade_date"])
        log.info("Already in Supabase: %d dates", len(existing_dates))

# ── 5. Now upsert only MISSING dates ──────────────────────────
        log.info("Starting price_history upsert (pass 2/2 — new dates only)...")
        skipped = 0
        for i, fpath in enumerate(bhav_files, 1):
            m     = pattern.match(fpath.name)
            dstr  = m.group(1)
            tdate = pd.to_datetime(dstr, format="%Y%m%d").date()

            # ← KEY CHANGE: skip if already uploaded
            if tdate in existing_dates:
                skipped += 1
                continue

            df = load_csv_file(str(fpath), pd.Timestamp(tdate))
            if df.empty:
                log.info("[%d/%d] %s — no EQ+BE rows, skipped",
                        i, len(bhav_files), fpath.name)
                continue

            rows = build_price_rows(df, tdate)
            for start in range(0, len(rows), BATCH_SIZE):
                await upsert_price_batch(conn, rows[start:start + BATCH_SIZE])

            total_rows += len(rows)
            log.info("[%d/%d] %s → %d rows (total so far: %d)",
                    i, len(bhav_files), fpath.name, len(rows), total_rows)

        log.info("Skipped %d dates already in Supabase", skipped)

        # ── 4. Now upsert price_history — FK constraint satisfied ─────
        
    elapsed = time.monotonic() - t0

    log.info("=" * 60)
    log.info("  BACKFILL COMPLETE")
    log.info("  Files processed : %d", len(bhav_files))
    log.info("  Price rows      : %d", total_rows)
    log.info("  Time            : %.1fs", elapsed)
    log.info("=" * 60)
    log.info("")
    log.info("Next steps:")
    log.info("  1. Verify: psql $DATABASE_URL -c 'select count(*) from price_history'")
    log.info("  2. Run the engine once: python scripts/run_engine_cli.py")
    log.info("  3. Once engine run succeeds → delete LOCAL_BHAV_FOLDER")
    log.info("  4. Remove DATA_FOLDER from backend/.env and GitHub secrets")
    log.info("  5. Remove EQUITY_L_B64 and SECTOR_MASTER_B64 from GitHub secrets")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())