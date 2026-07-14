"""
downloader.py
─────────────
Downloads NSE Bhav Copy CSV for a given trade date.

Strategy (two-tier with automatic fallback)
───────────────────────────────────────────
PRIMARY   — `nse` PyPI package  (pip install nse)
              Handles session cookies, retries, holiday detection.
              Returns None automatically on market holidays.

FALLBACK  — Direct NSE archives URL (no library required)
              https://archives.nseindia.com/products/content/
              sec_bhavdata_full_DDMMYYYY.csv
              Simpler, but occasionally rate-limited by NSE.

Both methods produce the same CSV column layout:
    SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, LAST,
    PREVCLOSE, TOTTRDQTY, TOTTRDVAL, TIMESTAMP,
    TOTALTRADES, ISIN

The file is normalised and saved as  YYYYMMDD_NSE.csv  in DATA_FOLDER,
exactly matching the naming convention the analytics engine expects.

Usage
─────
    from app.services.downloader import download_bhav
    path = await download_bhav(datetime.date(2026, 3, 13), "/tmp/bhav")

Install
───────
    pip install nse httpx
"""
import asyncio
import csv
import datetime
import io
import logging
import zipfile
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── NSE archives fallback URL ─────────────────────────────────────────
# Older full bhavcopy — reliable, no session cookie needed.
# Format:  sec_bhavdata_full_DDMMYYYY.csv
_NSE_ARCHIVE_URL = (
    "https://archives.nseindia.com/products/content/"
    "sec_bhavdata_full_{date_str}.csv"
)

# Bhav fetch from the live NSE reports API (used in httpx fallback)
_NSE_HOME      = "https://www.nseindia.com"
_NSE_BHAV_URL  = (
    "https://www.nseindia.com/api/reports"
    "?archives=%5B%7B%22name%22%3A%22CM%20-%20Bhavcopy(csv)%22"
    "%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22"
    "%2C%22section%22%3A%22equities%22%7D%5D"
    "&date={date_str}&type=equities&mode=single"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/market-data/bhavcopy-eod",
    "Connection":      "keep-alive",
}

# Minimum CSV size — anything smaller is an error page or empty response
_MIN_CSV_BYTES = 10_000


# ─────────────────────────────────────────────────────────────────────
#  PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────

async def download_bhav(
    trade_date:  datetime.date,
    data_folder: str,
    overwrite:   bool = False,
) -> Optional[str]:
    """
    Download the NSE bhav copy for `trade_date`.

    Tries the `nse` PyPI package first; falls back to NSE archives URL;
    falls back to NSE live API (cookie-based) as last resort.

    Saves as  {data_folder}/YYYYMMDD_NSE.csv

    Returns
    -------
    str   — path to saved file on success
    None  — market was closed (holiday/weekend) or all methods failed
    """
    out_name = trade_date.strftime("%Y%m%d") + "_NSE.csv"
    out_path = Path(data_folder) / out_name
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not overwrite:
        logger.info("[downloader] Already exists: %s", out_path)
        return str(out_path)

    logger.info("[downloader] Fetching bhav for %s ...", trade_date)

    # ── Method 1: nse PyPI package ────────────────────────────────────
    content = await _try_nse_package(trade_date)

    # ── Method 2: NSE archives (direct CSV, no cookie) ────────────────
    if content is None:
        logger.info("[downloader] nse package failed — trying archives URL")
        content = await _try_archive_url(trade_date)

    # ── Method 3: NSE live API (cookie-based, fragile) ────────────────
    if content is None:
        logger.info("[downloader] archive URL failed — trying live NSE API")
        content = await _try_live_api(trade_date)

    if content is None:
        logger.warning(
            "[downloader] All methods failed for %s — likely a holiday", trade_date
        )
        return None

    out_path.write_bytes(content)
    logger.info("[downloader] Saved: %s  (%d bytes)", out_path, len(content))
    return str(out_path)


async def download_bhav_bytes(
    trade_date: datetime.date,
) -> Optional[bytes]:
    """
    Download today\'s bhav CSV and return it as raw bytes.
    Nothing is written to disk.

    This is the primary entry point used by the DB-native engine.
    The bytes are parsed in memory and inserted directly into Supabase.

    Returns
    -------
    bytes  -- raw CSV content on success
    None   -- market was closed (holiday/weekend) or all methods failed
    """
    logger.info("[downloader] Fetching bhav bytes for %s ...", trade_date)

    content = await _try_nse_package(trade_date)

    if content is None:
        logger.info("[downloader] nse package failed -- trying archives URL")
        content = await _try_archive_url(trade_date)

    if content is None:
        logger.info("[downloader] archive URL failed -- trying live NSE API")
        content = await _try_live_api(trade_date)

    if content is None:
        logger.warning(
            "[downloader] All methods failed for %s -- likely a holiday", trade_date
        )
        return None

    logger.info("[downloader] Got %d bytes for %s", len(content), trade_date)
    return content


async def download_bhav_range(
    start_date:  datetime.date,
    end_date:    datetime.date,
    data_folder: str,
    overwrite:   bool = False,
) -> list[str]:
    """
    Download bhav files for all weekdays in [start_date, end_date].
    Returns list of successfully saved file paths.
    (Kept for the offline analytics_engine.py Excel workflow.)
    """
    saved   = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:   # Mon–Fri only
            path = await download_bhav(current, data_folder, overwrite)
            if path:
                saved.append(path)
            await asyncio.sleep(1.5)   # be polite to NSE servers
        current += datetime.timedelta(days=1)
    logger.info("[downloader] Downloaded %d files", len(saved))
    return saved


# ─────────────────────────────────────────────────────────────────────
#  METHOD 1 — nse PyPI package
# ─────────────────────────────────────────────────────────────────────

async def _try_nse_package(trade_date: datetime.date) -> Optional[bytes]:
    """
    Use the `nse` PyPI package (pip install nse) to fetch the bhav copy.

    The NSE class handles session cookies, retries, and holiday detection
    automatically — it returns None on holidays/weekends.

    We run the synchronous nse.equityBhavcopy() in a thread pool to avoid
    blocking the async event loop.
    """
    try:
        import asyncio
        from nse import NSE          # pip install nse
        from datetime import datetime as dt

        def _sync_fetch():
            # NSE() needs a writable folder for its internal cache
            with NSE("/tmp/nse_cache") as nse:
                # equityBhavcopy returns a pandas DataFrame or None on holiday
                df = nse.equityBhavcopy(
                    date=dt.combine(trade_date, dt.min.time())
                )
            return df

        loop = asyncio.get_event_loop()
        df   = await loop.run_in_executor(None, _sync_fetch)

        if df is None or df.empty:
            logger.debug("[downloader:nse] No data returned (holiday?)")
            return None

        # Normalise column names to uppercase to match engine expectations
        df.columns = df.columns.str.strip().str.upper()

        # The nse package uses TIMESTAMP for the date — rename to DATE
        # so our bhav loader can read it if needed (we save as-is anyway)
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        if len(csv_bytes) < _MIN_CSV_BYTES:
            logger.debug("[downloader:nse] CSV too small (%d bytes)", len(csv_bytes))
            return None

        logger.info("[downloader:nse] OK (%d rows, %d bytes)",
                    len(df), len(csv_bytes))
        return csv_bytes

    except ImportError:
        logger.debug("[downloader:nse] `nse` package not installed — skipping")
        return None
    except Exception as e:
        logger.debug("[downloader:nse] Failed: %s", e)
        return None


# ─────────────────────────────────────────────────────────────────────
#  METHOD 2 — NSE Archives URL (direct download, no cookie)
# ─────────────────────────────────────────────────────────────────────

async def _try_archive_url(trade_date: datetime.date) -> Optional[bytes]:
    """
    Download from NSE archives:
        https://archives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv

    This URL serves the full EQ bhavcopy without needing a session cookie.
    It is occasionally rate-limited (~50 req/day per IP) but works well
    in GitHub Actions where the IP changes daily.
    """
    date_str = trade_date.strftime("%d%m%Y")   # e.g. 13032026
    url      = _NSE_ARCHIVE_URL.format(date_str=date_str)

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            r = await client.get(url)

        if r.status_code == 200 and len(r.content) >= _MIN_CSV_BYTES:
            logger.info("[downloader:archive] OK (%d bytes)", len(r.content))
            return r.content
        elif r.status_code == 404:
            logger.debug("[downloader:archive] 404 — holiday or no data")
            return None
        else:
            logger.debug("[downloader:archive] HTTP %d", r.status_code)
            return None

    except Exception as e:
        logger.debug("[downloader:archive] Failed: %s", e)
        return None


# ─────────────────────────────────────────────────────────────────────
#  METHOD 3 — Live NSE API (cookie-based, last resort)
# ─────────────────────────────────────────────────────────────────────

async def _try_live_api(trade_date: datetime.date) -> Optional[bytes]:
    """
    Hit NSE's live reports API with a browser-spoofed session cookie.
    Fragile — NSE may add bot detection at any time — used only as
    last resort when both nse package and archives URL have failed.
    """
    date_str = trade_date.strftime("%d%b%Y").upper()   # e.g. 13MAR2026
    url      = _NSE_BHAV_URL.format(date_str=date_str)

    try:
        async with httpx.AsyncClient(
            headers=_HEADERS,
            follow_redirects=True,
            timeout=30,
        ) as client:
            await client.get(_NSE_HOME)        # get session cookie
            await asyncio.sleep(1)
            r = await client.get(url)

        if r.status_code != 200 or len(r.content) < _MIN_CSV_BYTES:
            logger.debug("[downloader:live] HTTP %d or too small", r.status_code)
            return None

        content = r.content

        # NSE sometimes returns a ZIP — extract the CSV inside
        if content[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                csv_names = [n for n in zf.namelist()
                             if n.upper().endswith(".CSV")]
                if not csv_names:
                    logger.debug("[downloader:live] Zip has no CSV: %s",
                                 zf.namelist())
                    return None
                content = zf.read(csv_names[0])

        if len(content) >= _MIN_CSV_BYTES:
            logger.info("[downloader:live] OK (%d bytes)", len(content))
            return content

        return None

    except Exception as e:
        logger.debug("[downloader:live] Failed: %s", e)
        return None