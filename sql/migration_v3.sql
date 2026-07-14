-- ═══════════════════════════════════════════════════════════════════
--  TrendPulse — Migration v3
--  Adds Super Strength columns + removes bool/pct matrix from storage
--  Run in Supabase SQL Editor
--  SAFE: uses ADD COLUMN IF NOT EXISTS — won't break existing data
-- ═══════════════════════════════════════════════════════════════════

-- ── New indicator columns ────────────────────────────────────────────
ALTER TABLE trend_results
  ADD COLUMN IF NOT EXISTS ema_9          numeric(12,4),
  ADD COLUMN IF NOT EXISTS ema_21         numeric(12,4),
  ADD COLUMN IF NOT EXISTS rpi_2w         numeric(5,1),   -- 2-week RPI percentile 1-99
  ADD COLUMN IF NOT EXISTS rpi_3m         numeric(5,1),   -- 3-month RPI percentile 1-99
  ADD COLUMN IF NOT EXISTS rpi_6m         numeric(5,1),   -- 6-month RPI percentile 1-99
  ADD COLUMN IF NOT EXISTS rpi_6m_sma2w   numeric(5,1),   -- 2-week SMA of 6m RPI
  ADD COLUMN IF NOT EXISTS rsi_1d         numeric(6,2),   -- RSI(1) = 1-day momentum
  ADD COLUMN IF NOT EXISTS rsi_1w         numeric(6,2);   -- RSI(5) = 1-week momentum

-- ── Indexes for Super Strength sort ──────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_tr_date_rpi6m
  ON trend_results (trade_date DESC, rpi_6m DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_tr_date_wrpi
  ON trend_results (trade_date DESC, weighted_rpi DESC NULLS LAST)
  WHERE weighted_rpi IS NOT NULL;

-- ── FULL RESET PROCEDURE ─────────────────────────────────────────────
-- Run this block when you want to recompute everything from scratch:
--
--   TRUNCATE trend_results;
--   TRUNCATE sector_daily;
--   TRUNCATE market_calendar;
--   UPDATE symbols SET is_active = true;
--   -- Then run: python scripts/compute_today.py
--
-- ── Verify ───────────────────────────────────────────────────────────
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'trend_results'
  AND column_name IN (
    'ema_9','ema_21','rpi_2w','rpi_3m','rpi_6m',
    'rpi_6m_sma2w','rsi_1d','rsi_1w','weighted_rpi'
  )
ORDER BY column_name;