from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # ── Database ─────────────────────────────────────────
    # Supabase: Settings → Database → Connection string → URI mode
    DATABASE_URL: str = ""
    DB_MIN_POOL:  int = 2
    DB_MAX_POOL:  int = 10

    # ── Data paths (used by engine) ───────────────────────
    DATA_FOLDER:       str = ""   # folder with YYYYMMDD_NSE.csv files
    NSE_MASTER_CSV:    str = ""   # EQUITY_L.csv path
    NSE_SECTOR_MASTER: str = ""   # nse_sector_master.csv path

    # ── Security ──────────────────────────────────────────
    # Used to protect POST /api/admin/* endpoints
    # Set to a long random string in production
    ADMIN_SECRET: str = "change-me-in-production"

    # ── App settings ──────────────────────────────────────
    APP_ENV:      str = "production"   # 'development' | 'production'
    LOG_LEVEL:    str = "INFO"
    ALLOWED_ORIGINS: str = "*"         # comma-separated or * for all

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
