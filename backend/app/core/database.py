import asyncpg
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def init_db() -> None:
    global _pool
    if not settings.DATABASE_URL:
        logger.warning("DATABASE_URL not set — DB features disabled")
        return
    try:
        _pool = await asyncpg.create_pool(
            settings.DATABASE_URL,
            min_size=settings.DB_MIN_POOL,
            max_size=settings.DB_MAX_POOL,
            statement_cache_size=settings.DB_STATEMENT_CACHE_SIZE,
            command_timeout=60,
            # Serialize datetime objects automatically
            init=_init_connection,
        )
        logger.info("Database pool initialised (%d–%d connections)",
                    settings.DB_MIN_POOL, settings.DB_MAX_POOL)
    except Exception as e:
        logger.error("Database pool failed: %s", e)
        raise


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Run once per new connection — set codec for jsonb."""
    await conn.set_type_codec(
        "jsonb",
        encoder=_json_encode,
        decoder=_json_decode,
        schema="pg_catalog",
        format="text",
    )


def _json_encode(value) -> str:
    import json
    return json.dumps(value, default=str)


def _json_decode(value: str):
    import json
    return json.loads(value)


async def get_pool() -> asyncpg.Pool:
    """FastAPI dependency — injects DB pool into route handlers."""
    if _pool is None:
        raise RuntimeError("Database pool not initialised")
    return _pool
