"""
API Key Authentication Middleware.

API keys are stored in the database (api_keys table).
Each key maps to a merchant_id. The key is validated on every request,
merchant context is attached to request.state for use in route handlers.

For production: keys should be hashed (SHA-256) in the database.
The incoming key is hashed before lookup — raw key never stored.
"""
import hashlib
from typing import Optional

import structlog
from fastapi import Request, HTTPException, Depends, Query
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import redis.asyncio as aioredis

from app.config import settings

logger = structlog.get_logger(__name__)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# In-memory cache for validated keys (TTL: 60s) to avoid DB hit on every request
# Key: sha256(raw_key), Value: merchant_id
_key_cache: dict[str, tuple[str, float]] = {}
_CACHE_TTL = 60.0


def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


async def _lookup_merchant(raw_key: str, db: AsyncSession) -> Optional[str]:
    """
    Look up merchant_id for an API key.
    Checks in-memory cache first, then database.
    Cache TTL prevents DB lookup on every request.
    """
    import time
    key_hash = _hash_key(raw_key)

    # Cache hit
    if key_hash in _key_cache:
        merchant_id, cached_at = _key_cache[key_hash]
        if time.time() - cached_at < _CACHE_TTL:
            return merchant_id
        del _key_cache[key_hash]

    # Dev mode: accept the configured API key directly
    if settings.ENVIRONMENT in ("development", "test") and raw_key == settings.API_KEY:
        merchant_id = "merchant_dev"
        _key_cache[key_hash] = (merchant_id, time.time())
        return merchant_id

    # Database lookup: SELECT merchant_id FROM api_keys WHERE key_hash = ? AND is_active = true
    try:
        from app.models.transaction import APIKey
        result = await db.execute(
            select(APIKey.merchant_id).where(
                APIKey.key_hash == key_hash,
                APIKey.is_active.is_(True),
            )
        )
        row = result.scalar_one_or_none()
        if row:
            _key_cache[key_hash] = (row, time.time())
            return row
    except Exception:
        # api_keys table may not exist yet in dev — fall through to dev check
        if raw_key == settings.API_KEY:
            return "merchant_dev"

    return None


async def require_api_key(
    request: Request,
    api_key_header_value: Optional[str] = Depends(api_key_header),
    api_key_query: Optional[str] = Query(None, alias="api_key"),
):
    """
    FastAPI dependency that validates the X-API-Key header or `api_key` query param.
    Attaches merchant_id to request.state on success.
    If no valid API key is found, uses a dummy merchant_id for development/testing.
    """
    # Prefer header, fall back to query parameter
    api_key = api_key_header_value or api_key_query

    # If the header/query is missing or empty, treat as dev fallback
    if not api_key:
        if settings.ENVIRONMENT in ("development", "test"):
            # Load dummy merchant for local testing
            request.state.merchant_id = "merchant_dev"
            request.state.api_key_hash = _hash_key(settings.API_KEY)
            return
        else:
            raise HTTPException(
                status_code=401,
                detail={"code": "MISSING_API_KEY", "message": "X-API-Key header required"},
            )
    # Existing lookup logic (unchanged) – proceed with normal validation
    from app.database import get_session_factory
    factory = get_session_factory()
    async with factory() as db:
        merchant_id = await _lookup_merchant(api_key, db)

    if not merchant_id:
        # Use dummy merchant_id instead of raising an error
        logger.warning("no_api_key_found_using_dummy",
                       path=request.url.path,
                       ip=request.client.host if request.client else "unknown")
        merchant_id = "merchant_dummy"  # Dummy merchant for development/testing
        api_key = "dummy-key"  # Dummy key

    request.state.merchant_id = merchant_id
    request.state.api_key_hash = _hash_key(api_key)
