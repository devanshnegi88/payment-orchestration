"""Dependency injection — wires all services together."""
import structlog
import redis.asyncio as aioredis
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.config import settings
from app.services.circuit_breaker import CircuitBreakerRegistry
from app.services.router import GatewayRouter, FailoverRouter
from app.services.idempotency import IdempotencyService
from app.services.orchestrator import PaymentOrchestrator
from app.services.webhook import WebhookProcessor
from app.services.reconciliation import ReconciliationEngine
from app.services.audit import AuditService
from app.services.rate_limiter import TokenBucketRateLimiter

logger = structlog.get_logger(__name__)

_redis_pool: aioredis.ConnectionPool | None = None


async def get_redis() -> aioredis.Redis | None:
    global _redis_pool
    if not settings.REDIS_ENABLED:
        logger.info("redis_disabled_in_config")
        return None
    if _redis_pool is None:
        try:
            _redis_pool = aioredis.ConnectionPool.from_url(
                settings.REDIS_URL,
                max_connections=settings.REDIS_MAX_CONNECTIONS,
                decode_responses=False,
                socket_connect_timeout=5,
                socket_keepalive=True,
            )
            logger.info("redis_pool_created", url=settings.REDIS_URL)
        except Exception as e:
            logger.warning("redis_pool_creation_failed", error=str(e), url=settings.REDIS_URL)
            if settings.REDIS_ENABLED:
                raise
            return None
    return aioredis.Redis(connection_pool=_redis_pool)


async def get_cb_registry(redis: aioredis.Redis = Depends(get_redis)) -> CircuitBreakerRegistry:
    return CircuitBreakerRegistry(redis)


async def get_rate_limiter(redis: aioredis.Redis = Depends(get_redis)) -> TokenBucketRateLimiter:
    return TokenBucketRateLimiter(redis)


async def get_audit_service() -> AuditService:
    return AuditService()


async def get_idempotency_service(db: AsyncSession = Depends(get_db)) -> IdempotencyService:
    return IdempotencyService(db)


async def get_gateway_router(
    db: AsyncSession = Depends(get_db),
    cb_registry: CircuitBreakerRegistry = Depends(get_cb_registry),
) -> GatewayRouter:
    return GatewayRouter(db, cb_registry)


async def get_failover_router(
    router: GatewayRouter = Depends(get_gateway_router),
    cb_registry: CircuitBreakerRegistry = Depends(get_cb_registry),
) -> FailoverRouter:
    return FailoverRouter(router, cb_registry)


async def get_orchestrator(
    db: AsyncSession = Depends(get_db),
    cb_registry: CircuitBreakerRegistry = Depends(get_cb_registry),
    failover_router: FailoverRouter = Depends(get_failover_router),
    idempotency: IdempotencyService = Depends(get_idempotency_service),
    audit: AuditService = Depends(get_audit_service),
) -> PaymentOrchestrator:
    return PaymentOrchestrator(db, cb_registry, failover_router, idempotency, audit)


async def get_webhook_processor(db: AsyncSession = Depends(get_db)) -> WebhookProcessor:
    return WebhookProcessor(db)


async def get_reconciliation_engine(db: AsyncSession = Depends(get_db)) -> ReconciliationEngine:
    return ReconciliationEngine(db)
