"""
Token Bucket Rate Limiter — Redis-backed, per-gateway.

Each gateway has its own bucket with documented rate limits:
  Razorpay: 200 req/sec
  Stripe:   100 req/sec
  PayU:     150 req/sec
  UPI:      100 req/sec

Algorithm: token bucket with Redis atomic INCR + TTL.
- Each bucket refills to capacity every second.
- Requests consume one token; if empty, raise GatewayRateLimitError.
- Queuing: caller should retry after 1s (not drop the request).

Redis key: ratelimit:{gateway}:{epoch_second}
TTL: 2 seconds (allows current + previous second window to coexist safely)
"""
import time
from typing import Optional
import structlog
import redis.asyncio as aioredis

from app.domain.exceptions import GatewayRateLimitError

logger = structlog.get_logger(__name__)

# Gateway rate limits in requests per second
GATEWAY_LIMITS: dict[str, int] = {
    "razorpay": 200,
    "stripe":   100,
    "payu":     150,
    "upi":      100,
}


class TokenBucketRateLimiter:
    """
    Per-second token bucket using Redis atomic counters.
    Thread-safe and distributed across all API server instances.
    """

    def __init__(self, redis: aioredis.Redis | None):
        self._redis = redis

    async def acquire(self, gateway: str) -> None:
        """
        Attempt to consume one token for the given gateway.
        Raises GatewayRateLimitError if the bucket is empty.
        """
        # Skip rate limiting if Redis is disabled
        if self._redis is None:
            logger.debug("rate_limiting_disabled", gateway=gateway)
            return

        limit = GATEWAY_LIMITS.get(gateway, 100)
        epoch_sec = int(time.time())
        key = f"ratelimit:{gateway}:{epoch_sec}"

        try:
            # Atomically increment and get the new count
            count = await self._redis.incr(key)

            if count == 1:
                # First request in this second — set TTL so Redis auto-cleans
                await self._redis.expire(key, 2)

            if count > limit:
                # Bucket exhausted — calculate wait time
                retry_after = 1.0 - (time.time() - epoch_sec)
                logger.warning(
                    "rate_limit_exceeded",
                    gateway=gateway,
                    count=count,
                    limit=limit,
                    retry_after_ms=int(retry_after * 1000),
                )
                raise GatewayRateLimitError(gateway, max(retry_after, 0.1))
        except GatewayRateLimitError:
            # Re-raise rate limit errors
            raise
        except Exception as e:
            # Log Redis connection error but allow request to proceed
            logger.warning(
                "rate_limiter_unavailable",
                gateway=gateway,
                error=str(e),
                action="allowing_request_without_rate_limiting",
            )

    async def get_utilization(self, gateway: str) -> dict:
        """Return current utilization for monitoring endpoint."""
        limit = GATEWAY_LIMITS.get(gateway, 100)
        
        # Return offline status if Redis is disabled
        if self._redis is None:
            return {
                "gateway": gateway,
                "current_rps": 0,
                "limit_rps": limit,
                "utilization_pct": 0,
                "status": "rate_limiting_disabled",
            }
        
        epoch_sec = int(time.time())
        key = f"ratelimit:{gateway}:{epoch_sec}"
        try:
            count_raw = await self._redis.get(key)
            count = int(count_raw) if count_raw else 0
        except Exception as e:
            logger.error(
                "redis_connection_failed",
                gateway=gateway,
                error=str(e),
            )
            # Return unavailable status rather than crashing
            count = 0
        return {
            "gateway": gateway,
            "current_rps": count,
            "limit_rps": limit,
            "utilization_pct": round(count / limit * 100, 1),
        }

    async def get_all_utilizations(self) -> list[dict]:
        try:
            return [await self.get_utilization(gw) for gw in GATEWAY_LIMITS]
        except Exception as e:
            logger.error("failed_to_get_all_utilizations", error=str(e))
            # Return offline status for all gateways
            return [
                {
                    "gateway": gw,
                    "current_rps": 0,
                    "limit_rps": limit,
                    "utilization_pct": 0,
                    "status": "redis_unavailable",
                }
                for gw, limit in GATEWAY_LIMITS.items()
            ]
