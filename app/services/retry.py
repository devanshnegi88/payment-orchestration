"""
Retry Engine — exponential backoff with jitter + retry classification.

Not every failure should be retried. Classification:
  RETRYABLE:     Gateway 5xx, timeout, rate limit (transient — retry helps)
  NOT_RETRYABLE: Hard decline, invalid card, fraud block (retrying wastes money)
  FAILOVER:      Gateway down, circuit open (retry on a DIFFERENT gateway)
  ABORT:         Amount mismatch, invalid state (bug — stop immediately)

Backoff formula: min(cap, base * 2^attempt) + random jitter
  base=1s, cap=8s, jitter=0..1s
  Attempts: 1s, 2s±jitter, 4s±jitter, 8s±jitter (max 3 retries)
"""
import asyncio
import random
from enum import Enum
from typing import TypeVar, Callable, Awaitable, Optional, Type

import structlog

from app.domain.exceptions import (
    GatewayError,
    GatewayTimeoutError,
    GatewayServerError,
    GatewayRateLimitError,
    GatewayDeclineError,
    GatewayUnavailableError,
)

logger = structlog.get_logger(__name__)
T = TypeVar("T")


class RetryClass(str, Enum):
    RETRYABLE  = "RETRYABLE"    # same gateway, exponential backoff
    FAILOVER   = "FAILOVER"     # different gateway needed
    ABORT      = "ABORT"        # don't retry at all


def classify(exc: Exception) -> RetryClass:
    """Classify an exception to determine retry strategy."""
    if isinstance(exc, GatewayDeclineError):
        return RetryClass.ABORT           # hard decline — retrying wastes funds
    if isinstance(exc, GatewayUnavailableError):
        return RetryClass.FAILOVER        # circuit open — need different gateway
    if isinstance(exc, GatewayTimeoutError):
        return RetryClass.FAILOVER        # timeout → try another gateway fast
    if isinstance(exc, GatewayServerError):
        if exc.status_code in (500, 502, 503, 504):
            return RetryClass.RETRYABLE   # transient server error — same gateway
        return RetryClass.ABORT           # 4xx from gateway → bug, stop
    if isinstance(exc, GatewayRateLimitError):
        return RetryClass.RETRYABLE       # rate limited — wait then retry same
    return RetryClass.ABORT


def _backoff_seconds(attempt: int, base: float = 1.0, cap: float = 8.0) -> float:
    """Exponential backoff with full jitter (AWS recommendation)."""
    exponential = min(cap, base * (2 ** attempt))
    return random.uniform(0, exponential)


async def with_retry(
    fn: Callable[[], Awaitable[T]],
    gateway: str,
    operation: str,
    max_retries: int = 3,
) -> T:
    """
    Execute fn with retries for RETRYABLE errors.
    FAILOVER and ABORT errors are re-raised immediately to the orchestrator.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        try:
            result = await fn()
            if attempt > 0:
                logger.info("retry_succeeded", gateway=gateway, operation=operation,
                            attempt=attempt)
            return result

        except GatewayError as exc:
            last_exc = exc
            rc = classify(exc)

            if rc == RetryClass.ABORT:
                logger.info("retry_aborted_not_retryable",
                            gateway=gateway, operation=operation,
                            error_type=type(exc).__name__)
                raise

            if rc == RetryClass.FAILOVER:
                logger.warning("retry_requires_failover",
                               gateway=gateway, operation=operation,
                               error_type=type(exc).__name__)
                raise  # Let orchestrator handle failover

            # RETRYABLE — wait and retry same gateway
            if attempt >= max_retries:
                logger.error("retry_max_attempts_exceeded",
                             gateway=gateway, operation=operation, attempts=attempt + 1)
                raise

            delay = _backoff_seconds(attempt)
            if isinstance(exc, GatewayRateLimitError):
                # Honour the gateway's Retry-After
                delay = max(delay, exc.retry_after_seconds)

            logger.warning("retry_attempt",
                           gateway=gateway, operation=operation,
                           attempt=attempt + 1, delay_s=round(delay, 2),
                           error=str(exc))
            await asyncio.sleep(delay)

    raise last_exc  # unreachable but satisfies type checker
