"""
Redis-backed circuit breaker with Prometheus metrics on every state change.

Redis can be temporarily disabled. When Redis is unavailable/disabled,
the circuit breaker operates in bypass mode:
    - is_open() -> False
    - record_success() -> no-op
    - record_failure() -> no-op
    - get_status() -> CLOSED / healthy

When Redis is enabled, the normal distributed circuit-breaker behavior
is preserved.
"""

import time
from enum import Enum
from typing import Optional
from dataclasses import dataclass

import structlog
import redis.asyncio as aioredis

from app.config import settings


logger = structlog.get_logger(__name__)


class CircuitBreakerState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: int = 30
    half_open_max_calls: int = 1


@dataclass
class CircuitBreakerStatus:
    state: CircuitBreakerState
    failure_count: int
    success_count: int
    last_failure_time: Optional[float]
    last_state_change: float
    gateway: str
    payment_method: Optional[str]

    @property
    def health_score(self) -> float:
        return {
            CircuitBreakerState.CLOSED: 1.0,
            CircuitBreakerState.HALF_OPEN: 0.5,
            CircuitBreakerState.OPEN: 0.0,
        }[self.state]


class CircuitBreaker:
    def __init__(
        self,
        redis: Optional[aioredis.Redis],
        gateway: str,
        payment_method: Optional[str] = None,
        config: Optional[CircuitBreakerConfig] = None,
    ):
        self.redis = redis
        self.gateway = gateway
        self.payment_method = payment_method or "all"

        self.config = config or CircuitBreakerConfig(
            failure_threshold=settings.CB_FAILURE_THRESHOLD,
            success_threshold=settings.CB_SUCCESS_THRESHOLD,
            timeout_seconds=settings.CB_TIMEOUT_SECONDS,
            half_open_max_calls=settings.CB_HALF_OPEN_MAX_CALLS,
        )

        self._p = f"cb:{gateway}:{self.payment_method}"

    # ------------------------------------------------------------------
    # Redis helpers
    # ------------------------------------------------------------------

    async def _get(self, suffix: str):
        """
        Get a Redis value.

        When Redis is disabled, return None instead of crashing.
        """
        if self.redis is None:
            return None

        return await self.redis.get(
            f"{self._p}:{suffix}"
        )

    async def _set(
        self,
        suffix: str,
        value: str,
        ttl: Optional[int] = None,
    ) -> None:
        """
        Set a Redis value.

        No-op when Redis is disabled.
        """
        if self.redis is None:
            return

        key = f"{self._p}:{suffix}"

        await self.redis.set(
            key,
            value,
        )

        if ttl:
            await self.redis.expire(
                key,
                ttl,
            )

    async def _incr(
        self,
        suffix: str,
        ttl: int = 600,
    ) -> int:
        """
        Increment a Redis counter.

        Returns 0 when Redis is disabled.
        """
        if self.redis is None:
            return 0

        key = f"{self._p}:{suffix}"

        count = await self.redis.incr(
            key
        )

        await self.redis.expire(
            key,
            ttl,
        )

        return count

    async def _del(
        self,
        *suffixes: str,
    ) -> None:
        """
        Delete Redis keys.

        No-op when Redis is disabled.
        """
        if self.redis is None:
            return

        keys = [
            f"{self._p}:{suffix}"
            for suffix in suffixes
        ]

        if keys:
            await self.redis.delete(
                *keys
            )

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    async def _state(self) -> CircuitBreakerState:
        """
        Get current circuit-breaker state.

        Redis disabled:
            CLOSED

        This means the circuit breaker is bypassed temporarily.
        """
        if self.redis is None:
            return CircuitBreakerState.CLOSED

        value = await self._get("state")

        if not value:
            return CircuitBreakerState.CLOSED

        try:
            return CircuitBreakerState(value)
        except ValueError:
            logger.warning(
                "invalid_circuit_breaker_state",
                gateway=self.gateway,
                value=value,
            )
            return CircuitBreakerState.CLOSED

    async def _set_state(
        self,
        state: CircuitBreakerState,
    ) -> None:
        """
        Persist a state transition when Redis is enabled.

        Metrics/logging are still attempted when Redis is disabled.
        """
        old_state_raw = await self._get(
            "state"
        )

        old_state = (
            old_state_raw
            or CircuitBreakerState.CLOSED.value
        )

        if self.redis is not None:
            await self._set(
                "state",
                state.value,
            )

            await self._set(
                "state_changed_at",
                str(time.time()),
            )

        # Prometheus metrics must never break payment processing.
        try:
            from app.services.metrics import (
                circuit_breaker_trips,
                set_circuit_breaker_state,
            )

            circuit_breaker_trips.labels(
                gateway=self.gateway,
                from_state=old_state,
                to_state=state.value,
            ).inc()

            set_circuit_breaker_state(
                self.gateway,
                state.value,
            )

        except Exception:
            pass

        logger.info(
            "circuit_breaker_state_change",
            gateway=self.gateway,
            pm=self.payment_method,
            from_state=old_state,
            to_state=state.value,
        )

    # ------------------------------------------------------------------
    # Public circuit-breaker operations
    # ------------------------------------------------------------------

    async def is_open(self) -> bool:
        """
        Return True when the gateway circuit is open.

        Redis disabled:
            Always return False.

        This allows payment routing to continue while Redis
        is temporarily disabled.
        """
        if self.redis is None:
            return False

        state = await self._state()

        # --------------------------------------------------------------
        # CLOSED
        # --------------------------------------------------------------

        if state == CircuitBreakerState.CLOSED:
            return False

        # --------------------------------------------------------------
        # OPEN
        # --------------------------------------------------------------

        if state == CircuitBreakerState.OPEN:
            changed_at = await self._get(
                "state_changed_at"
            )

            if changed_at:
                try:
                    elapsed = (
                        time.time()
                        - float(changed_at)
                    )

                    if (
                        elapsed
                        >= self.config.timeout_seconds
                    ):
                        await self._set_state(
                            CircuitBreakerState.HALF_OPEN
                        )

                        await self._del(
                            "failures",
                            "successes",
                        )

                        # Allow only one probe request.
                        calls = await self._incr(
                            "half_open_calls",
                            ttl=self.config.timeout_seconds,
                        )

                        return (
                            calls
                            > self.config.half_open_max_calls
                        )

                except (ValueError, TypeError):
                    logger.warning(
                        "invalid_circuit_breaker_timestamp",
                        gateway=self.gateway,
                    )

            return True

        # --------------------------------------------------------------
        # HALF OPEN
        # --------------------------------------------------------------

        if state == CircuitBreakerState.HALF_OPEN:
            calls = await self._incr(
                "half_open_calls",
                ttl=self.config.timeout_seconds,
            )

            return (
                calls
                > self.config.half_open_max_calls
            )

        return False

    async def record_success(self) -> None:
        """
        Record a successful gateway request.
        """
        if self.redis is None:
            return

        state = await self._state()

        # HALF_OPEN → successful probes
        if state == CircuitBreakerState.HALF_OPEN:
            count = await self._incr(
                "successes"
            )

            if (
                count
                >= self.config.success_threshold
            ):
                await self._set_state(
                    CircuitBreakerState.CLOSED
                )

                await self._del(
                    "failures",
                    "successes",
                    "half_open_calls",
                )

        # CLOSED → clear failures
        elif state == CircuitBreakerState.CLOSED:
            await self._del(
                "failures"
            )

    async def record_failure(self) -> None:
        """
        Record a failed gateway request.
        """
        if self.redis is None:
            return

        state = await self._state()

        # HALF_OPEN → immediately reopen
        if state == CircuitBreakerState.HALF_OPEN:
            await self._set_state(
                CircuitBreakerState.OPEN
            )

            await self._del(
                "failures",
                "successes",
                "half_open_calls",
            )

            return

        # CLOSED → increment failures
        if state == CircuitBreakerState.CLOSED:
            count = await self._incr(
                "failures"
            )

            await self._set(
                "last_failure",
                str(time.time()),
            )

            if (
                count
                >= self.config.failure_threshold
            ):
                await self._set_state(
                    CircuitBreakerState.OPEN
                )

                await self._del(
                    "failures",
                    "successes",
                )

                logger.error(
                    "circuit_breaker_tripped",
                    gateway=self.gateway,
                    failures=count,
                    threshold=(
                        self.config.failure_threshold
                    ),
                )

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    async def get_status(
        self,
    ) -> CircuitBreakerStatus:
        """
        Return circuit-breaker status.

        Redis disabled:
            CLOSED + healthy.
        """
        if self.redis is None:
            return CircuitBreakerStatus(
                state=CircuitBreakerState.CLOSED,
                failure_count=0,
                success_count=0,
                last_failure_time=None,
                last_state_change=time.time(),
                gateway=self.gateway,
                payment_method=self.payment_method,
            )

        state = await self._state()

        changed = await self._get(
            "state_changed_at"
        )

        last_fail = await self._get(
            "last_failure"
        )

        failures_raw = await self._get(
            "failures"
        )

        successes_raw = await self._get(
            "successes"
        )

        return CircuitBreakerStatus(
            state=state,

            failure_count=(
                int(failures_raw)
                if failures_raw
                else 0
            ),

            success_count=(
                int(successes_raw)
                if successes_raw
                else 0
            ),

            last_failure_time=(
                float(last_fail)
                if last_fail
                else None
            ),

            last_state_change=(
                float(changed)
                if changed
                else time.time()
            ),

            gateway=self.gateway,
            payment_method=self.payment_method,
        )

    # ------------------------------------------------------------------
    # Administrative operations
    # ------------------------------------------------------------------

    async def force_open(self) -> None:
        """
        Force the circuit open.

        Ignored when Redis is disabled because there is
        nowhere to persist the state.
        """
        if self.redis is None:
            logger.warning(
                "circuit_breaker_disabled",
                gateway=self.gateway,
                action="force_open_ignored",
            )
            return

        await self._set_state(
            CircuitBreakerState.OPEN
        )

    async def force_close(self) -> None:
        """
        Force the circuit closed.
        """
        if self.redis is None:
            return

        await self._set_state(
            CircuitBreakerState.CLOSED
        )

        await self._del(
            "failures",
            "successes",
            "half_open_calls",
        )


class CircuitBreakerRegistry:
    """
    Registry containing circuit breakers for each
    gateway/payment-method combination.
    """

    def __init__(
        self,
        redis: Optional[aioredis.Redis],
    ):
        self.redis = redis
        self._breakers: dict[
            str,
            CircuitBreaker,
        ] = {}

    def get(
        self,
        gateway: str,
        payment_method: Optional[str] = None,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> CircuitBreaker:

        key = (
            f"{gateway}:"
            f"{payment_method or 'all'}"
        )

        if key not in self._breakers:
            self._breakers[key] = CircuitBreaker(
                redis=self.redis,
                gateway=gateway,
                payment_method=payment_method,
                config=config,
            )

        return self._breakers[key]

    async def get_all_statuses(
        self,
    ) -> list[CircuitBreakerStatus]:

        return [
            await breaker.get_status()
            for breaker in self._breakers.values()
        ]