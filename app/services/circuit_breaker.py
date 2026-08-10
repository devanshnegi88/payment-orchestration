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

    async def _get(self, suffix: str):
        if self.redis is None:
            return None

        return await self.redis.get(f"{self._p}:{suffix}")

    async def _set(
        self,
        suffix: str,
        value: str,
        ttl: Optional[int] = None,
    ) -> None:
        if self.redis is None:
            return

        key = f"{self._p}:{suffix}"
        await self.redis.set(key, value)

        if ttl:
            await self.redis.expire(key, ttl)

    async def _incr(self, suffix: str, ttl: int = 600) -> int:
        if self.redis is None:
            return 0

        key = f"{self._p}:{suffix}"
        count = await self.redis.incr(key)
        await self.redis.expire(key, ttl)
        return count

    async def _del(self, *suffixes: str) -> None:
        if self.redis is None:
            return

        await self.redis.delete(
            *[f"{self._p}:{s}" for s in suffixes]
        )

    async def _state(self) -> CircuitBreakerState:
        # Redis disabled → behave as CLOSED
        if self.redis is None:
            return CircuitBreakerState.CLOSED

        value = await self._get("state")

        if not value:
            return CircuitBreakerState.CLOSED

        return CircuitBreakerState(value)

    async def _set_state(
        self,
        state: CircuitBreakerState,
    ) -> None:
        old_state_raw = await self._get("state")
        old_state = old_state_raw or "CLOSED"

        # Redis disabled: don't persist state
        if self.redis is not None:
            await self._set("state", state.value)
            await self._set(
                "state_changed_at",
                str(time.time()),
            )

        # Metrics should still work without Redis
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

    async def is_open(self) -> bool:
        # Redis disabled → circuit breaker bypassed
        if self.redis is None:
            return False

        state = await self._state()

        if state == CircuitBreakerState.CLOSED:
            return False

        if state == CircuitBreakerState.OPEN:
            changed_at = await self._get("state_changed_at")

            if (
                changed_at
                and (
                    time.time() - float(changed_at)
                    >= self.config.timeout_seconds
                )
            ):
                await self._set_state(
                    CircuitBreakerState.HALF_OPEN
                )

                await self._del(
                    "failures",
                    "successes",
                )

                calls = await self._incr(
                    "half_open_calls",
                    ttl=self.config.timeout_seconds,
                )

                return calls > self.config.half_open_max_calls

            return True

        if state == CircuitBreakerState.HALF_OPEN:
            calls = await self._incr(
                "half_open_calls",
                ttl=self.config.timeout_seconds,
            )

            return calls > self.config.half_open_max_calls

        return False

    async def record_success(self) -> None:
        # No Redis → nothing to persist
        if self.redis is None:
            return

        state = await self._state()

        if state == CircuitBreakerState.HALF_OPEN:
            count = await self._incr("successes")

            if count >= self.config.success_threshold:
                await self._set_state(
                    CircuitBreakerState.CLOSED
                )

                await self._del(
                    "failures",
                    "successes",
                    "half_open_calls",
                )

        elif state == CircuitBreakerState.CLOSED:
            await self._del("failures")

    async def record_failure(self) -> None:
        # No Redis → circuit breaker disabled
        if self.redis is None:
            return

        state = await self._state()

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

        if state == CircuitBreakerState.CLOSED:
            count = await self._incr("failures")

            await self._set(
                "last_failure",
                str(time.time()),
            )

            if count >= self.config.failure_threshold:
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
                    threshold=self.config.failure_threshold,
                )

    async def get_status(self) -> CircuitBreakerStatus:
        # Redis disabled → report healthy/closed state
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

    async def force_open(self) -> None:
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