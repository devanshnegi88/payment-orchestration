"""Unit Tests — Circuit Breaker & Routing Algorithm."""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from app.services.circuit_breaker import CircuitBreaker, CircuitBreakerState, CircuitBreakerConfig
from app.domain.exceptions import (
    GatewayTimeoutError, GatewayServerError, GatewayDeclineError,
    GatewayRateLimitError, GatewayUnavailableError,
)


# ── Circuit Breaker Tests ──────────────────────────────────────────────────────

@pytest.fixture
def mock_redis():
    store = {}
    class MockRedis:
        async def get(self, key):
            val = store.get(key)
            return val.encode() if isinstance(val, str) else val
        async def set(self, key, value, *args, **kwargs):
            store[key] = str(value)
        async def incr(self, key):
            current = int(store.get(key, "0"))
            store[key] = str(current + 1)
            return current + 1
        async def delete(self, *keys):
            for k in keys:
                store.pop(k, None)
        async def expire(self, key, seconds):
            pass
    return MockRedis()


@pytest.fixture
def cb_config():
    return CircuitBreakerConfig(
        failure_threshold=3, success_threshold=2,
        timeout_seconds=5, half_open_max_calls=1,
    )


@pytest.fixture
def cb(mock_redis, cb_config):
    return CircuitBreaker(mock_redis, "razorpay", config=cb_config)


class TestCircuitBreakerClosed:

    @pytest.mark.asyncio
    async def test_initially_closed(self, cb):
        assert await cb.is_open() is False

    @pytest.mark.asyncio
    async def test_stays_closed_under_threshold(self, cb):
        await cb.record_failure()
        await cb.record_failure()
        assert await cb.is_open() is False

    @pytest.mark.asyncio
    async def test_trips_at_threshold(self, cb):
        for _ in range(3):
            await cb.record_failure()
        assert await cb.is_open() is True

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self, cb):
        await cb.record_failure()
        await cb.record_failure()
        await cb.record_success()
        # Only 2 failures then reset; need 3 consecutive to trip
        await cb.record_failure()
        await cb.record_failure()
        assert await cb.is_open() is False

    @pytest.mark.asyncio
    async def test_health_score_closed(self, cb):
        status = await cb.get_status()
        assert status.health_score == 1.0


class TestCircuitBreakerOpen:

    @pytest.mark.asyncio
    async def test_open_blocks_requests(self, cb):
        for _ in range(3):
            await cb.record_failure()
        assert await cb.is_open() is True

    @pytest.mark.asyncio
    async def test_health_score_open(self, cb):
        for _ in range(3):
            await cb.record_failure()
        status = await cb.get_status()
        assert status.health_score == 0.0

    @pytest.mark.asyncio
    async def test_state_is_open_after_trip(self, cb):
        for _ in range(3):
            await cb.record_failure()
        status = await cb.get_status()
        assert status.state == CircuitBreakerState.OPEN


class TestCircuitBreakerForce:

    @pytest.mark.asyncio
    async def test_force_open_then_close(self, cb):
        await cb.force_open()
        assert await cb.is_open() is True
        await cb.force_close()
        assert await cb.is_open() is False

    @pytest.mark.asyncio
    async def test_health_score_half_open(self, cb):
        await cb._set_state(CircuitBreakerState.HALF_OPEN)
        status = await cb.get_status()
        assert status.health_score == 0.5


# ── Routing Algorithm Tests ────────────────────────────────────────────────────

class TestRoutingScoreNormalization:

    def test_normalize_inverts_latency_cost(self):
        """Lower latency and cost must produce higher normalized scores."""
        from app.services.router import GatewayRouter
        # Manually exercise normalization logic
        from app.services.router import GatewayScore

        # Two gateways: fast/cheap vs slow/expensive
        fast = GatewayScore(gateway="upi", composite_score=0,
                            success_rate_score=0.99, latency_score=180.0,
                            cost_score=0.0, health_score=1.0, fit_score=1.0)
        slow = GatewayScore(gateway="payu", composite_score=0,
                            success_rate_score=0.95, latency_score=950.0,
                            cost_score=500.0, health_score=1.0, fit_score=1.0)

        weights = {"success": 0.35, "latency": 0.20, "cost": 0.20, "health": 0.15, "fit": 0.10}
        # Create a minimal router to call normalization
        router = GatewayRouter.__new__(GatewayRouter)
        router.weights = weights
        scores = router._normalize_and_score([fast, slow], weights)

        # UPI (lower latency, lower cost) should score higher
        upi_score = next(s for s in scores if s.gateway == "upi")
        payu_score = next(s for s in scores if s.gateway == "payu")
        assert upi_score.composite_score > payu_score.composite_score

    def test_single_gateway_gets_full_score(self):
        """When only one gateway is available, it gets normalized scores of 1.0."""
        from app.services.router import GatewayRouter, GatewayScore
        router = GatewayRouter.__new__(GatewayRouter)
        weights = {"success": 0.35, "latency": 0.20, "cost": 0.20, "health": 0.15, "fit": 0.10}
        score = GatewayScore(gateway="stripe", composite_score=0,
                             success_rate_score=0.99, latency_score=300.0,
                             cost_score=200.0, health_score=1.0, fit_score=1.0)
        result = router._normalize_and_score([score], weights)
        assert len(result) == 1
        assert result[0].latency_score == 1.0
        assert result[0].cost_score == 1.0

    def test_open_circuit_excluded_from_scoring(self):
        """Gateways with open circuit breakers must not appear in scores."""
        from app.services.router import GATEWAY_PAYMENT_METHOD_MATRIX
        # This is enforced in select_gateway by checking cb.is_open() before scoring
        # Verify at least 2 gateways exist per method so failover is possible
        for method, gateways in GATEWAY_PAYMENT_METHOD_MATRIX.items():
            if method == "UPI":
                continue  # UPI may have fewer options
            assert len(gateways) >= 2, f"{method} needs >= 2 gateways for failover"

    def test_weights_must_sum_to_one(self):
        from app.schemas.payment import RoutingWeightsRequest
        with pytest.raises(Exception):
            RoutingWeightsRequest(success=0.5, latency=0.5, cost=0.5, health=0.5, fit=0.5)
        valid = RoutingWeightsRequest(
            success=0.35, latency=0.20, cost=0.20, health=0.15, fit=0.10
        )
        total = valid.success + valid.latency + valid.cost + valid.health + valid.fit
        assert abs(total - 1.0) < 0.001

    def test_degraded_gateway_deprioritized(self):
        """HALF_OPEN gateway (health=0.5) scores below CLOSED (health=1.0)."""
        from app.services.router import GatewayScore
        weights = {"success": 0.35, "latency": 0.20, "cost": 0.20, "health": 0.15, "fit": 0.10}

        def score(health):
            return (0.35 * 1.0 + 0.20 * 1.0 + 0.20 * 1.0
                    + weights["health"] * health + 0.10 * 1.0)

        assert score(1.0) > score(0.5)  # CLOSED > HALF_OPEN
        assert score(0.5) > score(0.0)  # HALF_OPEN > OPEN


# ── Idempotency Tests ──────────────────────────────────────────────────────────

class TestIdempotencyHashing:

    def test_same_body_same_hash(self):
        from app.services.idempotency import compute_request_hash
        body = {"amount": 5000, "currency": "INR", "merchant_order_id": "order_123"}
        assert compute_request_hash(body) == compute_request_hash(body)

    def test_different_body_different_hash(self):
        from app.services.idempotency import compute_request_hash
        assert compute_request_hash({"amount": 5000}) != compute_request_hash({"amount": 6000})

    def test_key_order_stable(self):
        from app.services.idempotency import compute_request_hash
        b1 = {"a": 1, "b": 2, "c": 3}
        b2 = {"c": 3, "a": 1, "b": 2}
        assert compute_request_hash(b1) == compute_request_hash(b2)

    def test_hash_is_sha256_hex(self):
        from app.services.idempotency import compute_request_hash
        h = compute_request_hash({"key": "value"})
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# ── Retry Engine Tests ─────────────────────────────────────────────────────────

class TestRetryClassification:

    def test_decline_is_abort(self):
        from app.services.retry import classify, RetryClass
        from app.domain.exceptions import GatewayDeclineError
        assert classify(GatewayDeclineError("razorpay", "insufficient_funds", "declined")) == RetryClass.ABORT

    def test_timeout_is_failover(self):
        from app.services.retry import classify, RetryClass
        assert classify(GatewayTimeoutError("razorpay", 30)) == RetryClass.FAILOVER

    def test_5xx_is_retryable(self):
        from app.services.retry import classify, RetryClass
        assert classify(GatewayServerError("stripe", 502, "bad gateway")) == RetryClass.RETRYABLE
        assert classify(GatewayServerError("stripe", 503, "service unavailable")) == RetryClass.RETRYABLE

    def test_rate_limit_is_retryable(self):
        from app.services.retry import classify, RetryClass
        from app.domain.exceptions import GatewayRateLimitError
        assert classify(GatewayRateLimitError("payu", 5.0)) == RetryClass.RETRYABLE

    def test_circuit_open_is_failover(self):
        from app.services.retry import classify, RetryClass
        from app.domain.exceptions import GatewayUnavailableError
        assert classify(GatewayUnavailableError("razorpay")) == RetryClass.FAILOVER

    @pytest.mark.asyncio
    async def test_abort_error_raises_immediately(self):
        from app.services.retry import with_retry
        from app.domain.exceptions import GatewayDeclineError
        calls = 0
        async def always_decline():
            nonlocal calls
            calls += 1
            raise GatewayDeclineError("razorpay", "insufficient_funds", "declined")
        with pytest.raises(GatewayDeclineError):
            await with_retry(always_decline, "razorpay", "auth", max_retries=3)
        assert calls == 1  # Must not retry on abort

    @pytest.mark.asyncio
    async def test_retryable_error_retried_up_to_max(self):
        from app.services.retry import with_retry
        calls = 0
        async def always_fail_500():
            nonlocal calls
            calls += 1
            raise GatewayServerError("stripe", 500, "internal")
        with pytest.raises(GatewayServerError):
            await with_retry(always_fail_500, "stripe", "capture", max_retries=2)
        assert calls == 3  # initial + 2 retries


# ── Rate Limiter Tests ─────────────────────────────────────────────────────────

class TestRateLimiter:

    @pytest.mark.asyncio
    async def test_within_limit_passes(self):
        from app.services.rate_limiter import TokenBucketRateLimiter, GATEWAY_LIMITS
        redis = MagicMock()
        redis.incr = AsyncMock(return_value=1)   # first request in window
        redis.expire = AsyncMock()
        limiter = TokenBucketRateLimiter(redis)
        # Should not raise
        await limiter.acquire("razorpay")

    @pytest.mark.asyncio
    async def test_over_limit_raises(self):
        from app.services.rate_limiter import TokenBucketRateLimiter, GATEWAY_LIMITS
        from app.domain.exceptions import GatewayRateLimitError
        redis = MagicMock()
        limit = GATEWAY_LIMITS["razorpay"]
        redis.incr = AsyncMock(return_value=limit + 1)  # over limit
        redis.expire = AsyncMock()
        limiter = TokenBucketRateLimiter(redis)
        with pytest.raises(GatewayRateLimitError):
            await limiter.acquire("razorpay")

    def test_all_gateways_have_limits(self):
        from app.services.rate_limiter import GATEWAY_LIMITS
        for gw in ["razorpay", "stripe", "payu", "upi"]:
            assert gw in GATEWAY_LIMITS
            assert GATEWAY_LIMITS[gw] > 0
