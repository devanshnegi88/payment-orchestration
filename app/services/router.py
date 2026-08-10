"""
Gateway Router Engine — real multi-criteria scoring with normalization.

Score(gateway) =
    (W_success * NormalizedSuccessRate)    <- 15-min sliding window from DB
  + (W_latency * (1 - NormalizedLatency)) <- P95 from DB metrics
  + (W_cost    * (1 - NormalizedCost))    <- computed from gateway fee config
  + (W_health  * HealthScore)             <- from Redis circuit breaker state
  + (W_fit     * FitScore)               <- from gateway capability matrix

All normalization is min-max across eligible gateways for this request.
Weights stored in routing_config table — live-updateable, no restart needed.
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from app.config import settings
from app.models.transaction import GatewayConfig, GatewayHealthMetric, RoutingConfig
from app.services.circuit_breaker import CircuitBreakerRegistry, CircuitBreakerState
from app.domain.exceptions import NoAvailableGatewayError

import pytz
from datetime import datetime, timedelta

logger = structlog.get_logger(__name__)

# Payment method → gateways that support it
GATEWAY_PAYMENT_METHOD_MATRIX: dict[str, list[str]] = {
    "CARD":       ["razorpay", "stripe", "payu"],
    "UPI":        ["razorpay", "upi"],
    "NETBANKING": ["razorpay", "payu"],
    "WALLET":     ["razorpay", "payu"],
    "EMI":        ["razorpay", "stripe"],
}

# Default weights (overridden by routing_config table)
DEFAULT_WEIGHTS = {
    "success": settings.ROUTING_WEIGHT_SUCCESS,
    "latency": settings.ROUTING_WEIGHT_LATENCY,
    "cost":    settings.ROUTING_WEIGHT_COST,
    "health":  settings.ROUTING_WEIGHT_HEALTH,
    "fit":     settings.ROUTING_WEIGHT_FIT,
}


@dataclass
class GatewayScore:
    gateway: str
    composite_score: float
    success_rate_score: float   # normalized 0–1, higher = better
    latency_score: float        # normalized 0–1, higher = better (inverted latency)
    cost_score: float           # normalized 0–1, higher = better (inverted cost)
    health_score: float         # 1.0/0.5/0.0
    fit_score: float            # 1.0 or 0.0
    is_degraded: bool = False


@dataclass
class RoutingDecision:
    selected_gateway: str
    scores: list[GatewayScore]
    payment_method: str
    trace_id: Optional[str] = None
    decision_time_ms: float = 0.0


class GatewayRouter:
    def __init__(self, db: AsyncSession, cb_registry: CircuitBreakerRegistry):
        self.db = db
        self.cb_registry = cb_registry

    async def select_gateway(
        self,
        payment_method: str,
        amount_paise: int,
        trace_id: Optional[str] = None,
        exclude_gateways: Optional[list[str]] = None,
    ) -> RoutingDecision:
        t0 = time.monotonic()
        exclude = set(exclude_gateways or [])

        eligible = [
            gw for gw in GATEWAY_PAYMENT_METHOD_MATRIX.get(payment_method.upper(), [])
            if gw not in exclude
        ]
        if not eligible:
            raise NoAvailableGatewayError(payment_method)

        # Load everything in parallel
        metrics_map, config_map, weights = await asyncio.gather(
            self._load_metrics(eligible),
            self._load_configs(eligible),
            self._load_weights(),
        )

        # Build raw scores, filter out gateways with fully open circuit breakers
        raw_scores: list[GatewayScore] = []
        for gw in eligible:
            cb = self.cb_registry.get(gw, payment_method)
            if await cb.is_open():
                logger.warning("router_skipping_open_circuit", gateway=gw, trace_id=trace_id)
                continue

            cb_status = await cb.get_status()
            metrics = metrics_map.get(gw)
            config = config_map.get(gw)

            # Raw success rate from sliding window
            if metrics and metrics["total"] > 0:
                raw_success = metrics["success"] / metrics["total"]
            else:
                raw_success = 0.90  # conservative default when no data

            # P95 latency in ms
            raw_latency = float(metrics["p95_ms"]) if metrics and metrics.get("p95_ms") else 500.0

            # Cost in paise for this transaction
            if config:
                raw_cost = float(int(amount_paise * config.fee_percentage / 100)
                                 + config.fee_fixed_paise)
            else:
                raw_cost = float(int(amount_paise * 0.025))

            raw_scores.append(GatewayScore(
                gateway=gw,
                composite_score=0.0,          # computed below after normalization
                success_rate_score=raw_success,
                latency_score=raw_latency,     # temporarily raw; normalized below
                cost_score=raw_cost,           # temporarily raw; normalized below
                health_score=cb_status.health_score,
                fit_score=1.0,
                is_degraded=(cb_status.state == CircuitBreakerState.HALF_OPEN),
            ))

        if not raw_scores:
            raise NoAvailableGatewayError(f"{payment_method} — all circuits open")

        # Min-max normalize latency and cost across eligible gateways, compute composite
        scores = self._normalize_and_score(raw_scores, weights)
        scores.sort(key=lambda s: s.composite_score, reverse=True)

        # Deprioritize HALF_OPEN if the second-best is within 20% score gap
        selected = scores[0]
        if selected.is_degraded and len(scores) > 1:
            gap = selected.composite_score - scores[1].composite_score
            if gap <= settings.DEGRADED_GATEWAY_SCORE_THRESHOLD:
                selected = scores[1]
                logger.info("router_deprioritized_degraded",
                            skipped=scores[0].gateway, selected=selected.gateway,
                            gap=round(gap, 4))

        decision_ms = round((time.monotonic() - t0) * 1000, 2)
        logger.info(
            "route_selected",
            trace_id=trace_id,
            gateway=selected.gateway,
            composite=round(selected.composite_score, 4),
            success=round(selected.success_rate_score, 4),
            latency=round(selected.latency_score, 4),
            cost=round(selected.cost_score, 4),
            health=selected.health_score,
            fit=selected.fit_score,
            decision_ms=decision_ms,
        )
        return RoutingDecision(
            selected_gateway=selected.gateway,
            scores=scores,
            payment_method=payment_method,
            trace_id=trace_id,
            decision_time_ms=decision_ms,
        )

    def _normalize_and_score(
        self, scores: list[GatewayScore], weights: dict
    ) -> list[GatewayScore]:
        """
        Min-max normalize latency and cost (lower is better → invert after normalizing).
        Success rate is already 0–1 (higher is better, no inversion needed).
        Then compute composite score.
        """
        if len(scores) == 1:
            # Single gateway: all normalized values are 1.0 except health/fit
            s = scores[0]
            s.composite_score = (
                weights["success"] * s.success_rate_score
                + weights["latency"] * 1.0
                + weights["cost"]   * 1.0
                + weights["health"] * s.health_score
                + weights["fit"]    * s.fit_score
            )
            s.latency_score = 1.0
            s.cost_score = 1.0
            return scores

        latencies = [s.latency_score for s in scores]
        costs     = [s.cost_score for s in scores]

        min_lat, max_lat = min(latencies), max(latencies)
        min_cost, max_cost = min(costs), max(costs)

        def minmax(v: float, lo: float, hi: float) -> float:
            return (v - lo) / (hi - lo) if hi != lo else 1.0

        for s in scores:
            norm_latency = minmax(s.latency_score, min_lat, max_lat)
            norm_cost    = minmax(s.cost_score, min_cost, max_cost)

            # Invert: lower latency/cost → higher score
            s.composite_score = (
                weights["success"] * s.success_rate_score      # higher = better, no invert
                + weights["latency"] * (1.0 - norm_latency)   # lower latency = higher score
                + weights["cost"]   * (1.0 - norm_cost)       # lower cost = higher score
                + weights["health"] * s.health_score
                + weights["fit"]    * s.fit_score
            )
            # Store normalized values for logging/analytics
            s.latency_score = 1.0 - norm_latency
            s.cost_score    = 1.0 - norm_cost

        return scores

    async def _load_metrics(self, gateways: list[str]) -> dict[str, dict]:
        """Load 15-minute sliding window aggregates from gateway_health_metrics."""
        window_start = datetime.now(pytz.UTC) - timedelta(minutes=settings.METRICS_WINDOW_MINUTES)
        result = await self.db.execute(
            select(
                GatewayHealthMetric.gateway,
                func.sum(GatewayHealthMetric.total_requests).label("total"),
                func.sum(GatewayHealthMetric.successful_requests).label("success"),
                func.avg(GatewayHealthMetric.p95_latency_ms).label("p95_ms"),
            )
            .where(and_(
                GatewayHealthMetric.gateway.in_(gateways),
                GatewayHealthMetric.window_start >= window_start,
            ))
            .group_by(GatewayHealthMetric.gateway)
        )
        return {
            row.gateway: {
                "total":  int(row.total or 0),
                "success": int(row.success or 0),
                "p95_ms": int(row.p95_ms or 500),
            }
            for row in result.all()
        }

    async def _load_configs(self, gateways: list[str]) -> dict[str, GatewayConfig]:
        result = await self.db.execute(
            select(GatewayConfig).where(
                and_(GatewayConfig.gateway_name.in_(gateways),
                     GatewayConfig.is_active.is_(True))
            )
        )
        return {row.gateway_name: row for row in result.scalars().all()}

    async def _load_weights(self) -> dict:
        result = await self.db.execute(
            select(RoutingConfig).where(RoutingConfig.config_key == "routing_weights")
        )
        cfg = result.scalar_one_or_none()
        if cfg and isinstance(cfg.config_value, dict):
            return cfg.config_value
        return DEFAULT_WEIGHTS


class FailoverRouter:
    def __init__(self, router: GatewayRouter, cb_registry: CircuitBreakerRegistry):
        self.router = router
        self.cb_registry = cb_registry

    async def select_with_failover(
        self,
        payment_method: str,
        amount_paise: int,
        trace_id: Optional[str] = None,
        previous_failed_gateways: Optional[list[str]] = None,
    ) -> RoutingDecision:
        try:
            async with asyncio.timeout(settings.FAILOVER_TIMEOUT_SECONDS):
                return await self.router.select_gateway(
                    payment_method=payment_method,
                    amount_paise=amount_paise,
                    trace_id=trace_id,
                    exclude_gateways=previous_failed_gateways or [],
                )
        except asyncio.TimeoutError:
            raise NoAvailableGatewayError(f"{payment_method} (routing timeout)")
