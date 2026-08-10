"""
Prometheus Metrics — real instrumentation wired into every code path.

Counters:
  payments_initiated_total{gateway, payment_method, status}
  payment_failovers_total{from_gateway, to_gateway}
  webhook_received_total{gateway, event_type, result}
  circuit_breaker_state_changes_total{gateway, from_state, to_state}

Histograms:
  gateway_latency_seconds{gateway, operation}   — auth, capture, refund, status
  payment_routing_duration_seconds              — time to select gateway
  webhook_processing_duration_seconds{gateway}

Gauges:
  circuit_breaker_state{gateway}                — 0=CLOSED,1=HALF_OPEN,2=OPEN
  dlq_depth                                     — unresolved DLQ items
  gateway_rate_limit_utilization{gateway}        — current req/s / limit
"""
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

# Single registry shared across the app
REGISTRY = CollectorRegistry()

# ── Counters ──────────────────────────────────────────────────────────────────

payments_initiated = Counter(
    "payments_initiated_total",
    "Total payment attempts",
    ["gateway", "payment_method", "status"],
    registry=REGISTRY,
)

payment_failovers = Counter(
    "payment_failovers_total",
    "Number of gateway failover events",
    ["from_gateway", "to_gateway", "reason"],
    registry=REGISTRY,
)

webhook_received = Counter(
    "webhook_received_total",
    "Incoming webhook events",
    ["gateway", "event_type", "result"],  # result: processed|duplicate|invalid_sig|dlq
    registry=REGISTRY,
)

circuit_breaker_trips = Counter(
    "circuit_breaker_trips_total",
    "Circuit breaker state change events",
    ["gateway", "from_state", "to_state"],
    registry=REGISTRY,
)

idempotency_hits = Counter(
    "idempotency_hits_total",
    "Idempotency key cache hits (duplicate requests blocked)",
    ["result"],  # replay|conflict|mismatch
    registry=REGISTRY,
)

reconciliation_discrepancies = Counter(
    "reconciliation_discrepancies_total",
    "Reconciliation discrepancies found",
    ["gateway", "discrepancy_type", "is_anomaly"],
    registry=REGISTRY,
)

# ── Histograms ─────────────────────────────────────────────────────────────────

gateway_latency = Histogram(
    "gateway_latency_seconds",
    "Gateway API call latency",
    ["gateway", "operation"],  # operation: auth|capture|refund|void|status
    buckets=(0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 5.0, 10.0, 30.0),
    registry=REGISTRY,
)

routing_duration = Histogram(
    "payment_routing_duration_seconds",
    "Time taken to select a gateway",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
    registry=REGISTRY,
)

webhook_processing_duration = Histogram(
    "webhook_processing_duration_seconds",
    "Time to process a webhook event end-to-end",
    ["gateway"],
    buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0),
    registry=REGISTRY,
)

# ── Gauges ────────────────────────────────────────────────────────────────────

circuit_breaker_state = Gauge(
    "circuit_breaker_state",
    "Circuit breaker state (0=CLOSED, 1=HALF_OPEN, 2=OPEN)",
    ["gateway"],
    registry=REGISTRY,
)

dlq_depth = Gauge(
    "dlq_depth",
    "Number of unresolved dead-letter queue items",
    registry=REGISTRY,
)

rate_limit_utilization = Gauge(
    "gateway_rate_limit_utilization_pct",
    "Current requests/sec as percentage of limit",
    ["gateway"],
    registry=REGISTRY,
)


# ── Helper functions ───────────────────────────────────────────────────────────

def record_gateway_latency(gateway: str, operation: str, latency_ms: int) -> None:
    gateway_latency.labels(gateway=gateway, operation=operation).observe(latency_ms / 1000)


def record_payment_attempt(gateway: str, payment_method: str, status: str) -> None:
    payments_initiated.labels(
        gateway=gateway, payment_method=payment_method, status=status
    ).inc()


def record_failover(from_gw: str, to_gw: str, reason: str) -> None:
    payment_failovers.labels(from_gateway=from_gw, to_gateway=to_gw, reason=reason).inc()


def record_webhook(gateway: str, event_type: str, result: str) -> None:
    webhook_received.labels(gateway=gateway, event_type=event_type, result=result).inc()


def set_circuit_breaker_state(gateway: str, state: str) -> None:
    value_map = {"CLOSED": 0, "HALF_OPEN": 1, "OPEN": 2}
    circuit_breaker_state.labels(gateway=gateway).set(value_map.get(state, 0))
