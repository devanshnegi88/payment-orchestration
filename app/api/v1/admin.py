"""Gateway health, analytics, reconciliation, and DLQ admin APIs."""
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
import pytz

from app.database import get_db
from app.api.deps import get_cb_registry, get_redis, get_reconciliation_engine
from app.models.transaction import (
    Transaction, GatewayConfig, GatewayHealthMetric, RoutingConfig,
    ReconciliationRun, ReconciliationLog, DeadLetterQueue,
)
from app.schemas.payment import (
    GatewayHealthResponse, GatewayMetricsResponse,
    RoutingWeightsRequest, RoutingWeightsResponse,
    ReconciliationRunResponse, ReconciliationLogEntry,
    SuccessRateResponse, VolumeResponse,
    DLQItemResponse,
)
from app.services.circuit_breaker import CircuitBreakerRegistry
from app.services.reconciliation import ReconciliationEngine
from app.api.middleware.auth import require_api_key

gateways_router = APIRouter(prefix="/gateways", tags=["Gateway Health"])
routing_router = APIRouter(prefix="/routing", tags=["Routing Config"])
reconciliation_router = APIRouter(prefix="/reconciliation", tags=["Reconciliation"])
analytics_router = APIRouter(prefix="/analytics", tags=["Analytics"])
dlq_router = APIRouter(prefix="/dlq", tags=["Dead Letter Queue"])


# ── Gateway Health ─────────────────────────────────────────────────────────────

@gateways_router.get("", summary="List all configured gateways")
async def list_gateways(
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    result = await db.execute(select(GatewayConfig))
    configs = result.scalars().all()
    return [
        {
            "name": c.gateway_name,
            "is_active": c.is_active,
            "supported_methods": c.supported_payment_methods,
            "fee_percentage": c.fee_percentage,
        }
        for c in configs
    ]


@gateways_router.get("/{gateway_name}/health", response_model=GatewayHealthResponse)
async def get_gateway_health(
    gateway_name: str,
    cb_registry: CircuitBreakerRegistry = Depends(get_cb_registry),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """Get real-time health status including circuit breaker state."""
    cb = cb_registry.get(gateway_name)
    status = await cb.get_status()

    config_result = await db.execute(
        select(GatewayConfig).where(GatewayConfig.gateway_name == gateway_name)
    )
    config = config_result.scalar_one_or_none()

    return GatewayHealthResponse(
        gateway=gateway_name,
        circuit_breaker={
            "state": status.state.value,
            "failure_count": status.failure_count,
            "success_count": status.success_count,
            "last_state_change": status.last_state_change,
            "health_score": status.health_score,
        },
        is_active=config.is_active if config else False,
    )


@gateways_router.get("/{gateway_name}/metrics", response_model=GatewayMetricsResponse)
async def get_gateway_metrics(
    gateway_name: str,
    window_minutes: int = Query(default=15, ge=1, le=1440),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """Get performance metrics for the sliding window."""
    cutoff = datetime.now(pytz.UTC) - timedelta(minutes=window_minutes)
    result = await db.execute(
        select(
            func.sum(GatewayHealthMetric.total_requests),
            func.sum(GatewayHealthMetric.successful_requests),
            func.sum(GatewayHealthMetric.failed_requests),
            func.avg(GatewayHealthMetric.p95_latency_ms),
            func.avg(GatewayHealthMetric.p50_latency_ms),
        ).where(
            and_(GatewayHealthMetric.gateway == gateway_name,
                 GatewayHealthMetric.window_start >= cutoff)
        )
    )
    row = result.one()
    total = row[0] or 0
    success = row[1] or 0
    return GatewayMetricsResponse(
        gateway=gateway_name,
        window_minutes=window_minutes,
        total_requests=total,
        successful_requests=success,
        failed_requests=row[2] or 0,
        success_rate=round(success / total, 4) if total > 0 else 0.0,
        p95_latency_ms=int(row[3]) if row[3] else None,
        p50_latency_ms=int(row[4]) if row[4] else None,
    )


@gateways_router.put("/{gateway_name}/config", summary="Update gateway configuration")
async def update_gateway_config(
    gateway_name: str,
    updates: dict,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    result = await db.execute(select(GatewayConfig).where(GatewayConfig.gateway_name == gateway_name))
    config = result.scalar_one_or_none()
    if not config:
        raise HTTPException(status_code=404, detail={"code": "GATEWAY_NOT_FOUND"})

    allowed_fields = {"is_active", "cb_failure_threshold", "cb_timeout_seconds", "rate_limit_per_second"}
    for key, val in updates.items():
        if key in allowed_fields:
            setattr(config, key, val)
    await db.commit()
    return {"status": "updated", "gateway": gateway_name}


# ── Routing Config ─────────────────────────────────────────────────────────────

@routing_router.get("/config", response_model=RoutingWeightsResponse)
async def get_routing_config(
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    result = await db.execute(select(RoutingConfig).where(RoutingConfig.config_key == "routing_weights"))
    config = result.scalar_one_or_none()
    if not config:
        from app.config import settings
        return RoutingWeightsResponse(
            success=settings.ROUTING_WEIGHT_SUCCESS,
            latency=settings.ROUTING_WEIGHT_LATENCY,
            cost=settings.ROUTING_WEIGHT_COST,
            health=settings.ROUTING_WEIGHT_HEALTH,
            fit=settings.ROUTING_WEIGHT_FIT,
        )
    return RoutingWeightsResponse(**config.config_value)


@routing_router.put("/config", response_model=RoutingWeightsResponse)
async def update_routing_config(
    body: RoutingWeightsRequest,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """Update routing algorithm weights. Changes take effect immediately (no restart needed)."""
    result = await db.execute(select(RoutingConfig).where(RoutingConfig.config_key == "routing_weights"))
    config = result.scalar_one_or_none()
    weights = body.model_dump()

    if config:
        config.config_value = weights
    else:
        config = RoutingConfig(config_key="routing_weights", config_value=weights)
        db.add(config)
    await db.commit()
    return RoutingWeightsResponse(**weights)


# ── Reconciliation ─────────────────────────────────────────────────────────────

@reconciliation_router.post("/trigger", response_model=ReconciliationRunResponse, status_code=202)
async def trigger_reconciliation(
    engine: ReconciliationEngine = Depends(get_reconciliation_engine),
    _: None = Depends(require_api_key),
):
    """Manually trigger a reconciliation run."""
    run = await engine.run(triggered_by="api_manual")
    return ReconciliationRunResponse.model_validate(run)


@reconciliation_router.get("/reports/{run_id}", response_model=ReconciliationRunResponse)
async def get_reconciliation_report(
    run_id: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    result = await db.execute(select(ReconciliationRun).where(ReconciliationRun.id == run_id))
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail={"code": "RUN_NOT_FOUND"})
    return ReconciliationRunResponse.model_validate(run)


@reconciliation_router.get("/reports/{run_id}/discrepancies", response_model=list[ReconciliationLogEntry])
async def get_discrepancies(
    run_id: str,
    anomalies_only: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    query = select(ReconciliationLog).where(ReconciliationLog.run_id == run_id)
    if anomalies_only:
        query = query.where(ReconciliationLog.is_anomaly == True)
    result = await db.execute(query)
    return [ReconciliationLogEntry.model_validate(r) for r in result.scalars().all()]


# ── Analytics ──────────────────────────────────────────────────────────────────

@analytics_router.get("/success-rate", response_model=list[SuccessRateResponse])
async def get_success_rates(
    window_hours: int = Query(default=24, ge=1, le=168),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """Gateway success rates for the given window."""
    cutoff = datetime.now(pytz.UTC) - timedelta(hours=window_hours)
    result = await db.execute(
        select(
            GatewayHealthMetric.gateway,
            func.sum(GatewayHealthMetric.total_requests).label("total"),
            func.sum(GatewayHealthMetric.successful_requests).label("success"),
        )
        .where(GatewayHealthMetric.window_start >= cutoff)
        .group_by(GatewayHealthMetric.gateway)
    )
    return [
        SuccessRateResponse(
            gateway=row.gateway,
            window_hours=window_hours,
            total_transactions=row.total or 0,
            successful_transactions=row.success or 0,
            success_rate_pct=round((row.success or 0) / (row.total or 1) * 100, 2),
        )
        for row in result.all()
    ]


@analytics_router.get("/volume", response_model=list[VolumeResponse])
async def get_volume(
    days: int = Query(default=7, ge=1, le=90),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """Daily transaction volume for the given number of days."""
    cutoff = datetime.now(pytz.UTC) - timedelta(days=days)
    result = await db.execute(
        select(
            func.date(Transaction.created_at).label("date"),
            func.count(Transaction.id).label("total"),
            func.sum(Transaction.amount_paise).label("amount"),
            func.count(Transaction.id).filter(Transaction.state == "CAPTURED").label("successful"),
            func.count(Transaction.id).filter(Transaction.state == "FAILED").label("failed"),
        )
        .where(Transaction.created_at >= cutoff)
        .group_by(func.date(Transaction.created_at))
        .order_by(func.date(Transaction.created_at))
    )
    return [
        VolumeResponse(
            date=str(row.date),
            total_transactions=row.total,
            total_amount_paise=row.amount or 0,
            successful_transactions=row.successful or 0,
            failed_transactions=row.failed or 0,
        )
        for row in result.all()
    ]


# ── DLQ Management ─────────────────────────────────────────────────────────────

@dlq_router.get("", response_model=list[DLQItemResponse])
async def list_dlq_items(
    resolved: bool = Query(default=False),
    gateway: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """List items in the dead letter queue."""
    query = select(DeadLetterQueue)
    if not resolved:
        query = query.where(DeadLetterQueue.resolved_at.is_(None))
    if gateway:
        query = query.where(DeadLetterQueue.gateway == gateway)
    result = await db.execute(query.order_by(DeadLetterQueue.created_at.desc()).limit(100))
    return [DLQItemResponse.model_validate(item) for item in result.scalars().all()]


@dlq_router.post("/{dlq_id}/replay", status_code=202)
async def replay_dlq_item(
    dlq_id: str,
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_api_key),
):
    """Replay a DLQ item after root cause is fixed."""
    from app.services.webhook import WebhookProcessor
    processor = WebhookProcessor(db)
    success = await processor.replay_dlq_item(dlq_id)
    if not success:
        raise HTTPException(status_code=404, detail={"code": "DLQ_ITEM_NOT_FOUND"})
    return {"status": "replayed", "dlq_id": dlq_id}
