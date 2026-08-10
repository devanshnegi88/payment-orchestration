"""
Celery Workers — Async task queue for:
- Webhook event processing (from WebhookQueue)
- Reconciliation batch jobs (every 15 minutes)
- Gateway health metric aggregation
- Idempotency key cleanup
- DLQ monitoring alerts
"""
import asyncio
from datetime import datetime, timedelta
from celery import Celery
from celery.schedules import crontab
import structlog

from app.config import settings

logger = structlog.get_logger(__name__)

celery_app = Celery(
    "payment_orchestration",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,           # Ack only after successful processing
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,  # One task at a time per worker (payment safety)
    task_track_started=True,
    result_expires=3600,

    # Beat schedule for periodic tasks
    beat_schedule={
        "process-webhook-queue": {
            "task": "app.workers.tasks.process_webhook_queue_batch",
            "schedule": 5.0,  # Every 5 seconds
        },
        "reconciliation-run": {
            "task": "app.workers.tasks.run_reconciliation",
            "schedule": crontab(minute="*/15"),  # Every 15 minutes
        },
        "aggregate-gateway-metrics": {
            "task": "app.workers.tasks.aggregate_gateway_metrics",
            "schedule": 60.0,  # Every minute
        },
        "cleanup-idempotency-keys": {
            "task": "app.workers.tasks.cleanup_idempotency_keys",
            "schedule": crontab(hour="*/1"),  # Every hour
        },
        "monitor-dlq": {
            "task": "app.workers.tasks.monitor_dlq_depth",
            "schedule": 300.0,  # Every 5 minutes
        },
    },
)


def run_async(coro):
    """Run async coroutine in Celery's sync context."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


@celery_app.task(
    name="app.workers.tasks.process_webhook_event",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    autoretry_for=(Exception,),
    retry_backoff=True,
)
def process_webhook_event(self, queue_item_id: int):
    """Process a single webhook queue item."""
    async def _process():
        from app.database import get_db_context
        from app.services.webhook import WebhookProcessor
        async with get_db_context() as db:
            processor = WebhookProcessor(db)
            return await processor.process_queued_event(queue_item_id)

    return run_async(_process())


@celery_app.task(name="app.workers.tasks.process_webhook_queue_batch")
def process_webhook_queue_batch():
    """Poll the webhook queue and dispatch pending items."""
    async def _batch():
        from sqlalchemy import select, and_
        from app.database import get_db_context
        from app.models.transaction import WebhookQueue

        async with get_db_context() as db:
            result = await db.execute(
                select(WebhookQueue.id).where(
                    and_(
                        WebhookQueue.status.in_(["PENDING", "FAILED"]),
                        (WebhookQueue.next_retry_at.is_(None)) |
                        (WebhookQueue.next_retry_at <= datetime.utcnow()),
                    )
                ).limit(100)
            )
            ids = [row[0] for row in result.all()]

        for queue_id in ids:
            process_webhook_event.delay(queue_id)

        if ids:
            logger.info("webhook_batch_dispatched", count=len(ids))

    run_async(_batch())


@celery_app.task(name="app.workers.tasks.run_reconciliation")
def run_reconciliation():
    """Execute the periodic reconciliation batch."""
    async def _reconcile():
        from app.database import get_db_context
        from app.services.reconciliation import ReconciliationEngine
        async with get_db_context() as db:
            engine = ReconciliationEngine(db)
            run = await engine.run(triggered_by="celery_scheduler")
            logger.info(
                "scheduled_reconciliation_completed",
                run_id=run.id,
                checked=run.transactions_checked,
                discrepancies=run.discrepancies_found,
                anomalies=run.anomalies_found,
            )

    run_async(_reconcile())


@celery_app.task(name="app.workers.tasks.aggregate_gateway_metrics")
def aggregate_gateway_metrics():
    """Aggregate per-minute gateway health metrics from transaction logs."""
    async def _aggregate():
        from sqlalchemy import select, func, and_
        from app.database import get_db_context
        from app.models.transaction import Transaction, GatewayHealthMetric
        import pytz

        now = datetime.now(pytz.UTC)
        window_start = now - timedelta(minutes=1)
        window_end = now

        async with get_db_context() as db:
            result = await db.execute(
                select(
                    Transaction.gateway,
                    func.count(Transaction.id).label("total"),
                    func.count(Transaction.id).filter(
                        Transaction.state.in_(["CAPTURED", "AUTHORISED"])
                    ).label("success"),
                    func.count(Transaction.id).filter(
                        Transaction.state.in_(["FAILED", "AUTH_FAILED"])
                    ).label("failed"),
                )
                .where(
                    and_(
                        Transaction.updated_at >= window_start,
                        Transaction.updated_at < window_end,
                        Transaction.gateway.isnot(None),
                    )
                )
                .group_by(Transaction.gateway)
            )

            for row in result.all():
                if not row.gateway:
                    continue
                metric = GatewayHealthMetric(
                    gateway=row.gateway,
                    window_start=window_start,
                    window_end=window_end,
                    total_requests=row.total,
                    successful_requests=row.success or 0,
                    failed_requests=row.failed or 0,
                )
                db.add(metric)

    run_async(_aggregate())


@celery_app.task(name="app.workers.tasks.cleanup_idempotency_keys")
def cleanup_idempotency_keys():
    """Remove expired idempotency keys."""
    async def _cleanup():
        from app.database import get_db_context
        from app.services.idempotency import IdempotencyService
        async with get_db_context() as db:
            service = IdempotencyService(db)
            count = await service.cleanup_expired()
            logger.info("idempotency_cleanup", deleted=count)

    run_async(_cleanup())


@celery_app.task(name="app.workers.tasks.monitor_dlq_depth")
def monitor_dlq_depth():
    """Alert if DLQ depth is non-zero."""
    async def _monitor():
        from sqlalchemy import select, func
        from app.database import get_db_context
        from app.models.transaction import DeadLetterQueue

        async with get_db_context() as db:
            result = await db.execute(
                select(func.count(DeadLetterQueue.id)).where(
                    DeadLetterQueue.resolved_at.is_(None)
                )
            )
            depth = result.scalar() or 0
            if depth > 0:
                logger.error("dlq_non_zero_alert", depth=depth)
                # In production: send PagerDuty/Slack alert here

    run_async(_monitor())
