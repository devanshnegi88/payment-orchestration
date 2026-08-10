"""
FastAPI Application — production wiring with Prometheus metrics endpoint,
structured logging, request tracing, and proper lifespan management.
"""
import time
import uuid
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, HTMLResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from pathlib import Path

from app.config import settings
from app.database import get_engine, close_db, Base
from app.services.metrics import REGISTRY
from app.api.v1 import payments, webhooks, admin
from app.domain.exceptions import (
    InvalidStateTransitionError, TransactionNotFoundError,
    IdempotencyConflictError, NoAvailableGatewayError,
    GatewayError, WebhookSignatureError,
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("starting", version=settings.APP_VERSION, env=settings.ENVIRONMENT)
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("ready")
    except Exception as e:
        logger.warning(f"Database unavailable (running in degraded mode): {e}")
    yield
    try:
        await close_db()
    except Exception:
        pass
    logger.info("shutdown_complete")


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    app.add_middleware(CORSMiddleware, allow_origins=["*"],
                       allow_methods=["*"], allow_headers=["*"])

    @app.get("/", tags=["System"], response_class=HTMLResponse)
    async def serve_ui():
        html_path = Path(__file__).parent / "static" / "index.html"
        if html_path.exists():
            return html_path.read_text(encoding="utf-8")
        return HTMLResponse(content="UI not found", status_code=404)

    @app.middleware("http")
    async def trace_requests(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        request.state.request_id = request_id
        t0 = time.monotonic()
        response = await call_next(request)
        duration_ms = int((time.monotonic() - t0) * 1000)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time-Ms"] = str(duration_ms)
        logger.info("http_request", method=request.method, path=request.url.path,
                    status=response.status_code, duration_ms=duration_ms,
                    request_id=request_id)
        return response

    # ── Exception handlers ─────────────────────────────────────────────────────

    @app.exception_handler(InvalidStateTransitionError)
    async def handle_invalid_transition(request: Request, exc: InvalidStateTransitionError):
        return JSONResponse(status_code=409, content={"error": {
            "code": "INVALID_STATE_TRANSITION", "message": exc.message,
            "details": exc.context,
            "request_id": getattr(request.state, "request_id", None),
        }})

    @app.exception_handler(TransactionNotFoundError)
    async def handle_not_found(request: Request, exc: TransactionNotFoundError):
        return JSONResponse(status_code=404, content={"error": {
            "code": "TRANSACTION_NOT_FOUND", "message": exc.message}})

    @app.exception_handler(IdempotencyConflictError)
    async def handle_idempotency(request: Request, exc: IdempotencyConflictError):
        return JSONResponse(status_code=409, content={"error": {
            "code": "IDEMPOTENCY_CONFLICT", "message": exc.message}})

    @app.exception_handler(NoAvailableGatewayError)
    async def handle_no_gateway(request: Request, exc: NoAvailableGatewayError):
        return JSONResponse(status_code=503, content={"error": {
            "code": "NO_GATEWAY_AVAILABLE", "message": exc.message}})

    @app.exception_handler(GatewayError)
    async def handle_gateway_error(request: Request, exc: GatewayError):
        return JSONResponse(status_code=502, content={"error": {
            "code": "GATEWAY_ERROR", "message": "Gateway error",
            "details": {"gateway": exc.gateway, "retryable": exc.retryable},
        }})

    @app.exception_handler(WebhookSignatureError)
    async def handle_webhook_signature(request: Request, exc: WebhookSignatureError):
        return JSONResponse(status_code=401, content={"error": {
            "code": "WEBHOOK_SIGNATURE_INVALID", "message": exc.message}})

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception):
        logger.error("unexpected_error", error=str(exc), path=request.url.path, exc_info=True)
        return JSONResponse(status_code=500, content={"error": {
            "code": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred",
            "request_id": getattr(request.state, "request_id", None),
        }})

    # ── Routers ────────────────────────────────────────────────────────────────
    prefix = settings.API_V1_PREFIX
    app.include_router(payments.router, prefix=prefix)
    app.include_router(webhooks.router, prefix=prefix)
    app.include_router(admin.gateways_router, prefix=prefix)
    app.include_router(admin.routing_router, prefix=prefix)
    app.include_router(admin.reconciliation_router, prefix=prefix)
    app.include_router(admin.analytics_router, prefix=prefix)
    app.include_router(admin.dlq_router, prefix=prefix)

    @app.get("/api/v1/health", tags=["System"])
    async def health():
        return {"status": "healthy", "version": settings.APP_VERSION,
                "environment": settings.ENVIRONMENT}

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics():
        """Prometheus scrape endpoint."""
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/v1/rate-limits", tags=["System"])
    async def rate_limit_utilization(request: Request):
        """Current requests/sec per gateway vs configured limit."""
        from app.api.deps import get_redis
        from app.services.rate_limiter import TokenBucketRateLimiter
        redis = await get_redis()
        limiter = TokenBucketRateLimiter(redis)
        return await limiter.get_all_utilizations()

    return app


app = create_app()
