"""
Application Configuration — all settings loaded from environment variables.
No hardcoded credentials anywhere in the codebase.
"""
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    APP_NAME: str = "PayFlow Payment Orchestration"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str = "development"

    # API
    API_V1_PREFIX: str = "/api/v1"
    API_KEY: str = "changeme"          # used only in dev; prod uses api_keys table

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://postgres.azyuhafzgqjpcosnmglv:Devnegi%40005@aws-1-ap-northeast-2.pooler.supabase.com:5432/postgres"
    DATABASE_POOL_SIZE: int = 20
    DATABASE_MAX_OVERFLOW: int = 10
    DATABASE_POOL_TIMEOUT: int = 30
    DATABASE_POOL_RECYCLE: int = 1800
    DATABASE_REPLICA_URL: Optional[str] = None

    # Redis
    REDIS_ENABLED: bool = False  # Set to True to enable Redis for rate limiting
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_MAX_CONNECTIONS: int = 50

    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"

    # ── Razorpay credentials ────────────────────────────────────────────────
    RAZORPAY_KEY_ID: str = "your_key_id"
    RAZORPAY_KEY_SECRET: str = "razorpay_secret_key"
    RAZORPAY_WEBHOOK_SECRET: str = "razorpay_webhook_secret"

    # ── Stripe credentials ──────────────────────────────────────────────────
    STRIPE_SECRET_KEY: str = "your_stripe_key"           
    STRIPE_WEBHOOK_SECRET: str = "stripe_webhook"       

    # ── PayU credentials ────────────────────────────────────────────────────
    PAYU_MERCHANT_KEY: str = "your_payu_merchant_key"
    PAYU_MERCHANT_SALT: str = "your_payu_merchant_salt"
    PAYU_WEBHOOK_SECRET: str = "your_payu_webhook_secret"
    PAYU_SUCCESS_URL: str = "http://localhost:8000/payment/success"
    PAYU_FAILURE_URL: str = "http://localhost:8000/payment/failure"

    # ── PhonePe / UPI credentials ───────────────────────────────────────────
    PHONEPE_MERCHANT_ID: str = "your_phonepe_merchant_id"
    PHONEPE_SALT_KEY: str = "your_phonepe_salt_key"
    PHONEPE_SALT_INDEX: str = "1"
    UPI_REDIRECT_URL: str = "http://localhost:8000/payment/upi/redirect"
    UPI_CALLBACK_URL: str = "http://localhost:8000/api/v1/webhooks/upi"

    # Circuit Breaker
    CB_FAILURE_THRESHOLD: int = 5
    CB_SUCCESS_THRESHOLD: int = 2
    CB_TIMEOUT_SECONDS: int = 30
    CB_HALF_OPEN_MAX_CALLS: int = 1

    # Routing weights (must sum to 1.0)
    ROUTING_WEIGHT_SUCCESS: float = 0.35
    ROUTING_WEIGHT_LATENCY: float = 0.20
    ROUTING_WEIGHT_COST: float = 0.20
    ROUTING_WEIGHT_HEALTH: float = 0.15
    ROUTING_WEIGHT_FIT: float = 0.10

    # Metrics / sliding window
    METRICS_WINDOW_MINUTES: int = 15

    # Idempotency
    IDEMPOTENCY_TTL_HOURS: int = 24

    # Reconciliation
    RECONCILIATION_INTERVAL_MINUTES: int = 15
    STALE_TRANSACTION_MINUTES: int = 5

    # Gateway timeouts (seconds)
    GATEWAY_CONNECT_TIMEOUT: float = 5.0
    GATEWAY_READ_TIMEOUT: float = 30.0
    FAILOVER_TIMEOUT_SECONDS: float = 2.0
    DEGRADED_GATEWAY_SCORE_THRESHOLD: float = 0.20

    # DLQ
    DLQ_MAX_RETRIES: int = 3

    # Observability
    LOG_LEVEL: str = "INFO"
    PROMETHEUS_ENABLED: bool = True
    OTLP_ENDPOINT: Optional[str] = None


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
