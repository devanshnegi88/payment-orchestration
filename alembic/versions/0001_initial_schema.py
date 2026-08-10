"""Initial schema — all tables.

Revision ID: 0001
Create Date: 2024-01-01
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Extensions
    op.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")

    # Enums
    op.execute("""
        CREATE TYPE transaction_state_enum AS ENUM (
            'CREATED','ABANDONED','ROUTE_SELECTED','ROUTE_FAILED',
            'AUTH_INITIATED','AUTHORISED','AUTH_FAILED','AUTH_TIMEOUT',
            'AUTH_EXPIRED','CAPTURE_INITIATED','CAPTURED','PARTIALLY_CAPTURED',
            'CAPTURE_FAILED','VOID_INITIATED','VOIDED','REFUND_INITIATED',
            'REFUNDED','PARTIALLY_REFUNDED','REFUND_FAILED','FAILED',
            'SETTLED','DISPUTE_OPENED','DISPUTE_RESOLVED'
        )
    """)
    op.execute("""
        CREATE TYPE transaction_event_enum AS ENUM (
            'PAYMENT_INITIATED','PAYMENT_ABANDONED','CAPTURE_REQUESTED',
            'VOID_REQUESTED','REFUND_REQUESTED','ROUTE_DECISION_MADE',
            'ROUTE_DECISION_FAILED','GATEWAY_AUTH_CALLED','GATEWAY_AUTH_SUCCESS',
            'GATEWAY_AUTH_DECLINED','GATEWAY_AUTH_TIMEOUT','GATEWAY_AUTH_EXPIRED',
            'GATEWAY_CAPTURE_CALLED','GATEWAY_CAPTURE_SUCCESS','GATEWAY_CAPTURE_PARTIAL',
            'GATEWAY_CAPTURE_FAILED','GATEWAY_VOID_CALLED','GATEWAY_VOID_SUCCESS',
            'GATEWAY_REFUND_CALLED','GATEWAY_REFUND_SUCCESS','GATEWAY_REFUND_PARTIAL',
            'GATEWAY_REFUND_FAILED','SETTLEMENT_CONFIRMED','MAX_RETRIES_EXCEEDED',
            'WEBHOOK_RECEIVED','RECONCILIATION_OVERRIDE','DISPUTE_RAISED',
            'DISPUTE_CLOSED','REJECTED_TRANSITION'
        )
    """)

    # transactions
    op.create_table("transactions",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("merchant_id", sa.String(100), nullable=False),
        sa.Column("merchant_order_id", sa.String(255), nullable=False),
        sa.Column("amount_paise", sa.BigInteger(), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False, server_default="INR"),
        sa.Column("authorized_amount_paise", sa.BigInteger(), nullable=True),
        sa.Column("captured_amount_paise", sa.BigInteger(), nullable=True, server_default="0"),
        sa.Column("refunded_amount_paise", sa.BigInteger(), nullable=True, server_default="0"),
        sa.Column("payment_method", sa.String(50), nullable=False),
        sa.Column("state", sa.Enum(name="transaction_state_enum"), nullable=False,
                  server_default="CREATED"),
        sa.Column("gateway", sa.String(50), nullable=True),
        sa.Column("gateway_reference", sa.String(255), nullable=True),
        sa.Column("gateway_order_id", sa.String(255), nullable=True),
        sa.Column("idempotency_key", sa.String(255), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("customer_id", sa.String(255), nullable=True),
        sa.Column("customer_email", sa.String(255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("trace_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("amount_paise > 0", name="chk_amount_positive"),
        sa.UniqueConstraint("merchant_id", "merchant_order_id", name="uq_merchant_order"),
        sa.UniqueConstraint("merchant_id", "idempotency_key", name="uq_merchant_idempotency"),
    )
    op.create_index("idx_transactions_state", "transactions", ["state"])
    op.create_index("idx_transactions_gateway_ref", "transactions", ["gateway_reference"])
    op.create_index("idx_transactions_merchant_order", "transactions",
                    ["merchant_id", "merchant_order_id"])
    op.create_index("idx_transactions_created_at", "transactions", ["created_at"])
    op.create_index("idx_transactions_gateway_state", "transactions", ["gateway", "state"])

    # transaction_state_log  (INSERT-only audit trail)
    op.create_table("transaction_state_log",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=False),
                  sa.ForeignKey("transactions.id"), nullable=False),
        sa.Column("from_state", sa.Enum(name="transaction_state_enum"), nullable=False),
        sa.Column("to_state", sa.Enum(name="transaction_state_enum"), nullable=False),
        sa.Column("event", sa.Enum(name="transaction_event_enum"), nullable=False),
        sa.Column("gateway_reference", sa.String(255), nullable=True),
        sa.Column("gateway_response", postgresql.JSONB(), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("created_by", sa.String(100), nullable=False, server_default="system"),
    )
    op.create_index("idx_state_log_transaction", "transaction_state_log", ["transaction_id"])
    op.create_index("idx_state_log_created_at", "transaction_state_log", ["created_at"])

    # idempotency_keys
    op.create_table("idempotency_keys",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("merchant_id", sa.String(100), nullable=False),
        sa.Column("key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="PROCESSING"),
        sa.Column("response_code", sa.Integer(), nullable=True),
        sa.Column("response_body", postgresql.JSONB(), nullable=True),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW() + INTERVAL '24 hours'")),
        sa.UniqueConstraint("merchant_id", "key", name="uq_merchant_idempotency_key"),
        sa.CheckConstraint("status IN ('PROCESSING','COMPLETED','FAILED')",
                           name="chk_idempotency_status"),
    )
    op.create_index("idx_idempotency_expires", "idempotency_keys", ["expires_at"])

    # processed_webhook_events
    op.create_table("processed_webhook_events",
        sa.Column("event_id", sa.String(255), nullable=False),
        sa.Column("gateway", sa.String(50), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("gateway", "event_id"),
    )
    op.create_index("idx_webhook_events_transaction", "processed_webhook_events", ["transaction_id"])

    # webhook_queue
    op.create_table("webhook_queue",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("gateway", sa.String(50), nullable=False),
        sa.Column("event_id", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("raw_body", sa.Text(), nullable=False),
        sa.Column("signature", sa.Text(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="PENDING"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('PENDING','PROCESSING','COMPLETED','FAILED','DLQ')",
                           name="chk_webhook_queue_status"),
    )
    op.create_index("idx_webhook_queue_status", "webhook_queue", ["status", "created_at"])

    # dead_letter_queue
    op.create_table("dead_letter_queue",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("original_queue_id", sa.BigInteger(), nullable=False),
        sa.Column("gateway", sa.String(50), nullable=False),
        sa.Column("event_id", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("raw_body", sa.Text(), nullable=False),
        sa.Column("signature", sa.Text(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by", sa.String(100), nullable=True),
    )

    # gateway_health_metrics
    op.create_table("gateway_health_metrics",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("gateway", sa.String(50), nullable=False),
        sa.Column("payment_method", sa.String(50), nullable=True),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_requests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("successful_requests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_requests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("timeout_requests", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("avg_latency_ms", sa.Integer(), nullable=True),
        sa.Column("p50_latency_ms", sa.Integer(), nullable=True),
        sa.Column("p95_latency_ms", sa.Integer(), nullable=True),
        sa.Column("p99_latency_ms", sa.Integer(), nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
    )
    op.create_index("idx_health_metrics_gateway_time", "gateway_health_metrics",
                    ["gateway", "window_start"])

    # gateway_config
    op.create_table("gateway_config",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("gateway_name", sa.String(50), nullable=False, unique=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("base_url", sa.String(500), nullable=False),
        sa.Column("supports_auth_capture", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("supports_partial_capture", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("supports_partial_refund", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("supported_payment_methods", postgresql.JSONB(), nullable=False,
                  server_default="'[]'"),
        sa.Column("supported_currencies", postgresql.JSONB(), nullable=False,
                  server_default="'[]'"),
        sa.Column("cb_failure_threshold", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("cb_success_threshold", sa.Integer(), nullable=False, server_default="2"),
        sa.Column("cb_timeout_seconds", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("fee_percentage", sa.Float(), nullable=False, server_default="2.0"),
        sa.Column("fee_fixed_paise", sa.Integer(), nullable=False, server_default="200"),
        sa.Column("rate_limit_per_second", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("auth_timeout_seconds", sa.Integer(), nullable=False, server_default="30"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
    )

    # routing_config
    op.create_table("routing_config",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("config_key", sa.String(100), nullable=False, unique=True),
        sa.Column("config_value", postgresql.JSONB(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("updated_by", sa.String(100), nullable=True),
    )

    # gateway_routes
    op.create_table("gateway_routes",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=False),
                  sa.ForeignKey("transactions.id"), nullable=False),
        sa.Column("gateway", sa.String(50), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("composite_score", sa.Float(), nullable=True),
        sa.Column("success_rate_score", sa.Float(), nullable=True),
        sa.Column("latency_score", sa.Float(), nullable=True),
        sa.Column("cost_score", sa.Float(), nullable=True),
        sa.Column("health_score", sa.Float(), nullable=True),
        sa.Column("fit_score", sa.Float(), nullable=True),
        sa.Column("outcome", sa.String(50), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("error_type", sa.String(100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
    )
    op.create_index("idx_gateway_routes_transaction", "gateway_routes", ["transaction_id"])

    # refunds
    op.create_table("refunds",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=False),
                  sa.ForeignKey("transactions.id"), nullable=False),
        sa.Column("amount_paise", sa.BigInteger(), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False, server_default="INR"),
        sa.Column("state", sa.String(50), nullable=False, server_default="INITIATED"),
        sa.Column("gateway", sa.String(50), nullable=False),
        sa.Column("gateway_refund_id", sa.String(255), nullable=True, unique=True),
        sa.Column("reason", sa.String(500), nullable=True),
        sa.Column("idempotency_key", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.CheckConstraint("amount_paise > 0", name="chk_refund_amount_positive"),
    )
    op.create_index("idx_refunds_transaction", "refunds", ["transaction_id"])

    # reconciliation tables
    op.create_table("reconciliation_runs",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="RUNNING"),
        sa.Column("transactions_checked", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("discrepancies_found", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("anomalies_found", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("triggered_by", sa.String(100), nullable=False, server_default="scheduler"),
    )
    op.create_table("reconciliation_log",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("run_id", postgresql.UUID(as_uuid=False),
                  sa.ForeignKey("reconciliation_runs.id"), nullable=False),
        sa.Column("transaction_id", postgresql.UUID(as_uuid=False),
                  sa.ForeignKey("transactions.id"), nullable=False),
        sa.Column("gateway", sa.String(50), nullable=False),
        sa.Column("internal_state", sa.String(50), nullable=False),
        sa.Column("gateway_state", sa.String(50), nullable=True),
        sa.Column("discrepancy_type", sa.String(100), nullable=True),
        sa.Column("internal_amount_paise", sa.BigInteger(), nullable=True),
        sa.Column("gateway_amount_paise", sa.BigInteger(), nullable=True),
        sa.Column("is_anomaly", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("resolution", sa.String(100), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
    )
    op.create_index("idx_recon_log_run", "reconciliation_log", ["run_id"])
    op.create_index("idx_recon_log_anomaly", "reconciliation_log", ["is_anomaly"])

    # api_keys (real merchant key store)
    op.create_table("api_keys",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True,
                  server_default=sa.text("uuid_generate_v4()")),
        sa.Column("merchant_id", sa.String(100), nullable=False),
        sa.Column("key_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("name", sa.String(255), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_api_keys_hash", "api_keys", ["key_hash"])
    op.create_index("idx_api_keys_merchant", "api_keys", ["merchant_id"])


def downgrade() -> None:
    for t in ["api_keys", "reconciliation_log", "reconciliation_runs", "refunds",
              "gateway_routes", "routing_config", "gateway_config",
              "gateway_health_metrics", "dead_letter_queue", "webhook_queue",
              "processed_webhook_events", "idempotency_keys",
              "transaction_state_log", "transactions"]:
        op.drop_table(t)
    op.execute("DROP TYPE IF EXISTS transaction_event_enum")
    op.execute("DROP TYPE IF EXISTS transaction_state_enum")
