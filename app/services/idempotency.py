"""
Idempotency Service

Implements the idempotency pattern from Section A4:
1. Check if key exists → return cached response if COMPLETED
2. Acquire advisory lock (prevents concurrent duplicate requests — FS-09)
3. Insert key with PROCESSING status
4. Process request
5. Update key with COMPLETED status + cached response

Uses PostgreSQL advisory locks (pg_advisory_xact_lock) scoped to (merchant_id, key).
Idempotency keys are scoped per merchant to prevent cross-tenant collisions (FS-13).
"""
import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional, Any
import structlog
from sqlalchemy import select, delete, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.transaction import IdempotencyKey
from app.domain.exceptions import (
    IdempotencyConflictError,
    IdempotencyRequestMismatchError,
)

logger = structlog.get_logger(__name__)


def compute_request_hash(request_body: dict) -> str:
    """Deterministic SHA-256 hash of the request body (sorted keys for stability)."""
    canonical = json.dumps(request_body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class IdempotencyService:
    """
    Provides idempotency guarantees for payment operations.
    Thread-safe and distributed-safe via database advisory locks.
    """

    def __init__(self, db: AsyncSession):
        self.db = db

    async def acquire_lock(
        self,
        merchant_id: str,
        idempotency_key: str,
        request_body: dict,
    ) -> Optional[dict]:
        """
        Attempt to acquire an idempotency lock.

        Returns:
            - None: lock acquired, proceed with processing
            - dict: cached response from a previous completed request (replay it)

        Raises:
            IdempotencyConflictError: another request with this key is in-flight
            IdempotencyRequestMismatchError: same key, different request body
        """
        request_hash = compute_request_hash(request_body)

        # Step 1: Acquire PostgreSQL advisory lock (transaction-scoped)
        # This prevents race conditions between concurrent requests with the same key
        lock_key = hashlib.md5(f"idem_{merchant_id}_{idempotency_key}".encode()).hexdigest()
        lock_int = int(lock_key[:8], 16)  # Convert first 8 hex chars to int
        await self.db.execute(text(f"SELECT pg_advisory_xact_lock({lock_int})"))

        # Step 2: Check if this key has been seen before
        result = await self.db.execute(
            select(IdempotencyKey).where(
                and_(
                    IdempotencyKey.merchant_id == merchant_id,
                    IdempotencyKey.key == idempotency_key,
                )
            )
        )
        existing = result.scalar_one_or_none()

        if existing is not None:
            # Validate request body matches (prevent key reuse with different payload)
            if existing.request_hash != request_hash:
                logger.warning(
                    "idempotency_hash_mismatch",
                    merchant_id=merchant_id,
                    key=idempotency_key,
                )
                raise IdempotencyRequestMismatchError(idempotency_key)

            if existing.status == "PROCESSING":
                # Another in-flight request — return 409
                raise IdempotencyConflictError(idempotency_key, "PROCESSING")

            if existing.status == "COMPLETED":
                # Return the cached response (idempotent replay)
                logger.info(
                    "idempotency_replay",
                    merchant_id=merchant_id,
                    key=idempotency_key,
                )
                return existing.response_body

            # FAILED status — allow retry by falling through to insert
            # Delete the failed entry so we can re-insert
            await self.db.delete(existing)
            await self.db.flush()

        # Step 3: Insert the key as PROCESSING
        expires_at = datetime.utcnow() + timedelta(hours=24)
        key_record = IdempotencyKey(
            merchant_id=merchant_id,
            key=idempotency_key,
            request_hash=request_hash,
            status="PROCESSING",
            expires_at=expires_at,
        )
        self.db.add(key_record)
        await self.db.flush()  # Persist within transaction (advisory lock is active)

        logger.info(
            "idempotency_lock_acquired",
            merchant_id=merchant_id,
            key=idempotency_key,
        )
        return None  # Lock acquired, proceed

    async def complete(
        self,
        merchant_id: str,
        idempotency_key: str,
        transaction_id: str,
        response_code: int,
        response_body: dict,
    ) -> None:
        """Mark an idempotency key as COMPLETED with the cached response."""
        result = await self.db.execute(
            select(IdempotencyKey).where(
                and_(
                    IdempotencyKey.merchant_id == merchant_id,
                    IdempotencyKey.key == idempotency_key,
                )
            )
        )
        key_record = result.scalar_one_or_none()

        if key_record:
            key_record.status = "COMPLETED"
            key_record.transaction_id = transaction_id
            key_record.response_code = response_code
            key_record.response_body = response_body
            await self.db.flush()

        logger.info(
            "idempotency_completed",
            merchant_id=merchant_id,
            key=idempotency_key,
            transaction_id=transaction_id,
        )

    async def fail(
        self,
        merchant_id: str,
        idempotency_key: str,
        error: str,
    ) -> None:
        """Mark an idempotency key as FAILED (allows retry)."""
        result = await self.db.execute(
            select(IdempotencyKey).where(
                and_(
                    IdempotencyKey.merchant_id == merchant_id,
                    IdempotencyKey.key == idempotency_key,
                )
            )
        )
        key_record = result.scalar_one_or_none()

        if key_record:
            key_record.status = "FAILED"
            await self.db.flush()

        logger.warning(
            "idempotency_failed",
            merchant_id=merchant_id,
            key=idempotency_key,
            error=error,
        )

    async def cleanup_expired(self) -> int:
        """
        Background job: delete expired idempotency keys.
        Should run hourly.
        """
        result = await self.db.execute(
            delete(IdempotencyKey).where(
                and_(
                    IdempotencyKey.expires_at < datetime.utcnow(),
                    IdempotencyKey.status != "COMPLETED",
                )
            )
        )
        count = result.rowcount
        if count > 0:
            logger.info("idempotency_keys_cleaned", count=count)
        return count
