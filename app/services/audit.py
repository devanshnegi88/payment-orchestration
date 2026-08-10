"""Audit service — structured logging wrapper."""
import structlog

logger = structlog.get_logger(__name__)


class AuditService:
    def log(self, event: str, **kwargs):
        logger.info(event, **kwargs)
