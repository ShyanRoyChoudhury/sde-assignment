"""
Structured logging wrapper (SUBMISSION.md §9.3).

Every log line MUST include interaction_id, trace_id, customer_id (when known),
and stage. Every log line MUST NOT include transcript content, phone numbers,
names, emails, or any free-text PII (per A8).

Defence in depth: this wrapper silently drops a known set of forbidden field
names. A separate CI grep check (out of scope here) catches code that bypasses
the wrapper.
"""

import logging
from typing import Any, Optional

REDACTED_KEYS = frozenset({
    "transcript_text",
    "conversation_data",
    "transcript",
    "content",
    "lead_phone",
    "lead_email",
    "lead_name",
    "phone",
    "email",
    "name",
})


class StructuredLogger:
    def __init__(self, name: str):
        self._logger = logging.getLogger(name)

    def _emit(
        self,
        level: int,
        event: str,
        *,
        interaction_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        customer_id: Optional[str] = None,
        stage: Optional[str] = None,
        **extra: Any,
    ) -> None:
        sanitised = {k: v for k, v in extra.items() if k not in REDACTED_KEYS}
        sanitised.update({
            "interaction_id": str(interaction_id) if interaction_id else None,
            "trace_id": str(trace_id) if trace_id else None,
            "customer_id": str(customer_id) if customer_id else None,
            "stage": stage,
        })
        self._logger.log(level, event, extra=sanitised)

    def debug(self, event: str, **kw: Any) -> None:
        self._emit(logging.DEBUG, event, **kw)

    def info(self, event: str, **kw: Any) -> None:
        self._emit(logging.INFO, event, **kw)

    def warning(self, event: str, **kw: Any) -> None:
        self._emit(logging.WARNING, event, **kw)

    def error(self, event: str, **kw: Any) -> None:
        self._emit(logging.ERROR, event, **kw)

    def exception(self, event: str, **kw: Any) -> None:
        # Same level as error but with traceback attached.
        sanitised = {k: v for k, v in kw.items() if k not in REDACTED_KEYS}
        self._logger.exception(event, extra=sanitised)


def get_structured_logger(name: str) -> StructuredLogger:
    return StructuredLogger(name)
