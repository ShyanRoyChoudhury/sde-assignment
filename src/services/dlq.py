"""
Dead-letter queue writer (SUBMISSION.md §8.6 / §8.7).

Replaces the bespoke `retry_queue` module (which only enqueued — its dequeue
side had zero callers, so failed tasks accumulated in Redis forever). The DLQ
lives in Postgres so failures survive a Redis bounce, and replay is a single
SQL operation.

Sources that can dead-letter:
  - 'outbox'   — dispatch retries exhausted
  - 'analysis' — non-retryable exception OR MAX_DEFER_ATTEMPTS exceeded
  - 'triage'   — crash with unhandlable exception (rare)

Recording failures do NOT come here — they're captured inline on the
interactions row as `recording_status`.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


async def write_dlq_entry(
    *,
    source: str,
    reason: str,
    original_payload: Dict[str, Any],
    error_history: List[Dict[str, Any]],
    interaction_id: Optional[str] = None,
    customer_id: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> int:
    """
    Insert a row into dead_letter_queue. Returns the new row id.

    error_history is a list of `{attempt, error_msg, occurred_at}` dicts.
    original_payload contains everything needed to replay (e.g., the celery
    task args, or the outbox row contents).
    """
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                """
                INSERT INTO dead_letter_queue
                    (interaction_id, customer_id, trace_id,
                     source, reason, error_history, original_payload, status)
                VALUES
                    (:interaction_id, :customer_id, :trace_id,
                     :source, :reason, CAST(:error_history AS jsonb),
                     CAST(:original_payload AS jsonb), 'pending_review')
                RETURNING id
                """
            ),
            {
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "trace_id": trace_id,
                "source": source,
                "reason": reason,
                "error_history": json.dumps(error_history),
                "original_payload": json.dumps(original_payload),
            },
        )
        row = result.first()
        await session.commit()
        dlq_id = row[0]

    logger.error(
        "dead_lettered",
        extra={
            "dlq_id": dlq_id,
            "interaction_id": interaction_id,
            "trace_id": trace_id,
            "source": source,
            "reason": reason,
        },
    )
    return dlq_id


async def replay(dlq_id: int, replayed_by: str, notes: Optional[str] = None) -> None:
    """
    Replay a DLQ entry by re-enqueueing the original task. Marks the row as
    replayed. Idempotent — fetching a non-pending_review entry raises.

    Imports inside the function to avoid circular imports (celery tasks import
    from this module too).
    """
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT source, original_payload, status
                FROM dead_letter_queue WHERE id = :id
                """
            ),
            {"id": dlq_id},
        )
        row = result.first()
        if row is None:
            raise ValueError(f"dlq_id {dlq_id} not found")

        source, original_payload, status = row
        if status != "pending_review":
            raise ValueError(
                f"dlq_id {dlq_id} status is {status!r}, not pending_review"
            )

    # Re-enqueue based on source
    if source == "outbox":
        from src.services.outbox import insert_outbox_row
        await insert_outbox_row(
            interaction_id=original_payload["interaction_id"],
            customer_id=original_payload["customer_id"],
            trace_id=original_payload["trace_id"],
            dispatch_type=original_payload["dispatch_type"],
            payload=original_payload["payload"],
        )
    elif source == "analysis":
        from src.tasks.celery_tasks import analyse_task
        analyse_task.apply_async(
            args=[original_payload],
            queue=original_payload.get("queue", "postcall_hot"),
        )
    elif source == "triage":
        from src.tasks.celery_tasks import triage_task
        triage_task.apply_async(args=[original_payload], queue="triage")
    else:
        raise ValueError(f"unknown DLQ source {source!r}")

    async with async_session_factory() as session:
        await session.execute(
            text(
                """
                UPDATE dead_letter_queue
                SET status = 'replayed',
                    reviewed_by = :replayed_by,
                    reviewed_at = NOW(),
                    review_notes = :notes
                WHERE id = :id
                """
            ),
            {"id": dlq_id, "replayed_by": replayed_by, "notes": notes},
        )
        await session.commit()

    logger.info(
        "dlq_replayed",
        extra={"dlq_id": dlq_id, "replayed_by": replayed_by, "source": source},
    )
