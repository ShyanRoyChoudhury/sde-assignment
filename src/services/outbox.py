"""
Outbox pattern (SUBMISSION.md §8).

The analyse task inserts rows into `signal_outbox` in the same Postgres
transaction as the analysis result write. A separate dispatcher worker
drains the outbox with retries; terminal failures move to the DLQ table.

Two-pronged idempotency:
  1. State-before-action — write status='in_progress' before invoking the
     downstream service. A crash between the downstream call and the final
     status='dispatched' write leaves the row 'in_progress'; the
     reconciliation beat task picks it back up after STUCK_THRESHOLD seconds.
  2. Idempotency key — every outbox row carries an `idempotency_key UUID`
     passed to the downstream service in its native idempotency header.

Combined: even with worker crashes, beat-tick races, and dispatcher restarts,
side effects fire exactly once observable on the downstream.
"""

import json
import logging
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import text

from src.config import settings
from src.services.dlq import write_dlq_entry
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """Dispatch failure that should be retried per OUTBOX_RETRY_SCHEDULE."""


class PermanentError(Exception):
    """Dispatch failure that should NOT be retried — go straight to DLQ."""


# ──────────────────────────────────────────────────────────────────────────────
# Dispatch handlers — register one per dispatch_type
# ──────────────────────────────────────────────────────────────────────────────

DispatchHandler = Callable[[Dict[str, Any], str], None]
DISPATCH_HANDLERS: Dict[str, DispatchHandler] = {}


def register_handler(dispatch_type: str):
    """Decorator: register a sync function as the handler for a dispatch_type."""

    def _wrap(fn: DispatchHandler) -> DispatchHandler:
        DISPATCH_HANDLERS[dispatch_type] = fn
        return fn

    return _wrap


@register_handler("signal_jobs")
def _handle_signal_jobs(payload: Dict[str, Any], idempotency_key: str) -> None:
    """Mock — production POSTs to internal signal_jobs service with idempotency header."""
    logger.info(
        "outbox_dispatched_signal_jobs",
        extra={
            "interaction_id": payload.get("interaction_id"),
            "trace_id": payload.get("trace_id"),
            "campaign_id": payload.get("campaign_id"),
            "call_stage": payload.get("call_stage"),
            "idempotency_key": idempotency_key,
        },
    )


@register_handler("lead_stage")
def _handle_lead_stage(payload: Dict[str, Any], idempotency_key: str) -> None:
    """Mock — production runs UPDATE leads SET stage = $2 WHERE id = $1."""
    logger.info(
        "outbox_dispatched_lead_stage",
        extra={
            "lead_id": payload.get("lead_id"),
            "interaction_id": payload.get("interaction_id"),
            "new_stage": payload.get("call_stage"),
            "idempotency_key": idempotency_key,
        },
    )


@register_handler("crm_push")
def _handle_crm_push(payload: Dict[str, Any], idempotency_key: str) -> None:
    """Mock — production POSTs to customer's configured CRM webhook."""
    logger.info(
        "outbox_dispatched_crm_push",
        extra={
            "interaction_id": payload.get("interaction_id"),
            "crm_target": payload.get("crm_target"),
            "idempotency_key": idempotency_key,
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Insert (called from analyse task inside its transaction)
# ──────────────────────────────────────────────────────────────────────────────


async def insert_outbox_row(
    *,
    interaction_id: str,
    customer_id: str,
    trace_id: str,
    dispatch_type: str,
    payload: Dict[str, Any],
    session=None,
) -> int:
    """
    Insert a row into signal_outbox. If `session` is provided, the insert
    runs inside that session/transaction (the typical case — caller commits
    along with their own writes). Otherwise opens a new session and commits.
    """
    sql = text(
        """
        INSERT INTO signal_outbox
            (interaction_id, customer_id, trace_id, dispatch_type, payload)
        VALUES
            (CAST(:interaction_id AS uuid), CAST(:customer_id AS uuid),
             CAST(:trace_id AS uuid), :dispatch_type, CAST(:payload AS jsonb))
        RETURNING id
        """
    )
    params = {
        "interaction_id": interaction_id,
        "customer_id": customer_id,
        "trace_id": trace_id,
        "dispatch_type": dispatch_type,
        "payload": json.dumps(payload),
    }

    if session is not None:
        result = await session.execute(sql, params)
        return result.first()[0]

    async with async_session_factory() as new_session:
        result = await new_session.execute(sql, params)
        row_id = result.first()[0]
        await new_session.commit()
        return row_id


# ──────────────────────────────────────────────────────────────────────────────
# Beat task — pull a batch of pending rows and enqueue per-row dispatches
# ──────────────────────────────────────────────────────────────────────────────


async def fetch_dispatch_batch(limit: int) -> List[int]:
    """
    Return a batch of outbox row ids ready to dispatch. Uses
    FOR UPDATE SKIP LOCKED so multiple beat ticks don't fetch overlapping batches.
    """
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT id FROM signal_outbox
                WHERE status = 'pending' AND next_attempt_at <= NOW()
                ORDER BY next_attempt_at
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
                """
            ),
            {"limit": limit},
        )
        ids = [row[0] for row in result.fetchall()]
        await session.commit()
        return ids


# ──────────────────────────────────────────────────────────────────────────────
# Dispatch one row (called from the per-row Celery task)
# ──────────────────────────────────────────────────────────────────────────────


async def dispatch_row(row_id: int) -> None:
    """
    Dispatch a single outbox row. Handles all retry / DLQ bookkeeping.

    Assumes the dispatch handlers themselves are SYNC (network calls);
    we run them via an executor inside the Celery task. This module just
    handles the state machine.
    """
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT id, interaction_id::text, customer_id::text, trace_id::text,
                       dispatch_type, payload, idempotency_key::text,
                       status, attempt_count
                FROM signal_outbox WHERE id = :id
                FOR UPDATE
                """
            ),
            {"id": row_id},
        )
        row = result.first()
        if row is None:
            return
        if row.status != "pending":
            await session.commit()
            return

        # Mark in_progress before invoking the downstream service.
        await session.execute(
            text(
                """
                UPDATE signal_outbox
                SET status = 'in_progress', in_progress_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": row_id},
        )
        await session.commit()

    # Outside the transaction so the in_progress write is visible to other workers.
    handler = DISPATCH_HANDLERS.get(row.dispatch_type)
    if handler is None:
        await _move_to_dlq(
            row,
            reason="unknown_dispatch_type",
            error=f"no handler for {row.dispatch_type!r}",
            permanent=True,
        )
        return

    try:
        handler(row.payload, row.idempotency_key)
    except RetryableError as e:
        await _schedule_retry_or_dlq(row, str(e))
        return
    except PermanentError as e:
        await _move_to_dlq(row, reason="permanent_error", error=str(e), permanent=True)
        return
    except Exception as e:
        # Treat unknown exceptions as retryable. This is conservative; teams
        # tighten this on a per-handler basis as production data accrues.
        await _schedule_retry_or_dlq(row, f"{type(e).__name__}: {e}")
        return

    # Success.
    async with async_session_factory() as session:
        await session.execute(
            text(
                """
                UPDATE signal_outbox
                SET status = 'dispatched', dispatched_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": row_id},
        )
        await session.commit()


async def _schedule_retry_or_dlq(row, error_msg: str) -> None:
    next_attempt = row.attempt_count + 1
    if next_attempt >= settings.OUTBOX_MAX_RETRIES:
        await _move_to_dlq(
            row,
            reason="max_retries_exhausted",
            error=error_msg,
            permanent=False,
        )
        return

    delay_seconds = settings.OUTBOX_RETRY_SCHEDULE_SECONDS[next_attempt]
    async with async_session_factory() as session:
        await session.execute(
            text(
                """
                UPDATE signal_outbox
                SET status = 'pending',
                    attempt_count = :attempt_count,
                    next_attempt_at = NOW() + (:delay || ' seconds')::interval,
                    last_error = :error
                WHERE id = :id
                """
            ),
            {
                "id": row.id,
                "attempt_count": next_attempt,
                "delay": str(delay_seconds),
                "error": error_msg,
            },
        )
        await session.commit()
    logger.warning(
        "outbox_retry_scheduled",
        extra={
            "outbox_id": row.id,
            "attempt": next_attempt,
            "delay_seconds": delay_seconds,
            "error": error_msg,
        },
    )


async def _move_to_dlq(row, *, reason: str, error: str, permanent: bool) -> None:
    error_history = [{"attempt": row.attempt_count + 1, "error": error}]
    await write_dlq_entry(
        source="outbox",
        reason=reason,
        original_payload={
            "interaction_id": row.interaction_id,
            "customer_id": row.customer_id,
            "trace_id": row.trace_id,
            "dispatch_type": row.dispatch_type,
            "payload": row.payload,
        },
        error_history=error_history,
        interaction_id=row.interaction_id,
        customer_id=row.customer_id,
        trace_id=row.trace_id,
    )
    async with async_session_factory() as session:
        await session.execute(
            text(
                "UPDATE signal_outbox SET status = 'failed', last_error = :error WHERE id = :id"
            ),
            {"id": row.id, "error": error},
        )
        await session.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Reconciliation — recover stuck `in_progress` rows (§8.5)
# ──────────────────────────────────────────────────────────────────────────────


async def reconcile_stuck_in_progress(threshold_seconds: int) -> int:
    """
    Reset rows stuck in `in_progress` beyond the threshold back to `pending`.
    Returns count reset. Called periodically by a Celery beat task.
    """
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                """
                UPDATE signal_outbox
                SET status = 'pending',
                    in_progress_at = NULL,
                    next_attempt_at = NOW()
                WHERE status = 'in_progress'
                  AND in_progress_at < NOW() - (:secs || ' seconds')::interval
                RETURNING id
                """
            ),
            {"secs": str(threshold_seconds)},
        )
        ids = [row[0] for row in result.fetchall()]
        await session.commit()

    if ids:
        logger.warning(
            "outbox_stuck_rows_reset",
            extra={"count": len(ids), "ids_sample": ids[:10]},
        )
    return len(ids)
