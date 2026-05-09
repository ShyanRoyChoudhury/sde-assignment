"""
Celery tasks for the post-call processing pipeline (SUBMISSION.md §3.2).

Each stage is its own task on its own queue so workers can be scaled
independently:

    triage        →  CPU-bound, ~1ms per call, trivially parallel
    postcall_hot  →  I/O-bound, rate-limited, reserved capacity first
    postcall_cold →  I/O-bound, rate-limited, burst-only, defers when contested
    recording     →  I/O-bound, polls Exotel with bounded backoff
    outbox        →  I/O-bound, fans out to downstream services with retry

Beat tasks:
    sweeper             →  reclaim stranded TPM ledger reservations
    pressure_publish    →  refresh the platform_pressure gauge
    outbox_beat         →  enqueue per-row dispatch tasks
    outbox_reconcile    →  reset outbox rows stuck in 'in_progress'
    recording_reconcile →  reset interactions stuck in 'uploading'
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict

from sqlalchemy import text

from src.config import settings
from src.services.event_log import Stage, Status, write as write_event
from src.services.outbox import (
    dispatch_row,
    fetch_dispatch_batch,
    insert_outbox_row,
    reconcile_stuck_in_progress,
)
from src.services.post_call_processor import (
    BudgetDenied,
    PostCallContext,
    post_call_processor,
)
from src.services.pressure_gauge import publish_pressure
from src.services.recording import (
    FetchStatus,
    fetch_exotel_recording,
    find_stuck_uploads,
    poll_delay_for_attempt,
    update_recording_status,
    upload_to_s3,
)
from src.services.tpm_ledger import tpm_ledger
from src.services.triage import classify
from src.tasks.celery_app import celery_app
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


def _run_async(coro):
    """Run an async coroutine inside a fresh event loop. Used by sync Celery tasks."""
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ──────────────────────────────────────────────────────────────────────────────
# Triage task — first stage; routes into skip / hot / cold
# ──────────────────────────────────────────────────────────────────────────────


@celery_app.task(name="triage_task", bind=True, queue=settings.QUEUE_TRIAGE,
                 acks_late=True, max_retries=3, default_retry_delay=10)
def triage_task(self, payload: Dict[str, Any]) -> None:
    try:
        _run_async(_triage_async(payload))
    except Exception as e:
        logger.exception("triage_task_failed",
                         extra={"interaction_id": payload.get("interaction_id"),
                                "trace_id": payload.get("trace_id"),
                                "error": str(e)})
        # Triage failures are rare. Retry a few times; if all retries fail,
        # the failed task itself goes to the standard Celery DLQ via acks_late
        # behaviour. Adding a DLQ row is handled by the analyse path.
        raise self.retry(exc=e)


async def _triage_async(payload: Dict[str, Any]) -> None:
    interaction_id = payload["interaction_id"]
    trace_id = payload["trace_id"]

    # Read the interaction row to get transcript + customer/lead/campaign IDs.
    row = await _load_interaction(interaction_id)
    if row is None:
        logger.warning("triage_interaction_not_found",
                       extra={"interaction_id": interaction_id, "trace_id": trace_id})
        return

    transcript = (row["conversation_data"] or {}).get("transcript", []) or []
    transcript_text = "\n".join(
        f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
        for turn in transcript
    )
    turn_count = len(transcript)

    verdict = classify(transcript_text, turn_count)

    await _update_interaction_lane(interaction_id, verdict.lane, verdict)
    await write_event(
        interaction_id=interaction_id,
        trace_id=trace_id,
        stage=Stage.TRIAGED,
        status=Status.SUCCESS,
        source="classifier",
        metadata={
            "lane": verdict.lane,
            "suggested_call_stage": verdict.suggested_call_stage,
            "matched_rules": verdict.matched_rules,
        },
    )

    if verdict.lane == "skip":
        await _emit_skip_result(row, trace_id, verdict)
        return

    # Hot or cold — enqueue the analyse task on the appropriate queue.
    queue = settings.QUEUE_HOT if verdict.lane == "hot" else settings.QUEUE_COLD
    analyse_payload = {
        **payload,
        "lane": verdict.lane,
        "suggested_call_stage": verdict.suggested_call_stage,
        "lead_id": str(row["lead_id"]),
        "campaign_id": str(row["campaign_id"]),
        "customer_id": str(row["customer_id"]),
        "agent_id": str(row["agent_id"]),
        "transcript_text": transcript_text,
        "conversation_data": row["conversation_data"] or {},
        "queue": queue,
    }
    analyse_task.apply_async(args=[analyse_payload], queue=queue)


async def _emit_skip_result(row: Dict[str, Any], trace_id: str, verdict) -> None:
    """Skip path — synthesise an analysis result without an LLM call (§6.3)."""
    interaction_id = str(row["id"])
    customer_id = str(row["customer_id"])

    synthetic_payload = {
        "call_stage": verdict.suggested_call_stage,
        "entities": {},
        "summary": f"Auto-classified: {verdict.suggested_call_stage}",
        "tokens_used": 0,
        "provider": "classifier",
        "model": "rules-v1",
        "analyzed_at": datetime.utcnow().isoformat(),
    }

    async with async_session_factory() as session:
        # Idempotent write — same anchor as the LLM path.
        await session.execute(
            text(
                """
                UPDATE interactions
                SET interaction_metadata = interaction_metadata || CAST(:patch AS jsonb),
                    analyzed_at = NOW(),
                    status = 'ANALYSIS_SKIPPED',
                    updated_at = NOW()
                WHERE id = CAST(:id AS uuid) AND analyzed_at IS NULL
                """
            ),
            {"id": interaction_id, "patch": json.dumps(synthetic_payload)},
        )

        # Outbox rows in the same transaction.
        await insert_outbox_row(
            interaction_id=interaction_id,
            customer_id=customer_id,
            trace_id=trace_id,
            dispatch_type="signal_jobs",
            payload={
                "interaction_id": interaction_id,
                "session_id": str(row["session_id"]),
                "campaign_id": str(row["campaign_id"]),
                "trace_id": trace_id,
                "call_stage": verdict.suggested_call_stage,
                "analysis_result": synthetic_payload,
            },
            session=session,
        )
        await insert_outbox_row(
            interaction_id=interaction_id,
            customer_id=customer_id,
            trace_id=trace_id,
            dispatch_type="lead_stage",
            payload={
                "lead_id": str(row["lead_id"]),
                "interaction_id": interaction_id,
                "trace_id": trace_id,
                "call_stage": verdict.suggested_call_stage,
            },
            session=session,
        )
        await session.commit()

    await write_event(
        interaction_id=interaction_id,
        trace_id=trace_id,
        stage=Stage.ANALYZED,
        status=Status.SUCCESS,
        source="classifier",
        metadata={
            "tokens_used": 0,
            "model": "rules-v1",
            "call_stage": verdict.suggested_call_stage,
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Analyse task — LLM call, gated by TPM ledger
# ──────────────────────────────────────────────────────────────────────────────


@celery_app.task(name="analyse_task", bind=True, acks_late=True,
                 max_retries=settings.MAX_DEFER_ATTEMPTS,
                 default_retry_delay=settings.MIN_RETRY_BACKOFF_MS // 1000)
def analyse_task(self, payload: Dict[str, Any]) -> None:
    try:
        _run_async(_analyse_async(payload, self.request.retries))
    except BudgetDenied as e:
        # Defer per §4.7 — the LLM was never called. Reschedule with the
        # ledger's recommended backoff.
        countdown = max(1, e.retry_after_ms // 1000)
        _record_defer(payload, e.reason, e.retry_after_ms, self.request.retries)
        raise self.retry(exc=e, countdown=countdown,
                         max_retries=settings.MAX_DEFER_ATTEMPTS)
    except Exception as e:
        logger.exception("analyse_task_failed",
                         extra={"interaction_id": payload.get("interaction_id"),
                                "trace_id": payload.get("trace_id"),
                                "error": str(e)})
        # Non-budget failures: limited retries; on exhaustion, DLQ.
        if self.request.retries >= 3:
            _run_async(_dlq_analysis_failure(payload, str(e), self.request.retries))
            return
        raise self.retry(exc=e, countdown=60)


async def _analyse_async(payload: Dict[str, Any], attempt: int) -> None:
    ctx = PostCallContext(
        interaction_id=payload["interaction_id"],
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=payload["customer_id"],
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
        trace_id=payload["trace_id"],
        classifier_hint=payload.get("suggested_call_stage"),
    )
    lane = payload.get("lane", "hot")

    await write_event(
        interaction_id=ctx.interaction_id,
        trace_id=ctx.trace_id,
        stage=Stage.ANALYZE_ACQUIRED,
        status=Status.IN_PROGRESS,
        source="ledger",
        metadata={"customer_id": ctx.customer_id, "lane": lane, "attempt": attempt},
    )

    result = await post_call_processor.process(ctx, lane=lane)

    # Outbox rows for downstream side effects, atomic with the analysis result.
    async with async_session_factory() as session:
        await insert_outbox_row(
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            trace_id=ctx.trace_id,
            dispatch_type="signal_jobs",
            payload={
                "interaction_id": ctx.interaction_id,
                "session_id": ctx.session_id,
                "campaign_id": ctx.campaign_id,
                "trace_id": ctx.trace_id,
                "call_stage": result.call_stage,
                "analysis_result": result.raw_response,
            },
            session=session,
        )
        await insert_outbox_row(
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            trace_id=ctx.trace_id,
            dispatch_type="lead_stage",
            payload={
                "lead_id": ctx.lead_id,
                "interaction_id": ctx.interaction_id,
                "trace_id": ctx.trace_id,
                "call_stage": result.call_stage,
            },
            session=session,
        )
        await session.commit()

    await write_event(
        interaction_id=ctx.interaction_id,
        trace_id=ctx.trace_id,
        stage=Stage.ANALYZED,
        status=Status.SUCCESS,
        source="llm",
        metadata={
            "tokens_used": result.tokens_used,
            "latency_ms": result.latency_ms,
            "model": result.model,
            "call_stage": result.call_stage,
        },
    )
    await write_event(
        interaction_id=ctx.interaction_id,
        trace_id=ctx.trace_id,
        stage=Stage.OUTBOX_INSERTED,
        status=Status.SUCCESS,
        source="analyse",
        metadata={"dispatch_types": ["signal_jobs", "lead_stage"]},
    )


def _record_defer(payload, reason, retry_after_ms, attempt):
    _run_async(write_event(
        interaction_id=payload["interaction_id"],
        trace_id=payload["trace_id"],
        stage=Stage.ANALYZE_DEFERRED,
        status=Status.DEFERRED,
        source="ledger",
        metadata={"reason": reason, "retry_after_ms": retry_after_ms, "attempt": attempt},
    ))


async def _dlq_analysis_failure(payload: Dict[str, Any], error: str, attempt: int) -> None:
    from src.services.dlq import write_dlq_entry
    dlq_id = await write_dlq_entry(
        source="analysis",
        reason="exceeded_retries",
        original_payload=payload,
        error_history=[{"attempt": attempt, "error": error}],
        interaction_id=payload.get("interaction_id"),
        customer_id=payload.get("customer_id"),
        trace_id=payload.get("trace_id"),
    )
    await write_event(
        interaction_id=payload["interaction_id"],
        trace_id=payload["trace_id"],
        stage=Stage.DEAD_LETTERED,
        status=Status.FAILED,
        source="analyse",
        metadata={"source": "analysis", "reason": "exceeded_retries", "dlq_id": dlq_id},
    )


# ──────────────────────────────────────────────────────────────────────────────
# Recording task — bounded poll, parallel to triage/analyse
# ──────────────────────────────────────────────────────────────────────────────


@celery_app.task(name="poll_recording", bind=True, queue=settings.QUEUE_RECORDING,
                 acks_late=True, max_retries=10)
def poll_recording(self, interaction_id: str, trace_id: str,
                   exotel_account_id: str, call_sid: str, attempt: int = 1) -> None:
    try:
        _run_async(_poll_recording_async(
            interaction_id, trace_id, exotel_account_id, call_sid, attempt
        ))
    except _RetryAfter as ra:
        raise self.retry(countdown=ra.countdown, max_retries=10,
                         kwargs={"attempt": ra.next_attempt})
    except Exception as e:
        logger.exception("poll_recording_failed",
                         extra={"interaction_id": interaction_id,
                                "trace_id": trace_id,
                                "error": str(e)})
        # Don't retry on unexpected errors — mark terminal.
        _run_async(update_recording_status(
            interaction_id, "fetch_error",
            is_terminal=True, bump_attempt=True,
        ))


class _RetryAfter(Exception):
    def __init__(self, countdown: float, next_attempt: int):
        self.countdown = countdown
        self.next_attempt = next_attempt


async def _poll_recording_async(interaction_id, trace_id, account_sid, call_sid, attempt):
    await update_recording_status(interaction_id, "pending", bump_attempt=True)
    result = await fetch_exotel_recording(call_sid, account_sid)

    await write_event(
        interaction_id=interaction_id,
        trace_id=trace_id,
        stage=Stage.RECORDING_POLL,
        status=Status.SUCCESS if result.status == FetchStatus.READY else Status.IN_PROGRESS,
        source="recording_poller",
        metadata={"attempt": attempt, "poll_status": result.status.value,
                  "error_detail": result.error_detail},
    )

    if result.status == FetchStatus.READY:
        await update_recording_status(interaction_id, "uploading")
        try:
            s3_key = await upload_to_s3(result.url, interaction_id)
        except Exception:
            await update_recording_status(
                interaction_id, "fetch_error",
                is_terminal=True, bump_attempt=False,
            )
            raise
        await update_recording_status(
            interaction_id, "uploaded",
            s3_key=s3_key, is_terminal=True, bump_attempt=False,
        )
        await write_event(
            interaction_id=interaction_id, trace_id=trace_id,
            stage=Stage.RECORDING_TERMINAL, status=Status.SUCCESS,
            source="recording_poller",
            metadata={"terminal_status": "uploaded", "attempt_count": attempt},
        )
        return

    if result.status == FetchStatus.PERMANENT_ERROR:
        await update_recording_status(
            interaction_id, "fetch_error",
            is_terminal=True, bump_attempt=False,
        )
        await write_event(
            interaction_id=interaction_id, trace_id=trace_id,
            stage=Stage.RECORDING_TERMINAL, status=Status.FAILED,
            source="recording_poller",
            metadata={"terminal_status": "fetch_error", "reason": result.error_detail},
        )
        return

    # not_ready or transient_error — retry per schedule
    next_attempt = attempt + 1
    delay = poll_delay_for_attempt(next_attempt)
    if delay is None:
        # Schedule exhausted.
        terminal = "unavailable" if result.status == FetchStatus.NOT_READY else "fetch_error"
        await update_recording_status(
            interaction_id, terminal,
            is_terminal=True, bump_attempt=False,
        )
        await write_event(
            interaction_id=interaction_id, trace_id=trace_id,
            stage=Stage.RECORDING_TERMINAL, status=Status.FAILED,
            source="recording_poller",
            metadata={"terminal_status": terminal, "attempt_count": attempt},
        )
        return

    raise _RetryAfter(countdown=delay, next_attempt=next_attempt)


# ──────────────────────────────────────────────────────────────────────────────
# Outbox tasks
# ──────────────────────────────────────────────────────────────────────────────


@celery_app.task(name="outbox_beat")
def outbox_beat() -> None:
    """Beat task — pull a batch of pending rows and enqueue per-row dispatches."""
    try:
        ids = _run_async(fetch_dispatch_batch(settings.OUTBOX_BATCH_SIZE))
        for row_id in ids:
            outbox_dispatch_one.apply_async(args=[row_id], queue=settings.QUEUE_OUTBOX)
    except Exception:
        logger.exception("outbox_beat_failed")


@celery_app.task(name="outbox_dispatch_one", queue="outbox", acks_late=True)
def outbox_dispatch_one(row_id: int) -> None:
    _run_async(dispatch_row(row_id))


@celery_app.task(name="outbox_reconcile_stuck")
def outbox_reconcile_stuck() -> None:
    """Beat task — reset rows stuck in 'in_progress' back to 'pending'."""
    try:
        _run_async(reconcile_stuck_in_progress(settings.OUTBOX_STUCK_THRESHOLD_SECONDS))
    except Exception:
        logger.exception("outbox_reconcile_failed")


# ──────────────────────────────────────────────────────────────────────────────
# TPM ledger sweeper + pressure gauge publisher
# ──────────────────────────────────────────────────────────────────────────────


@celery_app.task(name="ledger_sweep_expired")
def ledger_sweep_expired() -> None:
    """Beat task — reclaim stranded TPM ledger reservations (§4.5)."""
    try:
        count = _run_async(tpm_ledger.sweep_expired_reservations())
        if count:
            logger.info("ledger_sweep_completed", extra={"expired_count": count})
    except Exception:
        logger.exception("ledger_sweep_failed")


@celery_app.task(name="pressure_publish")
def pressure_publish() -> None:
    """Beat task — refresh the platform_pressure gauge (§4.9)."""
    try:
        _run_async(publish_pressure())
    except Exception:
        logger.exception("pressure_publish_failed")


# ──────────────────────────────────────────────────────────────────────────────
# Recording reconciliation beat
# ──────────────────────────────────────────────────────────────────────────────


@celery_app.task(name="recording_reconcile_stuck")
def recording_reconcile_stuck() -> None:
    """
    Beat task — recover interactions stuck in `recording_status='uploading'`
    (the §7.6 reconciliation path). For v1 we mark them fetch_error and let
    ops decide; production HEAD-checks S3 first.
    """
    try:
        ids = _run_async(find_stuck_uploads(settings.RECORDING_STUCK_THRESHOLD_SECONDS))
        for iid in ids:
            _run_async(update_recording_status(
                iid, "fetch_error", is_terminal=True, bump_attempt=False,
            ))
        if ids:
            logger.warning("recording_stuck_recovered",
                           extra={"count": len(ids), "ids_sample": ids[:10]})
    except Exception:
        logger.exception("recording_reconcile_failed")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


async def _load_interaction(interaction_id: str):
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT id, session_id, lead_id, campaign_id, customer_id, agent_id,
                       conversation_data, status
                FROM interactions WHERE id = CAST(:id AS uuid)
                """
            ),
            {"id": interaction_id},
        )
        row = result.first()
        if row is None:
            return None
        return {
            "id": row[0],
            "session_id": row[1],
            "lead_id": row[2],
            "campaign_id": row[3],
            "customer_id": row[4],
            "agent_id": row[5],
            "conversation_data": row[6],
            "status": row[7],
        }


async def _update_interaction_lane(interaction_id: str, lane: str, verdict) -> None:
    async with async_session_factory() as session:
        await session.execute(
            text(
                """
                UPDATE interactions
                SET lane = :lane,
                    classifier_verdict = CAST(:verdict AS jsonb),
                    status = CASE WHEN :lane = 'skip' THEN 'ANALYSIS_SKIPPED' ELSE 'ANALYZING' END,
                    updated_at = NOW()
                WHERE id = CAST(:id AS uuid)
                """
            ),
            {
                "id": interaction_id,
                "lane": lane,
                "verdict": json.dumps({
                    "lane": verdict.lane,
                    "suggested_call_stage": verdict.suggested_call_stage,
                    "matched_rules": verdict.matched_rules,
                }),
            },
        )
        await session.commit()
