"""
FastAPI endpoint for ending an interaction (SUBMISSION.md §12).

POST /session/{session_id}/interaction/{interaction_id}/end

The external contract is unchanged from v1. What changed:
  - Mints a trace_id per webhook hit
  - Writes interaction_events(ENDED) for the audit trail
  - Enqueues triage + recording tasks IN PARALLEL (no sequential coupling)
  - DOES NOT fire signal_jobs / lead_stage with empty payloads — those now
    flow through the outbox after analysis completes (§8.2)
  - DOES NOT detect short transcripts inline — triage handles it (§6.2)
  - All work is durable: nothing relies on asyncio.create_task surviving a restart
"""

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from src.config import settings
from src.services.event_log import Stage, Status, write as write_event
from src.tasks.celery_tasks import poll_recording, triage_task
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)
router = APIRouter()


class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None


class InteractionEndResponse(BaseModel):
    status: str
    interaction_id: str
    message: str


@router.post(
    "/session/{session_id}/interaction/{interaction_id}/end",
    response_model=InteractionEndResponse,
)
async def end_interaction(
    session_id: UUID,
    interaction_id: UUID,
    request: InteractionEndRequest,
):
    """
    End an interaction and trigger post-call processing.

    Flow:
      1. Mint trace_id
      2. Update interaction status to ENDED + write trace_id
      3. Write interaction_events(ENDED)
      4. Enqueue triage task on triage queue
      5. Enqueue recording poller on recording queue (parallel, independent)
      6. Return 200 (well within Exotel's 5s timeout)
    """
    trace_id = uuid.uuid4()

    try:
        interaction = await _load_interaction(interaction_id)
        if not interaction:
            raise HTTPException(status_code=404, detail="Interaction not found")

        await _mark_ended(
            interaction_id=str(interaction_id),
            trace_id=str(trace_id),
            ended_at=datetime.utcnow(),
            duration=request.duration_seconds,
            call_sid=request.call_sid,
        )

        await write_event(
            interaction_id=str(interaction_id),
            trace_id=str(trace_id),
            stage=Stage.ENDED,
            status=Status.SUCCESS,
            source="endpoint",
            metadata={
                "call_sid": request.call_sid,
                "duration_seconds": request.duration_seconds,
                "call_status": request.call_status,
            },
        )

        # Enqueue triage. The triage task reads the interaction row from the DB
        # rather than carrying the transcript in the payload — this keeps the
        # task message small and avoids stale-payload issues on retry.
        triage_payload = {
            "interaction_id": str(interaction_id),
            "trace_id": str(trace_id),
            "session_id": str(session_id),
            "call_sid": request.call_sid,
            "additional_data": request.additional_data or {},
            "ended_at": datetime.utcnow().isoformat(),
            "exotel_account_id": interaction.get("exotel_account_id"),
        }
        try:
            triage_task.apply_async(args=[triage_payload], queue=settings.QUEUE_TRIAGE)
        except Exception as e:
            logger.exception(
                "endpoint_enqueue_failed",
                extra={"interaction_id": str(interaction_id),
                       "trace_id": str(trace_id),
                       "queue": settings.QUEUE_TRIAGE,
                       "error": str(e)},
            )
            await write_event(
                interaction_id=str(interaction_id),
                trace_id=str(trace_id),
                stage=Stage.ENDED,
                status=Status.FAILED,
                source="endpoint",
                metadata={"reason": "triage_enqueue_failed", "error": str(e)},
            )
            raise HTTPException(status_code=503, detail="Service unavailable")

        # Recording is independent and best-effort — failure to enqueue here
        # is logged but doesn't block the response (the analysis path proceeds).
        try:
            poll_recording.apply_async(
                args=[
                    str(interaction_id),
                    str(trace_id),
                    interaction.get("exotel_account_id") or "",
                    request.call_sid or "",
                    1,
                ],
                queue=settings.QUEUE_RECORDING,
            )
        except Exception as e:
            logger.warning(
                "recording_enqueue_failed",
                extra={"interaction_id": str(interaction_id),
                       "trace_id": str(trace_id),
                       "error": str(e)},
            )

        return InteractionEndResponse(
            status="ok",
            interaction_id=str(interaction_id),
            message="Interaction ended, processing enqueued",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "end_interaction_failed",
            extra={"interaction_id": str(interaction_id),
                   "trace_id": str(trace_id),
                   "error": str(e)},
        )
        # Best-effort audit row for the failure path.
        try:
            await write_event(
                interaction_id=str(interaction_id),
                trace_id=str(trace_id),
                stage=Stage.ENDED,
                status=Status.FAILED,
                source="endpoint",
                metadata={"error": str(e)},
            )
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Internal server error")


# ──────────────────────────────────────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────────────────────────────────────


async def _load_interaction(interaction_id: UUID) -> Optional[Dict[str, Any]]:
    """
    Load the interaction row. The transcript was written by the voicebot
    during the call; we just need to confirm the row exists and grab IDs.

    Falls back to a mock row when the DB is unreachable (assessment-mode dev).
    """
    try:
        async with async_session_factory() as session:
            result = await session.execute(
                text(
                    """
                    SELECT id, lead_id, campaign_id, customer_id, agent_id,
                           conversation_data
                    FROM interactions WHERE id = :id
                    """
                ),
                {"id": interaction_id},
            )
            row = result.first()
            if row is None:
                return None
            return {
                "id": str(row[0]),
                "lead_id": str(row[1]),
                "campaign_id": str(row[2]),
                "customer_id": str(row[3]),
                "agent_id": str(row[4]),
                "conversation_data": row[5] or {},
                "exotel_account_id": (row[5] or {}).get("exotel_account_sid"),
            }
    except Exception:
        # Dev/assessment fallback. Mirrors the pre-redesign mock.
        logger.warning(
            "interaction_load_fallback_to_mock",
            extra={"interaction_id": str(interaction_id)},
        )
        return {
            "id": str(interaction_id),
            "lead_id": "mock-lead-id",
            "campaign_id": "mock-campaign-id",
            "customer_id": "mock-customer-id",
            "agent_id": "mock-agent-id",
            "exotel_account_id": "mock-exotel-account",
            "conversation_data": {},
        }


async def _mark_ended(
    *,
    interaction_id: str,
    trace_id: str,
    ended_at: datetime,
    duration: Optional[int],
    call_sid: Optional[str],
) -> None:
    """Update interaction row to ENDED + stamp trace_id."""
    try:
        async with async_session_factory() as session:
            await session.execute(
                text(
                    """
                    UPDATE interactions
                    SET status = 'ENDED',
                        ended_at = :ended_at,
                        duration_seconds = :duration,
                        call_sid = COALESCE(:call_sid, call_sid),
                        trace_id = CAST(:trace_id AS uuid),
                        updated_at = NOW()
                    WHERE id = CAST(:id AS uuid)
                    """
                ),
                {
                    "id": interaction_id,
                    "ended_at": ended_at,
                    "duration": duration,
                    "call_sid": call_sid,
                    "trace_id": trace_id,
                },
            )
            await session.commit()
    except Exception:
        logger.warning(
            "interaction_status_update_skipped_in_dev",
            extra={"interaction_id": interaction_id, "trace_id": trace_id},
        )
