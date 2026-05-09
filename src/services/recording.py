"""
Recording pipeline (SUBMISSION.md §7).

Replaces the unconditional `asyncio.sleep(45s)` with a bounded polling
loop that runs in its own Celery task, parallel to triage and analysis.

Three terminal states persisted to `interactions.recording_status`:
  - uploaded    — fetched + uploaded to S3 successfully
  - unavailable — all polls returned 404 (recording never produced)
  - fetch_error — persistent HTTP error or network failure

Each terminal is alertable via §9.4 thresholds.

The poller uses Celery's `countdown=` parameter between attempts (NOT
in-process sleep), so workers are freed between polls.
"""

import logging
import random
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import httpx
from sqlalchemy import text

from src.config import settings
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


class FetchStatus(str, Enum):
    READY = "ready"
    NOT_READY = "not_ready"          # 404 — keep polling
    TRANSIENT_ERROR = "transient_error"  # 5xx / network — keep polling
    PERMANENT_ERROR = "permanent_error"  # 401/403/other 4xx — fail immediately


@dataclass
class FetchResult:
    status: FetchStatus
    url: Optional[str] = None
    error_detail: Optional[str] = None


# ──────────────────────────────────────────────────────────────────────────────
# Poll-schedule helpers
# ──────────────────────────────────────────────────────────────────────────────


def jittered_delay(base_seconds: int) -> float:
    """Apply ±JITTER_FRACTION to a base interval."""
    j = settings.RECORDING_JITTER_FRACTION
    return base_seconds * random.uniform(1 - j, 1 + j)


def poll_delay_for_attempt(attempt: int) -> Optional[float]:
    """
    Return the wait (seconds) before attempt number `attempt` (1-indexed).
    None means no further attempts — the schedule is exhausted.
    """
    schedule = settings.RECORDING_POLL_SCHEDULE_SECONDS
    if attempt < 1 or attempt > len(schedule):
        return None
    return jittered_delay(schedule[attempt - 1])


# ──────────────────────────────────────────────────────────────────────────────
# Exotel fetch
# ──────────────────────────────────────────────────────────────────────────────


async def fetch_exotel_recording(call_sid: str, account_sid: str) -> FetchResult:
    """
    Hit the Exotel recording status endpoint. Distinguishes:
      - 200 + recording_url → ready
      - 404 → not_ready (keep polling)
      - 5xx / network → transient_error
      - 401/403/other 4xx → permanent_error
    """
    url = f"https://api.exotel.com/v1/Accounts/{account_sid}/Calls/{call_sid}/Recording"

    try:
        async with httpx.AsyncClient(timeout=settings.EXOTEL_HTTP_TIMEOUT_SECONDS) as client:
            resp = await client.get(url)

        if resp.status_code == 200:
            data = resp.json()
            recording_url = data.get("recording_url")
            if not recording_url:
                return FetchResult(status=FetchStatus.NOT_READY)
            return FetchResult(status=FetchStatus.READY, url=recording_url)

        if resp.status_code == 404:
            return FetchResult(status=FetchStatus.NOT_READY)

        if resp.status_code in (401, 403):
            return FetchResult(
                status=FetchStatus.PERMANENT_ERROR,
                error_detail=f"auth_failure_{resp.status_code}",
            )

        if 500 <= resp.status_code < 600:
            return FetchResult(
                status=FetchStatus.TRANSIENT_ERROR,
                error_detail=f"http_{resp.status_code}",
            )

        # Other 4xx
        return FetchResult(
            status=FetchStatus.PERMANENT_ERROR,
            error_detail=f"http_{resp.status_code}",
        )

    except httpx.HTTPError as e:
        return FetchResult(
            status=FetchStatus.TRANSIENT_ERROR,
            error_detail=f"network:{type(e).__name__}",
        )


# ──────────────────────────────────────────────────────────────────────────────
# Persistence helpers
# ──────────────────────────────────────────────────────────────────────────────


async def update_recording_status(
    interaction_id: str,
    status: str,
    *,
    s3_key: Optional[str] = None,
    is_terminal: bool = False,
    bump_attempt: bool = False,
) -> None:
    """Update recording_status (and related columns) on the interactions row."""
    fields: list[str] = ["recording_status = :status"]
    params: dict = {"id": interaction_id, "status": status}

    if s3_key is not None:
        fields.append("recording_s3_key = :s3_key")
        params["s3_key"] = s3_key

    if bump_attempt:
        fields.append("recording_attempt_count = recording_attempt_count + 1")
        fields.append("recording_last_attempt_at = NOW()")

    if is_terminal:
        fields.append("recording_terminal_at = NOW()")

    fields.append("updated_at = NOW()")

    async with async_session_factory() as session:
        await session.execute(
            text(
                f"UPDATE interactions SET {', '.join(fields)} WHERE id = CAST(:id AS uuid)"
            ),
            params,
        )
        await session.commit()


async def upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Stream the recording from Exotel and upload to S3. Returns the S3 key.

    State-before-action: caller writes status='uploading' before this runs;
    if we crash here or anywhere before the post-upload status='uploaded'
    write, the reconciliation beat task picks the row back up (§7.6).

    Mock implementation for the assessment — production streams via httpx
    chunked download → boto3 multipart upload.
    """
    s3_key = f"recordings/{interaction_id}.mp3"
    logger.info(
        "recording_uploaded",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key


# ──────────────────────────────────────────────────────────────────────────────
# Reconciliation — recovers stuck `uploading` rows (§7.6)
# ──────────────────────────────────────────────────────────────────────────────


async def find_stuck_uploads(threshold_seconds: int) -> list[str]:
    """
    Return interaction_ids whose recording_status is still 'uploading' beyond
    the staleness threshold. The reconciliation task HEAD-checks each S3 key.
    """
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT id::text FROM interactions
                WHERE recording_status = 'uploading'
                  AND updated_at < NOW() - (:secs || ' seconds')::interval
                LIMIT 100
                """
            ),
            {"secs": str(threshold_seconds)},
        )
        return [row[0] for row in result.fetchall()]
