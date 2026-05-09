"""
Append-only audit log writer (SUBMISSION.md §9.2).

Every state transition writes one row to `interaction_events`. Combined with
`trace_id` propagation (§9.1), this lets an on-call engineer reconstruct the
full journey of any interaction with a single SQL query — proven by the
walk-through in §9.6.
"""

import json
import logging
from typing import Any, Dict, Optional

from sqlalchemy import text

from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


# Stage vocabulary — must match SUBMISSION.md §9.2 table.
class Stage:
    ENDED = "ENDED"
    TRIAGED = "TRIAGED"
    ANALYZE_ACQUIRED = "ANALYZE_ACQUIRED"
    ANALYZE_DEFERRED = "ANALYZE_DEFERRED"
    ANALYZED = "ANALYZED"
    OUTBOX_INSERTED = "OUTBOX_INSERTED"
    OUTBOX_DISPATCHED = "OUTBOX_DISPATCHED"
    OUTBOX_FAILED = "OUTBOX_FAILED"
    RECORDING_POLL = "RECORDING_POLL"
    RECORDING_TERMINAL = "RECORDING_TERMINAL"
    DEAD_LETTERED = "DEAD_LETTERED"
    REPLAYED = "REPLAYED"
    TRANSCRIPT_ACCESS = "TRANSCRIPT_ACCESS"  # human read of a transcript (§11.6)
    CUSTOMER_DELETED = "CUSTOMER_DELETED"    # GDPR deletion event (§11.7)


class Status:
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    DEFERRED = "DEFERRED"
    IN_PROGRESS = "IN_PROGRESS"


async def write(
    *,
    interaction_id: str,
    trace_id: str,
    stage: str,
    status: str,
    source: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    session=None,
) -> None:
    """
    Insert one event row. If `session` is provided, runs inside that
    transaction (typical when the analyse task wants the event row to land
    atomically with the result write); otherwise opens a new session.

    metadata is a free-form JSONB. The redacting StructuredLogger in §9.3
    governs LOG content; this function trusts the caller to keep PII out
    of metadata (per the discipline checked in code review).
    """
    sql = text(
        """
        INSERT INTO interaction_events
            (interaction_id, trace_id, stage, status, source, metadata)
        VALUES
            (CAST(:interaction_id AS uuid), CAST(:trace_id AS uuid),
             :stage, :status, :source, CAST(:metadata AS jsonb))
        """
    )
    params = {
        "interaction_id": interaction_id,
        "trace_id": trace_id,
        "stage": stage,
        "status": status,
        "source": source,
        "metadata": json.dumps(metadata or {}),
    }

    if session is not None:
        await session.execute(sql, params)
        return

    async with async_session_factory() as new_session:
        await new_session.execute(sql, params)
        await new_session.commit()
