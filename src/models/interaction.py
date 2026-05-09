import enum
import uuid
from typing import Optional

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from src.models.base import Base


class InteractionStatus(str, enum.Enum):
    # Pre-call lifecycle (existing)
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    IN_PROGRESS = "IN_PROGRESS"
    ENDED = "ENDED"
    FAILED = "FAILED"
    PROCESSING = "PROCESSING"  # legacy; deprecated, retained for back-compat
    # Post-call lifecycle (new in v2 — see SUBMISSION.md §10.2)
    ANALYZING = "ANALYZING"
    ANALYZED = "ANALYZED"
    ANALYSIS_SKIPPED = "ANALYSIS_SKIPPED"
    DEAD_LETTERED = "DEAD_LETTERED"


class RecordingStatus(str, enum.Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    UNAVAILABLE = "unavailable"
    FETCH_ERROR = "fetch_error"


class Lane(str, enum.Enum):
    HOT = "hot"
    COLD = "cold"
    SKIP = "skip"


class Interaction(Base):
    __tablename__ = "interactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(
        UUID(as_uuid=True), ForeignKey("sessions.id"), nullable=False, index=True
    )
    lead_id = Column(
        UUID(as_uuid=True), ForeignKey("leads.id"), nullable=False, index=True
    )
    campaign_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    customer_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    agent_id = Column(UUID(as_uuid=True), nullable=False, index=True)

    # Status uses VARCHAR(32) with a DB-side CHECK constraint (see migration
    # V2_postcall_pipeline.sql). VARCHAR avoids ALTER TYPE complexity across
    # rolling worker restarts when new lifecycle states are added.
    status = Column(String(32), default=InteractionStatus.INITIATED.value, nullable=False)
    call_sid = Column(String(255), nullable=True, index=True)
    call_provider = Column(String(50), default="exotel")

    started_at = Column(DateTime(timezone=True), nullable=True)
    ended_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    # The transcript is stored as JSONB inside conversation_data
    # conversation_data = {"transcript": [...], "summary": "...", ...}
    conversation_data = Column(JSONB, default=dict)

    # interaction_metadata stores extracted entities, analysis results,
    # and dashboard-facing fields. This is the "hot cache" the dashboard reads.
    # Structure: {"entities": {...}, "call_stage": "...", "analyzed_at": "..."}
    interaction_metadata = Column(JSONB, default=dict)

    recording_url = Column(Text, nullable=True)
    recording_s3_key = Column(String(512), nullable=True)

    postcall_celery_task_id = Column(String(255), nullable=True)

    retry_count = Column(Integer, default=0)
    error_log = Column(JSONB, default=list)

    # ── New columns added in v2 (SUBMISSION.md §10.2) ─────────────────────
    trace_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    lane = Column(String(8), nullable=True)  # 'hot' | 'cold' | 'skip'
    classifier_verdict = Column(JSONB, nullable=True)  # {lane, suggested_call_stage, matched_rules}
    analyzed_at = Column(DateTime(timezone=True), nullable=True)  # idempotency anchor

    recording_status = Column(
        String(32), nullable=False, default=RecordingStatus.PENDING.value,
        server_default=RecordingStatus.PENDING.value,
    )
    recording_attempt_count = Column(Integer, nullable=False, default=0, server_default="0")
    recording_last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    recording_terminal_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    session = relationship("Session", back_populates="interactions")
    lead = relationship("Lead", back_populates="interactions")

    @property
    def transcript_text(self) -> str:
        transcript = (self.conversation_data or {}).get("transcript", [])
        if isinstance(transcript, list):
            return "\n".join(
                f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
                for turn in transcript
            )
        return str(transcript)

    @property
    def is_short_transcript(self) -> bool:
        transcript = (self.conversation_data or {}).get("transcript", [])
        return len(transcript) < 4

    @property
    def exotel_account_id(self) -> Optional[str]:
        return (self.conversation_data or {}).get("exotel_account_sid")
