"""
PostCallProcessor — runs LLM analysis on a completed call transcript.

Now gated by the TPM ledger (§4): every call goes through `acquire`/`commit`,
which proactively respects per-customer + global rate limits and never
surfaces 429s upstream.

The classifier hint (from triage, §6) is passed into the prompt as context,
not as instruction — the LLM still performs full analysis.

Result writes are idempotent (`WHERE analyzed_at IS NULL`) so Celery
redelivery cannot double-write.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from src.config import settings
from src.services.tpm_ledger import AcquireResult, tpm_ledger
from src.utils.db import async_session_factory

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Data shapes
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class PostCallContext:
    """Everything needed to process one completed call."""
    interaction_id: str
    session_id: str
    lead_id: str
    campaign_id: str
    customer_id: str
    agent_id: str
    call_sid: str
    transcript_text: str
    conversation_data: dict
    additional_data: dict
    ended_at: datetime
    exotel_account_id: Optional[str] = None
    trace_id: Optional[str] = None
    classifier_hint: Optional[str] = None  # suggested_call_stage from triage


@dataclass
class AnalysisResult:
    call_stage: str
    entities: Dict[str, Any]
    summary: str
    raw_response: Dict[str, Any]
    tokens_used: int
    latency_ms: float
    provider: str
    model: str


class BudgetDenied(Exception):
    """Raised when the TPM ledger denies the acquire."""

    def __init__(self, retry_after_ms: int, reason: str):
        super().__init__(f"budget_denied:{reason}:retry_after_ms={retry_after_ms}")
        self.retry_after_ms = retry_after_ms
        self.reason = reason


# ──────────────────────────────────────────────────────────────────────────────
# Processor
# ──────────────────────────────────────────────────────────────────────────────


class PostCallProcessor:
    """
    Runs full LLM analysis on a transcript, gated by the TPM ledger.

    Calls are routed by lane:
      - hot:  acquires from reserved capacity first; falls back to burst share
      - cold: only acquires from burst share (via §5's effective-budget formula)
    """

    async def process(
        self,
        ctx: PostCallContext,
        *,
        lane: str = "hot",
    ) -> AnalysisResult:
        """
        Acquire budget → call LLM → commit. Caller handles deferral on
        BudgetDenied (typically: schedule a Celery retry with countdown).
        """
        est_tokens = self._estimate_tokens(ctx)
        result: AcquireResult = await tpm_ledger.acquire(
            customer_id=ctx.customer_id,
            interaction_id=ctx.interaction_id,
            est_tokens=est_tokens,
            lane=lane,  # type: ignore[arg-type]
        )

        if not result.granted:
            raise BudgetDenied(
                retry_after_ms=result.retry_after_ms or settings.MIN_RETRY_BACKOFF_MS,
                reason=result.reason or "unknown",
            )

        reservation_id = result.reservation_id
        actual_tokens = 0
        try:
            prompt = self._build_analysis_prompt(
                ctx.transcript_text,
                ctx.additional_data,
                classifier_hint=ctx.classifier_hint,
            )
            start_time = datetime.utcnow()
            response = await self._call_llm(prompt)
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

            analysis = self._parse_response(response, elapsed_ms)
            actual_tokens = analysis.tokens_used
            await self._update_interaction_metadata(ctx.interaction_id, analysis)

            logger.info(
                "postcall_analysis_complete",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "trace_id": ctx.trace_id,
                    "customer_id": ctx.customer_id,
                    "campaign_id": ctx.campaign_id,
                    "lane": lane,
                    "call_stage": analysis.call_stage,
                    "tokens_used": analysis.tokens_used,
                    "latency_ms": analysis.latency_ms,
                },
            )
            return analysis

        except Exception as e:
            # Provider error — we still know prompt tokens were processed by
            # the provider, so charge those (§4.6 failure path). We DON'T
            # know exact prompt tokens here without re-tokenising, so we
            # use the est as a conservative upper bound.
            actual_tokens = self._estimate_prompt_tokens(ctx)
            logger.exception(
                "postcall_analysis_failed",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "trace_id": ctx.trace_id,
                    "error": str(e),
                },
            )
            raise

        finally:
            # Always commit — refund over-reservation and write durable ledger row.
            await tpm_ledger.commit(
                reservation_id=reservation_id,
                actual_tokens=actual_tokens,
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                trace_id=ctx.trace_id or ctx.interaction_id,
                campaign_id=ctx.campaign_id,
                model=settings.LLM_MODEL,
                provider=settings.LLM_PROVIDER,
            )

    # ──────────────────────────────────────────────────────────────────────
    # Token estimation
    # ──────────────────────────────────────────────────────────────────────

    def _estimate_tokens(self, ctx: PostCallContext) -> int:
        """est = prompt + max_completion (§4.2). Refunded down to actual on commit."""
        return self._estimate_prompt_tokens(ctx) + settings.LLM_MAX_COMPLETION_TOKENS

    def _estimate_prompt_tokens(self, ctx: PostCallContext) -> int:
        """
        Cheap approximation: ~4 chars per token. Production would call the
        provider's tokenizer (tiktoken etc.) for an exact count.
        """
        char_count = len(ctx.transcript_text) + len(json.dumps(ctx.additional_data))
        # Add ~200 chars of system prompt overhead.
        return max(100, (char_count + 200) // 4)

    # ──────────────────────────────────────────────────────────────────────
    # Prompt + LLM call
    # ──────────────────────────────────────────────────────────────────────

    def _build_analysis_prompt(
        self,
        transcript: str,
        additional_data: dict,
        *,
        classifier_hint: Optional[str] = None,
    ) -> str:
        system_prompt = """You are a call analysis assistant. Analyze the following
call transcript and extract:
1. call_stage: The outcome/disposition of the call
2. entities: Key information mentioned (dates, times, amounts, names, preferences)
3. summary: A brief summary of what happened in the call

Respond in JSON format:
{
    "call_stage": "...",
    "entities": {...},
    "summary": "..."
}"""

        hint_block = ""
        if classifier_hint:
            hint_block = (
                f"\n\nPre-classifier suggested this call's outcome may be: "
                f"{classifier_hint}. You may use this hint, but verify against "
                f"the transcript and override if wrong.\n"
            )

        return (
            f"{system_prompt}{hint_block}\n\n"
            f"Transcript:\n{transcript}\n\n"
            f"Additional context:\n{json.dumps(additional_data)}"
        )

    async def _call_llm(self, prompt: str) -> dict:
        """
        Call the configured LLM provider.

        In production this is an httpx POST to the provider's API with
        `max_tokens=settings.LLM_MAX_COMPLETION_TOKENS` and a timeout of
        `settings.LLM_REQUEST_HARD_TIMEOUT_SECONDS`.

        Mock implementation for the assessment.
        """
        return {
            "call_stage": "unknown",
            "entities": {},
            "summary": "Mock analysis result",
            "usage": {"total_tokens": 1500},
        }

    def _parse_response(self, response: dict, latency_ms: float) -> AnalysisResult:
        return AnalysisResult(
            call_stage=response.get("call_stage", "unknown"),
            entities=response.get("entities", {}),
            summary=response.get("summary", ""),
            raw_response=response,
            tokens_used=response.get("usage", {}).get("total_tokens", 0),
            latency_ms=latency_ms,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )

    # ──────────────────────────────────────────────────────────────────────
    # Idempotent result write
    # ──────────────────────────────────────────────────────────────────────

    async def _update_interaction_metadata(
        self, interaction_id: str, result: AnalysisResult
    ) -> None:
        """
        Write analysis to interaction_metadata. Idempotent on `analyzed_at` —
        second writes from Celery redelivery are no-ops, preserving the first
        result rather than overwriting it.
        """
        payload = {
            "call_stage": result.call_stage,
            "entities": result.entities,
            "summary": result.summary,
            "tokens_used": result.tokens_used,
            "latency_ms": result.latency_ms,
            "provider": result.provider,
            "model": result.model,
            "analyzed_at": datetime.utcnow().isoformat(),
        }
        async with async_session_factory() as session:
            await session.execute(
                text(
                    """
                    UPDATE interactions
                    SET interaction_metadata = interaction_metadata || CAST(:patch AS jsonb),
                        analyzed_at = NOW(),
                        status = 'ANALYZED',
                        updated_at = NOW()
                    WHERE id = CAST(:id AS uuid) AND analyzed_at IS NULL
                    """
                ),
                {"id": interaction_id, "patch": json.dumps(payload)},
            )
            await session.commit()


# Module singleton
post_call_processor = PostCallProcessor()
