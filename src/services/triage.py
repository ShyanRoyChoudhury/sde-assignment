"""
Rule-based triage classifier (SUBMISSION.md §6).

Routes every long call into one of three lanes — `skip`, `hot`, `cold` —
before any LLM call is considered. Pure rules, sub-millisecond per call.

Order matters:
    1. Skip on short transcript (turn_count < SHORT_TRANSCRIPT_TURN_THRESHOLD)
    2. Skip on strong negative keyword match (wrong_number / do_not_call)
    3. Hot before Cold (revenue protection on collisions)
    4. Default-hot on ambiguity (asymmetric cost; missed-hot is sales loss)

Single language (English) in v1 per A10. Non-matching transcripts default-hot
where the full LLM handles language-aware classification.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Literal, Optional

from src.config import settings

logger = logging.getLogger(__name__)

Lane = Literal["skip", "hot", "cold"]


@dataclass
class TriageVerdict:
    lane: Lane
    suggested_call_stage: Optional[str]  # used directly on skip; hint to LLM on hot/cold
    matched_rules: List[str] = field(default_factory=list)


def classify(transcript_text: str, turn_count: int) -> TriageVerdict:
    """
    Classify a call from its transcript text and turn count.

    transcript_text is the full transcript joined as "role: content" lines.
    turn_count is the number of turns (matches the existing endpoint check
    where < 4 was special-cased).
    """
    # Rule 1 — short transcript skip
    if turn_count < settings.SHORT_TRANSCRIPT_TURN_THRESHOLD:
        return TriageVerdict(
            lane="skip",
            suggested_call_stage="short_call",
            matched_rules=["min_turns"],
        )

    text = transcript_text.lower()

    # Rule 2 — strong negative skip (no entities for the LLM to extract)
    for stage, keywords in settings.SKIP_NEGATIVE_RULES.items():
        for kw in keywords:
            if kw in text:
                return TriageVerdict(
                    lane="skip",
                    suggested_call_stage=stage,
                    matched_rules=[f"skip_negative:{stage}:{kw}"],
                )

    # Rule 3 — Hot before Cold (revenue protection on collisions)
    for stage, keywords in settings.HOT_RULES.items():
        for kw in keywords:
            if kw in text:
                return TriageVerdict(
                    lane="hot",
                    suggested_call_stage=stage,
                    matched_rules=[f"hot:{stage}:{kw}"],
                )

    for stage, keywords in settings.COLD_RULES.items():
        for kw in keywords:
            if kw in text:
                return TriageVerdict(
                    lane="cold",
                    suggested_call_stage=stage,
                    matched_rules=[f"cold:{stage}:{kw}"],
                )

    # Rule 4 — ambiguous, default hot
    return TriageVerdict(lane="hot", suggested_call_stage=None, matched_rules=[])
