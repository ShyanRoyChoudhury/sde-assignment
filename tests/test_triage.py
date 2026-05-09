"""
Triage classifier tests (SUBMISSION.md §6, AC8).

Validates the rule-based classifier produces the expected lane and
suggested call_stage for each fixture transcript, and that skip-path
calls never trigger an LLM call.
"""

import pytest

from src.services.triage import classify


def _flatten(transcript):
    return "\n".join(f"{t['role']}: {t['content']}" for t in transcript)


# ──────────────────────────────────────────────────────────────────────────────
# AC8 — short transcripts skip LLM
# ──────────────────────────────────────────────────────────────────────────────


def test_short_transcript_routes_to_skip(sample_transcripts):
    """AC8: turn count < 4 → skip lane, suggested_call_stage='short_call'."""
    short = sample_transcripts["short_call_hangup"]
    text = _flatten(short["transcript"])
    verdict = classify(text, turn_count=len(short["transcript"]))
    assert verdict.lane == "skip"
    assert verdict.suggested_call_stage == "short_call"
    assert verdict.matched_rules == ["min_turns"]


def test_skip_takes_precedence_over_other_rules():
    """Even if a longer transcript would have matched hot/cold, < 4 turns wins."""
    transcript = "agent: confirmed\ncustomer: yes booked"
    verdict = classify(transcript, turn_count=2)
    assert verdict.lane == "skip"


def test_wrong_number_routes_to_skip():
    """Strong negative keyword 'wrong number' → skip."""
    transcript = (
        "agent: hello mr sharma\n"
        "customer: wrong number please remove\n"
        "agent: sorry sir\n"
        "customer: bye"
    )
    verdict = classify(transcript, turn_count=4)
    assert verdict.lane == "skip"
    assert verdict.suggested_call_stage == "wrong_number"


def test_do_not_call_routes_to_skip():
    transcript = (
        "agent: about your loan\n"
        "customer: stop calling me\n"
        "agent: ok sir\n"
        "customer: bye"
    )
    verdict = classify(transcript, turn_count=4)
    assert verdict.lane == "skip"
    assert verdict.suggested_call_stage == "do_not_call"


# ──────────────────────────────────────────────────────────────────────────────
# Hot lane — revenue-affecting outcomes
# ──────────────────────────────────────────────────────────────────────────────


def test_rebook_confirmed_routes_to_hot(sample_transcripts):
    rebook = sample_transcripts["rebook_confirmed"]
    text = _flatten(rebook["transcript"])
    verdict = classify(text, turn_count=len(rebook["transcript"]))
    assert verdict.lane == "hot"
    assert verdict.suggested_call_stage == "rebook_confirmed"


def test_demo_booked_routes_to_hot(sample_transcripts):
    demo = sample_transcripts["demo_booked"]
    text = _flatten(demo["transcript"])
    verdict = classify(text, turn_count=len(demo["transcript"]))
    assert verdict.lane == "hot"
    assert verdict.suggested_call_stage == "demo_booked"


def test_escalation_needed_routes_to_hot(sample_transcripts):
    esc = sample_transcripts["escalation_needed"]
    text = _flatten(esc["transcript"])
    verdict = classify(text, turn_count=len(esc["transcript"]))
    assert verdict.lane == "hot"
    assert verdict.suggested_call_stage == "escalation_needed"


# ──────────────────────────────────────────────────────────────────────────────
# Cold lane — low-value outcomes
# ──────────────────────────────────────────────────────────────────────────────


def test_not_interested_routes_to_cold(sample_transcripts):
    """Per A1: not_interested goes to cold (may still have entities worth extracting)."""
    ni = sample_transcripts["not_interested"]
    text = _flatten(ni["transcript"])
    verdict = classify(text, turn_count=len(ni["transcript"]))
    assert verdict.lane == "cold"
    assert verdict.suggested_call_stage == "not_interested"


def test_already_done_routes_to_cold(sample_transcripts):
    ad = sample_transcripts["already_purchased"]
    text = _flatten(ad["transcript"])
    verdict = classify(text, turn_count=len(ad["transcript"]))
    assert verdict.lane == "cold"
    assert verdict.suggested_call_stage == "already_done"


def test_callback_routes_to_cold():
    """Clean English callback request → cold lane."""
    transcript = (
        "agent: hello sir\n"
        "customer: i am busy now please call me later\n"
        "agent: when works for you?\n"
        "customer: try me after 6"
    )
    verdict = classify(transcript, turn_count=4)
    assert verdict.lane == "cold"
    assert verdict.suggested_call_stage == "callback_requested"


# ──────────────────────────────────────────────────────────────────────────────
# Hot-before-Cold rule order (§6.2 property #1)
# ──────────────────────────────────────────────────────────────────────────────


def test_hot_signal_wins_over_cold_in_same_call():
    """A transcript containing both 'confirmed' and 'not interested' routes hot."""
    transcript = (
        "agent: hello\n"
        "customer: not interested initially\n"
        "agent: but our offer is great\n"
        "customer: ok confirmed for tomorrow"
    )
    verdict = classify(transcript, turn_count=4)
    assert verdict.lane == "hot"
    assert verdict.suggested_call_stage == "rebook_confirmed"


# ──────────────────────────────────────────────────────────────────────────────
# Default-hot on ambiguity (§6.5)
# ──────────────────────────────────────────────────────────────────────────────


def test_no_keyword_match_defaults_to_hot():
    """A long transcript with no keyword match → default hot per §6.5."""
    transcript = "\n".join([f"agent: generic line number {i}" for i in range(8)])
    verdict = classify(transcript, turn_count=8)
    assert verdict.lane == "hot"
    assert verdict.suggested_call_stage is None
    assert verdict.matched_rules == []
