"""
TPM ledger tests (SUBMISSION.md §4, AC1 + AC2).

AC1: System never fires LLM requests beyond configured rate limits.
AC2: Per-customer token budget enforced — Customer A's budget does not
     consume Customer B's allocation.

Uses fakeredis as a drop-in for Redis. Patches the Postgres path (the
durable ledger row write) since we're testing enforcement semantics, not
durability — durability is tested separately.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest

from src.config import settings
from src.services.tpm_ledger import TpmLedger, _CustomerConfig


# ──────────────────────────────────────────────────────────────────────────────
# Test harness — wires a fresh fakeredis + stub customer config per test
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def configs():
    """Map customer_id → _CustomerConfig, used by the patched config loader."""
    return {}


@pytest.fixture
def ledger(fake_redis, configs):
    async def fake_load_config(customer_id):
        if customer_id not in configs:
            configs[customer_id] = _CustomerConfig(
                customer_id=customer_id,
                reserved_tpm=0,
                burst_weight=settings.DEFAULT_BURST_WEIGHT,
                max_tpm=settings.MAX_UNRESERVED_TPM,
            )
        return configs[customer_id]

    async def noop_session_call(*args, **kwargs):
        # Postgres write of token_ledger row — no-op for the in-memory test.
        return None

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=AsyncMock())
    mock_session.commit = AsyncMock()

    @asynccontextmanager_safe
    def session_factory():
        return _SessionCtx(mock_session)

    with patch("src.services.tpm_ledger.redis_client", fake_redis), \
         patch("src.services.tpm_ledger._load_customer_config", side_effect=fake_load_config), \
         patch("src.services.tpm_ledger.async_session_factory", session_factory):
        yield TpmLedger()


# Minimal async context-manager helper so the patched factory behaves like
# the real `async_session_factory()` (which returns an async-ctx-mgr).
class _SessionCtx:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *exc):
        return False


def asynccontextmanager_safe(fn):
    """Wrap a callable so each call returns the ctx manager — mirrors the
    real async_session_factory() invocation pattern."""
    return fn


# ──────────────────────────────────────────────────────────────────────────────
# AC1 — never exceed configured rate limits (no 429s surfaced)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_acquire_grants_when_budget_available(ledger, configs):
    configs["cust-a"] = _CustomerConfig("cust-a", reserved_tpm=10_000, burst_weight=0)
    result = await ledger.acquire(
        customer_id="cust-a",
        interaction_id="int-1",
        est_tokens=1500,
        lane="hot",
    )
    assert result.granted is True
    assert result.reservation_id is not None


@pytest.mark.asyncio
async def test_acquire_denies_when_customer_budget_exhausted(ledger, configs):
    """AC1: a customer at budget cap is denied (no 429 surfaced)."""
    configs["cust-a"] = _CustomerConfig("cust-a", reserved_tpm=2000, burst_weight=0)

    # First acquire consumes most of the budget.
    r1 = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-1",
        est_tokens=1500, lane="hot",
    )
    assert r1.granted is True

    # Second acquire would exceed the 2000 cap — should be denied.
    r2 = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-2",
        est_tokens=1000, lane="hot",
    )
    assert r2.granted is False
    assert r2.reason == "customer_full"
    assert r2.retry_after_ms is not None and r2.retry_after_ms > 0


@pytest.mark.asyncio
async def test_acquire_denies_when_global_budget_exhausted(ledger, configs, monkeypatch):
    """Global TPM ceiling triggers 'global_full' denial."""
    monkeypatch.setattr(settings, "LLM_TOKENS_PER_MINUTE", 5000)
    # One customer alone can saturate the global limit.
    configs["cust-a"] = _CustomerConfig("cust-a", reserved_tpm=10_000, burst_weight=0)

    r1 = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-1",
        est_tokens=4500, lane="hot",
    )
    assert r1.granted is True
    await ledger.commit(
        reservation_id=r1.reservation_id,
        actual_tokens=4500,
        interaction_id="int-1",
        customer_id="cust-a",
        trace_id="trace-1",
    )

    r2 = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-2",
        est_tokens=1000, lane="hot",
    )
    assert r2.granted is False
    assert r2.reason == "global_full"


# ──────────────────────────────────────────────────────────────────────────────
# AC2 — per-customer isolation
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_customer_a_exhausting_budget_does_not_block_customer_b(ledger, configs):
    """AC2: if Customer A exhausts THEIR reserved TPM, Customer B can still acquire."""
    configs["cust-a"] = _CustomerConfig("cust-a", reserved_tpm=2000, burst_weight=0)
    configs["cust-b"] = _CustomerConfig("cust-b", reserved_tpm=2000, burst_weight=0)

    # Customer A exhausts their budget.
    r_a1 = await ledger.acquire(
        customer_id="cust-a", interaction_id="a-1",
        est_tokens=2000, lane="hot",
    )
    assert r_a1.granted is True

    r_a2 = await ledger.acquire(
        customer_id="cust-a", interaction_id="a-2",
        est_tokens=500, lane="hot",
    )
    assert r_a2.granted is False  # A's pending+est exceeds 2000

    # Customer B still gets their full budget.
    r_b1 = await ledger.acquire(
        customer_id="cust-b", interaction_id="b-1",
        est_tokens=2000, lane="hot",
    )
    assert r_b1.granted is True


# ──────────────────────────────────────────────────────────────────────────────
# Refund on commit (§4.6)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_commit_refunds_over_reservation(ledger, configs):
    """Reserve est=2000, commit actual=1200 → 800 returned to budget."""
    configs["cust-a"] = _CustomerConfig("cust-a", reserved_tpm=2500, burst_weight=0)

    r1 = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-1",
        est_tokens=2000, lane="hot",
    )
    assert r1.granted is True

    await ledger.commit(
        reservation_id=r1.reservation_id,
        actual_tokens=1200,
        interaction_id="int-1",
        customer_id="cust-a",
        trace_id="trace-1",
    )

    # Window now shows 1200 used; 1300 remaining of 2500 budget.
    # A 1300-token request should still be granted.
    r2 = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-2",
        est_tokens=1300, lane="hot",
    )
    assert r2.granted is True


# ──────────────────────────────────────────────────────────────────────────────
# Reservation lease + sweeper (§4.5)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_sweeper_reclaims_expired_reservation(ledger, configs, fake_redis,
                                                     monkeypatch):
    """A stranded reservation past its lease is refunded by the sweeper."""
    monkeypatch.setattr(settings, "RESERVATION_LEASE_SECONDS", 0)  # immediately expirable
    configs["cust-a"] = _CustomerConfig("cust-a", reserved_tpm=2000, burst_weight=0)

    r1 = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-1",
        est_tokens=1500, lane="hot",
    )
    assert r1.granted is True

    # Without sweeping: pending=1500 → next acquire of 600 fails (1500+600 > 2000).
    r2_pre = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-2",
        est_tokens=600, lane="hot",
    )
    assert r2_pre.granted is False

    # Sweeper expires the stale reservation.
    expired = await ledger.sweep_expired_reservations()
    assert expired >= 1

    # After sweeping: pending=0, the same acquire now succeeds.
    r2 = await ledger.acquire(
        customer_id="cust-a", interaction_id="int-2",
        est_tokens=600, lane="hot",
    )
    assert r2.granted is True
