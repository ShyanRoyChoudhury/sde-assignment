"""
TPM ledger — the rate-limit enforcement layer (SUBMISSION.md §4).

Two layers:
  - Enforcement counter in Redis: pre-flight gate, sub-millisecond, rebuildable
  - Durable record in Postgres `token_ledger`: billing-grade, zero-loss

Lifecycle:
    acquire(customer, est_tokens, lane) → granted(reservation_id) | denied(retry_after_ms)
    LLM call
    commit(reservation_id, actual_tokens) → durable Postgres write + Redis decrement

Reservation safety: every reservation has a `lease_until` stamped at acquire
time. A periodic sweeper (Celery beat) reclaims stranded reservations whose
worker crashed.

Refund: actual ≤ est always (since est = prompt + max_completion). On commit,
pending is decremented by est and committed is incremented by actual — the
over-reservation is refunded so the customer is billed for actual usage only.
"""

import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

from sqlalchemy import text

from src.config import settings
from src.utils.db import async_session_factory
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

Lane = Literal["hot", "cold"]

# ──────────────────────────────────────────────────────────────────────────────
# Redis key conventions
# ──────────────────────────────────────────────────────────────────────────────


def _bucket_key(customer_id: str, minute_epoch: int) -> str:
    return f"tpm:cust:{customer_id}:{minute_epoch}"


def _global_bucket_key(minute_epoch: int) -> str:
    return f"tpm:global:{minute_epoch}"


def _pending_key(customer_id: str) -> str:
    return f"pending:cust:{customer_id}"


def _reservation_key(reservation_id: str) -> str:
    return f"reservation:{reservation_id}"


def _active_burst_key(customer_id: str) -> str:
    return f"active_burst:{customer_id}"


RESERVATIONS_BY_LEASE = "reservations_by_lease"  # Redis sorted set


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class AcquireResult:
    granted: bool
    reservation_id: Optional[str] = None
    retry_after_ms: Optional[int] = None
    reason: Optional[str] = None  # 'global_full' | 'customer_full' | 'burst_starved'


# Lua script: atomic acquire.
# KEYS[1] = pending_key
# KEYS[2] = current_minute_bucket (customer)
# KEYS[3] = previous_minute_bucket (customer)
# KEYS[4] = global current_minute_bucket
# KEYS[5] = global previous_minute_bucket
# KEYS[6] = reservation hash key
# KEYS[7] = reservations_by_lease sorted set
# ARGV[1] = elapsed_in_current (0..59)
# ARGV[2] = customer_effective_budget (int)
# ARGV[3] = global_limit (int)
# ARGV[4] = est_tokens
# ARGV[5] = reservation_id
# ARGV[6] = lease_until (unix seconds)
# ARGV[7] = customer_id
# Returns: {1, reservation_id} on grant; {0, retry_after_ms, reason} on deny.
LUA_ACQUIRE = """
local pending = tonumber(redis.call('GET', KEYS[1]) or '0')
local cur     = tonumber(redis.call('GET', KEYS[2]) or '0')
local prev    = tonumber(redis.call('GET', KEYS[3]) or '0')
local g_cur   = tonumber(redis.call('GET', KEYS[4]) or '0')
local g_prev  = tonumber(redis.call('GET', KEYS[5]) or '0')

local elapsed = tonumber(ARGV[1])
local weight_prev = (60 - elapsed) / 60.0
local cust_window  = math.floor(weight_prev * prev + cur)
local glob_window  = math.floor(weight_prev * g_prev + g_cur)

local cust_budget = tonumber(ARGV[2])
local glob_limit  = tonumber(ARGV[3])
local est         = tonumber(ARGV[4])

local cust_saturation = cust_window + pending
if cust_saturation + est > cust_budget then
    local retry_ms = (60 - elapsed) * 1000
    return {0, retry_ms, 'customer_full'}
end

if glob_window + est > glob_limit then
    local retry_ms = (60 - elapsed) * 1000
    return {0, retry_ms, 'global_full'}
end

-- Grant: bump pending, write reservation hash, index by lease.
redis.call('INCRBY', KEYS[1], est)
redis.call('HSET', KEYS[6],
    'customer_id', ARGV[7],
    'tokens',      est,
    'lease_until', ARGV[6])
redis.call('EXPIRE', KEYS[6], 600)
redis.call('ZADD', KEYS[7], ARGV[6], ARGV[5])
return {1, ARGV[5]}
"""

# Lua script: atomic commit (normal path — reservation still alive).
# KEYS[1] = reservation hash key
# KEYS[2] = pending_key
# KEYS[3] = customer current_minute_bucket
# KEYS[4] = global current_minute_bucket
# KEYS[5] = reservations_by_lease sorted set
# ARGV[1] = reservation_id
# ARGV[2] = actual_tokens
# Returns: {1, est_tokens} on normal commit; {0} if reservation already expired (late path).
LUA_COMMIT = """
local res = redis.call('HGETALL', KEYS[1])
if #res == 0 then
    return {0}
end

-- Build a table from the HGETALL list.
local h = {}
for i = 1, #res, 2 do h[res[i]] = res[i+1] end
local est = tonumber(h['tokens'])
local actual = tonumber(ARGV[2])

redis.call('DECRBY', KEYS[2], est)
if actual > 0 then
    redis.call('INCRBY', KEYS[3], actual)
    redis.call('EXPIRE', KEYS[3], 120)
    redis.call('INCRBY', KEYS[4], actual)
    redis.call('EXPIRE', KEYS[4], 120)
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[5], ARGV[1])
return {1, est}
"""


class TpmLedger:
    """
    Public interface for the TPM ledger. Stateless; all state is in Redis +
    Postgres. Safe to instantiate as a module-level singleton.
    """

    async def effective_budget(self, customer_id: str) -> int:
        """
        Compute customer's effective TPM ceiling at this moment per §5.2:

            reserved_tpm + (burst_weight / Σ_active_burst_weights) × burst_pool
            clamped by [MIN_FAIR_SHARE, max_tpm]
        """
        cfg = await _load_customer_config(customer_id)
        reserved = cfg.reserved_tpm
        weight = cfg.burst_weight
        max_tpm = cfg.max_tpm

        burst_pool = int(settings.LLM_TOKENS_PER_MINUTE * settings.BURST_FRACTION)
        burst_share = 0
        if weight > 0:
            sum_active = await _sum_active_burst_weights()
            # Include this customer's weight if not yet active (we're about to be).
            if sum_active <= 0:
                sum_active = weight
            burst_share = int((weight / sum_active) * burst_pool)

        effective = reserved + burst_share
        if max_tpm is not None:
            effective = min(effective, max_tpm)
        if weight > 0 and effective < settings.MIN_FAIR_SHARE_TPM:
            effective = settings.MIN_FAIR_SHARE_TPM
        return effective

    async def acquire(
        self,
        *,
        customer_id: str,
        interaction_id: str,
        est_tokens: int,
        lane: Lane,
    ) -> AcquireResult:
        """
        Pre-flight gate. Returns granted with a reservation_id, or denied
        with retry_after_ms and a reason. The LLM call MUST NOT fire if denied.
        """
        if est_tokens <= 0:
            raise ValueError("est_tokens must be positive")

        budget = await self.effective_budget(customer_id)
        global_limit = settings.LLM_TOKENS_PER_MINUTE
        cfg = await _load_customer_config(customer_id)

        # Mark this customer as actively bursting (TTL-decayed) so future
        # acquires correctly weight Σ_active_burst_weights.
        if cfg.burst_weight > 0:
            await redis_client.set(
                _active_burst_key(customer_id),
                cfg.burst_weight,
                ex=settings.ACTIVE_BURST_TTL_SECONDS,
            )

        now = time.time()
        elapsed = int(now) % 60
        cur_minute = int(now) // 60
        prev_minute = cur_minute - 1
        reservation_id = uuid.uuid4().hex
        lease_until = now + settings.RESERVATION_LEASE_SECONDS

        keys = [
            _pending_key(customer_id),
            _bucket_key(customer_id, cur_minute),
            _bucket_key(customer_id, prev_minute),
            _global_bucket_key(cur_minute),
            _global_bucket_key(prev_minute),
            _reservation_key(reservation_id),
            RESERVATIONS_BY_LEASE,
        ]
        argv = [
            elapsed,
            budget,
            global_limit,
            est_tokens,
            reservation_id,
            int(lease_until),
            customer_id,
        ]

        result = await redis_client.eval(LUA_ACQUIRE, len(keys), *keys, *argv)
        # redis-py returns list of bytes/str; normalise to ints/strs.
        granted = int(result[0]) == 1

        if granted:
            logger.debug(
                "ledger_acquire_granted",
                extra={
                    "customer_id": customer_id,
                    "interaction_id": interaction_id,
                    "reservation_id": reservation_id,
                    "est_tokens": est_tokens,
                    "lane": lane,
                    "effective_budget": budget,
                },
            )
            return AcquireResult(granted=True, reservation_id=reservation_id)

        retry_after_ms = max(int(result[1]), settings.MIN_RETRY_BACKOFF_MS)
        reason = result[2].decode() if isinstance(result[2], (bytes, bytearray)) else str(result[2])
        logger.info(
            "ledger_acquire_denied",
            extra={
                "customer_id": customer_id,
                "interaction_id": interaction_id,
                "lane": lane,
                "est_tokens": est_tokens,
                "reason": reason,
                "retry_after_ms": retry_after_ms,
            },
        )
        return AcquireResult(
            granted=False,
            retry_after_ms=retry_after_ms,
            reason=reason,
        )

    async def commit(
        self,
        *,
        reservation_id: str,
        actual_tokens: int,
        interaction_id: str,
        customer_id: str,
        trace_id: str,
        campaign_id: Optional[str] = None,
        model: Optional[str] = None,
        provider: Optional[str] = None,
    ) -> None:
        """
        Commit a successful (or partial) LLM call. Refunds over-reservation,
        increments committed buckets, writes durable Postgres ledger row.

        Tolerates the late-commit path: if the sweeper already expired the
        reservation, we just write the actual into the committed bucket and
        log the warning.
        """
        if actual_tokens < 0:
            raise ValueError("actual_tokens must be non-negative")

        now = time.time()
        cur_minute = int(now) // 60

        keys = [
            _reservation_key(reservation_id),
            _pending_key(customer_id),
            _bucket_key(customer_id, cur_minute),
            _global_bucket_key(cur_minute),
            RESERVATIONS_BY_LEASE,
        ]
        argv = [reservation_id, actual_tokens]

        result = await redis_client.eval(LUA_COMMIT, len(keys), *keys, *argv)
        normal_commit = int(result[0]) == 1

        if not normal_commit:
            # Late-commit path — sweeper already expired the reservation.
            # Just bump the committed buckets so accounting is right going forward.
            await redis_client.incrby(_bucket_key(customer_id, cur_minute), actual_tokens)
            await redis_client.expire(_bucket_key(customer_id, cur_minute), 120)
            await redis_client.incrby(_global_bucket_key(cur_minute), actual_tokens)
            await redis_client.expire(_global_bucket_key(cur_minute), 120)
            logger.warning(
                "commit_after_lease_expiry",
                extra={
                    "reservation_id": reservation_id,
                    "interaction_id": interaction_id,
                    "actual_tokens": actual_tokens,
                },
            )

        # Durable record.
        async with async_session_factory() as session:
            await session.execute(
                text(
                    """
                    INSERT INTO token_ledger
                        (interaction_id, trace_id, customer_id, campaign_id,
                         tokens_used, model, provider)
                    VALUES
                        (:interaction_id, :trace_id, :customer_id, :campaign_id,
                         :tokens_used, :model, :provider)
                    """
                ),
                {
                    "interaction_id": interaction_id,
                    "trace_id": trace_id,
                    "customer_id": customer_id,
                    "campaign_id": campaign_id,
                    "tokens_used": actual_tokens,
                    "model": model or settings.LLM_MODEL,
                    "provider": provider or settings.LLM_PROVIDER,
                },
            )
            await session.commit()

    async def sweep_expired_reservations(self) -> int:
        """
        Reclaim reservations whose lease has expired. Returns count expired.
        Called periodically (every SWEEPER_INTERVAL_SECONDS) by a Celery beat task.
        """
        now = int(time.time())
        expired_ids = await redis_client.zrangebyscore(RESERVATIONS_BY_LEASE, 0, now)
        if not expired_ids:
            return 0

        count = 0
        for raw in expired_ids:
            rid = raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
            res = await redis_client.hgetall(_reservation_key(rid))
            if not res:
                # Already gone (committed in the meantime).
                await redis_client.zrem(RESERVATIONS_BY_LEASE, rid)
                continue

            # Normalise keys (decode_responses=True returns strs already).
            cust = res.get("customer_id") if isinstance(res, dict) else None
            tokens = int(res.get("tokens", 0)) if cust else 0
            if cust and tokens > 0:
                await redis_client.decrby(_pending_key(cust), tokens)
            await redis_client.delete(_reservation_key(rid))
            await redis_client.zrem(RESERVATIONS_BY_LEASE, rid)
            count += 1
            logger.info(
                "reservation_expired",
                extra={"reservation_id": rid, "customer_id": cust, "tokens": tokens},
            )

        return count

    async def global_window_used(self) -> int:
        """Return current smoothed global TPM-window usage (for the pressure gauge)."""
        now = time.time()
        elapsed = int(now) % 60
        cur_minute = int(now) // 60
        prev_minute = cur_minute - 1

        cur = int(await redis_client.get(_global_bucket_key(cur_minute)) or 0)
        prev = int(await redis_client.get(_global_bucket_key(prev_minute)) or 0)
        weight_prev = (60 - elapsed) / 60.0
        return int(weight_prev * prev + cur)


# ──────────────────────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class _CustomerConfig:
    customer_id: str
    reserved_tpm: int = 0
    burst_weight: int = 0
    max_tpm: Optional[int] = None


async def _load_customer_config(customer_id: str) -> _CustomerConfig:
    """
    Load customer_config row, INSERT defaults on first acquire (§5.4).
    """
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                """
                INSERT INTO customer_config (customer_id, reserved_tpm, burst_weight, max_tpm)
                VALUES (:cid, 0, :default_weight, :default_max)
                ON CONFLICT (customer_id) DO NOTHING
                RETURNING customer_id, reserved_tpm, burst_weight, max_tpm
                """
            ),
            {
                "cid": customer_id,
                "default_weight": settings.DEFAULT_BURST_WEIGHT,
                "default_max": settings.MAX_UNRESERVED_TPM,
            },
        )
        row = result.first()
        if row is not None:
            await session.commit()
            return _CustomerConfig(*row)

        # Already existed; fetch.
        result = await session.execute(
            text(
                """
                SELECT customer_id, reserved_tpm, burst_weight, max_tpm
                FROM customer_config WHERE customer_id = :cid
                """
            ),
            {"cid": customer_id},
        )
        row = result.first()
        await session.commit()
        return _CustomerConfig(*row)


async def _sum_active_burst_weights() -> int:
    """
    Sum weights of customers who have demanded burst capacity in the last
    ACTIVE_BURST_TTL_SECONDS. Implemented via SCAN on `active_burst:*` keys —
    cheap because the keyspace is small (only customers actively bursting,
    bounded by concurrent-active-customers).
    """
    total = 0
    cursor = 0
    while True:
        cursor, keys = await redis_client.scan(
            cursor=cursor, match="active_burst:*", count=200
        )
        if keys:
            values = await redis_client.mget(*keys)
            for v in values:
                if v is not None:
                    try:
                        total += int(v)
                    except (TypeError, ValueError):
                        pass
        if cursor == 0:
            break
    return total


# Module singleton
tpm_ledger = TpmLedger()
