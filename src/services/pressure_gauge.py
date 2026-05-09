"""
Continuous platform-pressure gauge (SUBMISSION.md §4.9).

Replaces the dead binary circuit breaker. Publishes a 0.0-1.0 float to a
documented Redis key derived from real constraints — TPM utilisation and
cold-lane queue depth — not RPM (which the legacy implementation tracked
but the provider does not enforce on).

The dialler (external) reads this gauge and applies its own response curve.
There is no trip state, no freeze. Stale-safe: 15s TTL on the published
value; if the publisher dies the key disappears and the dialler can detect
the missing signal and fall back to a sensible default.
"""

import asyncio
import logging
from typing import Optional

from src.config import settings
from src.services.tpm_ledger import tpm_ledger
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


async def compute_platform_pressure() -> float:
    """Return current platform pressure in [0.0, 1.0]."""
    tpm_used = await tpm_ledger.global_window_used()
    tpm_limit = settings.LLM_TOKENS_PER_MINUTE

    # Celery uses Redis as broker, so cold-lane queue depth is just the list length.
    # `_kombu.binding.<queue>` would be exact for routing, but the queue itself is
    # an LIST keyed by queue name in default kombu+Redis configuration.
    queue_depth = await redis_client.llen(settings.QUEUE_COLD) or 0

    tpm_ratio = tpm_used / tpm_limit if tpm_limit > 0 else 0.0
    queue_ratio = queue_depth / settings.TARGET_COLD_QUEUE_DEPTH \
        if settings.TARGET_COLD_QUEUE_DEPTH > 0 else 0.0

    pressure = max(tpm_ratio, queue_ratio)
    return min(1.0, max(0.0, pressure))


async def publish_pressure() -> float:
    """Compute and publish the current pressure value. Returns the value published."""
    pressure = await compute_platform_pressure()
    await redis_client.set(
        settings.PRESSURE_GAUGE_KEY,
        f"{pressure:.4f}",
        ex=settings.PRESSURE_GAUGE_TTL_SECONDS,
    )
    logger.debug(
        "platform_pressure_published",
        extra={"pressure": pressure},
    )
    return pressure


async def read_pressure() -> Optional[float]:
    """Return the most recently published pressure, or None if stale."""
    raw = await redis_client.get(settings.PRESSURE_GAUGE_KEY)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


async def run_publisher_loop(stop_event: Optional[asyncio.Event] = None) -> None:
    """Long-running loop that refreshes the gauge every PRESSURE_REFRESH_SECONDS."""
    while True:
        try:
            await publish_pressure()
        except Exception as e:
            logger.exception("pressure_publish_failed", extra={"error": str(e)})

        if stop_event is not None and stop_event.is_set():
            return

        await asyncio.sleep(settings.PRESSURE_REFRESH_SECONDS)
