"""
Recording poller tests (SUBMISSION.md §7, AC4).

AC4: Recording poller retries with backoff; never silently skips.
     Unit test: simulate delayed recording, verify retry loop and failure logging.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.config import settings
from src.services.recording import (
    FetchResult,
    FetchStatus,
    poll_delay_for_attempt,
)


# ──────────────────────────────────────────────────────────────────────────────
# Poll schedule shape
# ──────────────────────────────────────────────────────────────────────────────


def test_poll_schedule_returns_jittered_values_for_each_attempt():
    """Each scheduled attempt returns a positive delay within ±20% of the base."""
    schedule = settings.RECORDING_POLL_SCHEDULE_SECONDS
    for attempt_idx, base in enumerate(schedule, start=1):
        delay = poll_delay_for_attempt(attempt_idx)
        assert delay is not None
        # Jitter is ±20%
        assert delay >= base * 0.8
        assert delay <= base * 1.2


def test_poll_schedule_returns_none_when_exhausted():
    """Beyond the schedule length, returns None (signal to terminate)."""
    schedule_len = len(settings.RECORDING_POLL_SCHEDULE_SECONDS)
    assert poll_delay_for_attempt(schedule_len + 1) is None
    assert poll_delay_for_attempt(0) is None


# ──────────────────────────────────────────────────────────────────────────────
# Fetch result classification
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_404_classified_as_not_ready():
    """A 404 from Exotel means 'not yet ready' — keep polling."""
    from src.services import recording

    mock_response = AsyncMock()
    mock_response.status_code = 404

    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("src.services.recording.httpx.AsyncClient", return_value=mock_client):
        result = await recording.fetch_exotel_recording("call-123", "acct-456")

    assert result.status == FetchStatus.NOT_READY


@pytest.mark.asyncio
async def test_500_classified_as_transient_error():
    """A 5xx is transient — keep polling per schedule."""
    from src.services import recording

    mock_response = AsyncMock()
    mock_response.status_code = 503

    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("src.services.recording.httpx.AsyncClient", return_value=mock_client):
        result = await recording.fetch_exotel_recording("call-123", "acct-456")

    assert result.status == FetchStatus.TRANSIENT_ERROR
    assert result.error_detail == "http_503"


@pytest.mark.asyncio
async def test_403_classified_as_permanent_error():
    """A 401/403 means config issue — fail immediately and alert."""
    from src.services import recording

    mock_response = AsyncMock()
    mock_response.status_code = 403

    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("src.services.recording.httpx.AsyncClient", return_value=mock_client):
        result = await recording.fetch_exotel_recording("call-123", "acct-456")

    assert result.status == FetchStatus.PERMANENT_ERROR
    assert result.error_detail == "auth_failure_403"


@pytest.mark.asyncio
async def test_200_with_url_classified_as_ready():
    """A 200 with recording_url means we're ready to upload."""
    from src.services import recording

    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json = lambda: {"recording_url": "https://example.com/rec.mp3"}

    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None
    mock_client.get = AsyncMock(return_value=mock_response)

    with patch("src.services.recording.httpx.AsyncClient", return_value=mock_client):
        result = await recording.fetch_exotel_recording("call-123", "acct-456")

    assert result.status == FetchStatus.READY
    assert result.url == "https://example.com/rec.mp3"


@pytest.mark.asyncio
async def test_network_error_classified_as_transient():
    """A network error is treated as transient — keep polling."""
    import httpx
    from src.services import recording

    mock_client = AsyncMock()
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None
    mock_client.get = AsyncMock(side_effect=httpx.ConnectError("nope"))

    with patch("src.services.recording.httpx.AsyncClient", return_value=mock_client):
        result = await recording.fetch_exotel_recording("call-123", "acct-456")

    assert result.status == FetchStatus.TRANSIENT_ERROR
    assert result.error_detail.startswith("network:")
