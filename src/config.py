import os


class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/voicebot"
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
    CELERY_RESULT_BACKEND: str = os.getenv(
        "CELERY_RESULT_BACKEND", "redis://localhost:6379/2"
    )

    # ── LLM ───────────────────────────────────────────────────────────────────
    # Hard, platform-wide ceilings. Subdivided across customers via the TPM
    # ledger (§4) and the per-customer budget formula (§5).
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", "openai")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "sk-mock-key-for-assessment")
    LLM_TOKENS_PER_MINUTE: int = int(os.getenv("LLM_TOKENS_PER_MINUTE", "90000"))
    LLM_REQUESTS_PER_MINUTE: int = int(os.getenv("LLM_REQUESTS_PER_MINUTE", "500"))
    LLM_AVG_TOKENS_PER_CALL: int = int(os.getenv("LLM_AVG_TOKENS_PER_CALL", "1500"))
    LLM_MAX_COMPLETION_TOKENS: int = int(os.getenv("LLM_MAX_COMPLETION_TOKENS", "500"))
    LLM_REQUEST_HARD_TIMEOUT_SECONDS: int = int(os.getenv("LLM_REQUEST_HARD_TIMEOUT_SECONDS", "30"))

    # ── Per-customer budgeting (§5) ───────────────────────────────────────────
    # Reserved/burst split. Σ reserved_tpm across customers may not exceed
    # RESERVED_FRACTION × LLM_TOKENS_PER_MINUTE (admission rule, §5.6).
    RESERVED_FRACTION: float = float(os.getenv("RESERVED_FRACTION", "0.70"))
    BURST_FRACTION: float = float(os.getenv("BURST_FRACTION", "0.30"))
    MIN_FAIR_SHARE_TPM: int = int(os.getenv("MIN_FAIR_SHARE_TPM", "50"))
    MAX_UNRESERVED_TPM: int = int(os.getenv("MAX_UNRESERVED_TPM", "5000"))
    DEFAULT_BURST_WEIGHT: int = int(os.getenv("DEFAULT_BURST_WEIGHT", "1"))
    ACTIVE_BURST_TTL_SECONDS: int = int(os.getenv("ACTIVE_BURST_TTL_SECONDS", "30"))

    # ── TPM ledger (§4) ───────────────────────────────────────────────────────
    RESERVATION_LEASE_SECONDS: int = int(os.getenv("RESERVATION_LEASE_SECONDS", "60"))
    SWEEPER_INTERVAL_SECONDS: int = int(os.getenv("SWEEPER_INTERVAL_SECONDS", "10"))
    MIN_RETRY_BACKOFF_MS: int = int(os.getenv("MIN_RETRY_BACKOFF_MS", "1000"))
    MAX_DEFER_ATTEMPTS: int = int(os.getenv("MAX_DEFER_ATTEMPTS", "50"))

    # ── Pressure gauge (§4.9) ─────────────────────────────────────────────────
    PRESSURE_GAUGE_KEY: str = "platform_pressure"
    PRESSURE_GAUGE_TTL_SECONDS: int = int(os.getenv("PRESSURE_GAUGE_TTL_SECONDS", "15"))
    PRESSURE_REFRESH_SECONDS: int = int(os.getenv("PRESSURE_REFRESH_SECONDS", "5"))
    TARGET_COLD_QUEUE_DEPTH: int = int(os.getenv("TARGET_COLD_QUEUE_DEPTH", "10000"))

    # ── Triage classifier (§6) ────────────────────────────────────────────────
    SHORT_TRANSCRIPT_TURN_THRESHOLD: int = int(os.getenv("SHORT_TRANSCRIPT_TURN_THRESHOLD", "4"))

    # Keyword sets — platform-wide. Per-customer overrides deferred (§15.1).
    SKIP_NEGATIVE_RULES: dict = {
        "wrong_number": ["wrong number"],
        "do_not_call": [
            "stop calling",
            "don't call again",
            "do not call",
            "remove my number",
            "take me off your list",
        ],
    }
    HOT_RULES: dict = {
        "rebook_confirmed": [
            "confirmed",
            "booked your slot",
            "scheduled for",
            "i've booked",
        ],
        "demo_booked": [
            "demo is booked",
            "calendar invite",
            "looking forward to",
        ],
        "escalation_needed": [
            "manager",
            "complaint",
            "unacceptable",
            "file a complaint",
            "escalate",
            "senior executive",
        ],
    }
    COLD_RULES: dict = {
        "not_interested": ["not interested"],
        "already_done": ["already booked", "already done", "already purchased"],
        "callback_requested": ["call back later", "call me later", "call me back"],
    }

    # ── Recording pipeline (§7) ───────────────────────────────────────────────
    # Replaces the old hardcoded RECORDING_WAIT_SECONDS=45.
    RECORDING_POLL_SCHEDULE_SECONDS: list = [5, 10, 30, 60, 120, 240]
    RECORDING_JITTER_FRACTION: float = float(os.getenv("RECORDING_JITTER_FRACTION", "0.20"))
    RECORDING_RECONCILIATION_INTERVAL_SECONDS: int = int(
        os.getenv("RECORDING_RECONCILIATION_INTERVAL_SECONDS", "300")
    )
    RECORDING_STUCK_THRESHOLD_SECONDS: int = int(
        os.getenv("RECORDING_STUCK_THRESHOLD_SECONDS", "120")
    )
    EXOTEL_HTTP_TIMEOUT_SECONDS: int = int(os.getenv("EXOTEL_HTTP_TIMEOUT_SECONDS", "10"))
    S3_BUCKET: str = os.getenv("S3_BUCKET", "voicebot-recordings")

    # ── Outbox dispatcher (§8) ────────────────────────────────────────────────
    OUTBOX_BEAT_INTERVAL_SECONDS: int = int(os.getenv("OUTBOX_BEAT_INTERVAL_SECONDS", "1"))
    OUTBOX_BATCH_SIZE: int = int(os.getenv("OUTBOX_BATCH_SIZE", "200"))
    OUTBOX_MAX_RETRIES: int = int(os.getenv("OUTBOX_MAX_RETRIES", "5"))
    # Wait BEFORE attempt N (index 0 unused; attempt 1 is initial).
    OUTBOX_RETRY_SCHEDULE_SECONDS: list = [0, 30, 120, 600, 3600]
    OUTBOX_STUCK_THRESHOLD_SECONDS: int = int(
        os.getenv("OUTBOX_STUCK_THRESHOLD_SECONDS", "120")
    )

    # ── Celery queues ─────────────────────────────────────────────────────────
    QUEUE_TRIAGE: str = "triage"
    QUEUE_HOT: str = "postcall_hot"
    QUEUE_COLD: str = "postcall_cold"
    QUEUE_RECORDING: str = "recording"
    QUEUE_OUTBOX: str = "outbox"

    # ── Legacy (kept for backward compatibility during cutover) ───────────────
    POSTCALL_CELERY_QUEUE: str = "postcall_processing"
    POSTCALL_MAX_RETRIES: int = 3
    POSTCALL_RETRY_DELAY: int = 60
    RECORDING_WAIT_SECONDS: int = 45  # deprecated; recording.py no longer uses this


settings = Settings()
