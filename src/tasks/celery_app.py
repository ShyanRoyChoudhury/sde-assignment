from celery import Celery
from celery.schedules import schedule

from src.config import settings

celery_app = Celery(
    "voicebot",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    # Late-ack: a worker crash redelivers the message rather than losing it.
    # Combined with the durability story in §8.11 (lease + outbox + DLQ),
    # this gives end-to-end no-loss semantics.
    task_acks_late=True,
    # Workers process one task at a time per process; concurrency comes from
    # running multiple worker processes / using gevent pool. Keeping this at 1
    # gives predictable backpressure and avoids head-of-line blocking on slow tasks.
    worker_prefetch_multiplier=1,
    # Default queue used only by ad-hoc tasks; pipeline tasks declare their queue.
    task_default_queue=settings.QUEUE_TRIAGE,
)

# Beat schedule (§4.5, §4.9, §8.4) — runs in a Celery beat process alongside workers.
celery_app.conf.beat_schedule = {
    "ledger-sweep-expired": {
        "task": "ledger_sweep_expired",
        "schedule": schedule(run_every=settings.SWEEPER_INTERVAL_SECONDS),
    },
    "pressure-publish": {
        "task": "pressure_publish",
        "schedule": schedule(run_every=settings.PRESSURE_REFRESH_SECONDS),
    },
    "outbox-beat": {
        "task": "outbox_beat",
        "schedule": schedule(run_every=settings.OUTBOX_BEAT_INTERVAL_SECONDS),
    },
    "outbox-reconcile-stuck": {
        "task": "outbox_reconcile_stuck",
        "schedule": schedule(run_every=300),  # every 5 minutes
    },
    "recording-reconcile-stuck": {
        "task": "recording_reconcile_stuck",
        "schedule": schedule(run_every=settings.RECORDING_RECONCILIATION_INTERVAL_SECONDS),
    },
}
