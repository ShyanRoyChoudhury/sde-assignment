-- Post-call pipeline v2 — additive migration.
-- Schema additions per SUBMISSION.md §10. Reversible; no backfill.

-- ──────────────────────────────────────────────────────────────────────────────
-- 1. New columns on `interactions`
-- ──────────────────────────────────────────────────────────────────────────────

ALTER TABLE interactions
    ADD COLUMN IF NOT EXISTS trace_id                  UUID,
    ADD COLUMN IF NOT EXISTS lane                      VARCHAR(8),
    ADD COLUMN IF NOT EXISTS classifier_verdict        JSONB,
    ADD COLUMN IF NOT EXISTS analyzed_at               TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recording_status          VARCHAR(32) NOT NULL DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS recording_attempt_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recording_last_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS recording_terminal_at     TIMESTAMPTZ;

-- Status enum becomes a CHECK-constrained VARCHAR to allow new values without
-- ALTER TYPE complexity across rolling worker restarts.
DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_name = 'interactions' AND column_name = 'status') != 'character varying' THEN
        ALTER TABLE interactions ALTER COLUMN status TYPE VARCHAR(32);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.constraint_column_usage
                   WHERE constraint_name = 'chk_interactions_status') THEN
        ALTER TABLE interactions
            ADD CONSTRAINT chk_interactions_status CHECK (status IN (
                'INITIATED', 'RINGING', 'IN_PROGRESS', 'ENDED', 'FAILED',
                'PROCESSING',
                'ANALYZING', 'ANALYZED', 'ANALYSIS_SKIPPED', 'DEAD_LETTERED'
            ));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_interactions_trace
    ON interactions (trace_id);
CREATE INDEX IF NOT EXISTS ix_interactions_recording_pending
    ON interactions (recording_status)
    WHERE recording_status IN ('pending', 'uploading');
CREATE INDEX IF NOT EXISTS ix_interactions_status_lane
    ON interactions (status, lane);

-- ──────────────────────────────────────────────────────────────────────────────
-- 2. token_ledger — billing-grade record of every successful LLM call
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS token_ledger (
    id              BIGSERIAL PRIMARY KEY,
    interaction_id  UUID NOT NULL,
    trace_id        UUID NOT NULL,
    customer_id     UUID NOT NULL,
    campaign_id     UUID,
    tokens_used     INTEGER NOT NULL,
    model           VARCHAR(64) NOT NULL,
    provider        VARCHAR(32) NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_tokens_nonneg CHECK (tokens_used >= 0)
);

CREATE INDEX IF NOT EXISTS ix_ledger_customer_time
    ON token_ledger (customer_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_ledger_campaign_time
    ON token_ledger (campaign_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_ledger_recent_global
    ON token_ledger (occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_ledger_interaction
    ON token_ledger (interaction_id);

-- ──────────────────────────────────────────────────────────────────────────────
-- 3. customer_config — per-customer budgeting parameters
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customer_config (
    customer_id    UUID PRIMARY KEY,
    reserved_tpm   INTEGER NOT NULL DEFAULT 0,
    burst_weight   INTEGER NOT NULL DEFAULT 1,
    max_tpm        INTEGER,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_reserved_nonneg CHECK (reserved_tpm >= 0),
    CONSTRAINT chk_weight_nonneg   CHECK (burst_weight >= 0),
    CONSTRAINT chk_max_positive    CHECK (max_tpm IS NULL OR max_tpm > 0)
);

-- ──────────────────────────────────────────────────────────────────────────────
-- 4. campaign_config — per-campaign reservation share override
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS campaign_config (
    campaign_id        UUID PRIMARY KEY,
    customer_id        UUID NOT NULL,
    reserved_share     NUMERIC(4,3) NOT NULL DEFAULT 1.000,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_share_range CHECK (reserved_share >= 0 AND reserved_share <= 1)
);

CREATE INDEX IF NOT EXISTS ix_campaign_customer
    ON campaign_config (customer_id);

-- ──────────────────────────────────────────────────────────────────────────────
-- 5. interaction_events — append-only audit log
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS interaction_events (
    id              BIGSERIAL PRIMARY KEY,
    interaction_id  UUID NOT NULL,
    trace_id        UUID NOT NULL,

    stage           VARCHAR(32) NOT NULL,
    status          VARCHAR(16) NOT NULL,
    source          VARCHAR(32),
    metadata        JSONB NOT NULL DEFAULT '{}',

    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_events_interaction_time
    ON interaction_events (interaction_id, occurred_at);
CREATE INDEX IF NOT EXISTS ix_events_trace
    ON interaction_events (trace_id);
CREATE INDEX IF NOT EXISTS ix_events_stage_time
    ON interaction_events (stage, occurred_at DESC);

-- ──────────────────────────────────────────────────────────────────────────────
-- 6. signal_outbox — durable side-effect queue
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS signal_outbox (
    id              BIGSERIAL PRIMARY KEY,
    interaction_id  UUID NOT NULL,
    customer_id     UUID NOT NULL,
    trace_id        UUID NOT NULL,

    dispatch_type   VARCHAR(32) NOT NULL,
    payload         JSONB NOT NULL,
    idempotency_key UUID NOT NULL DEFAULT uuid_generate_v4(),

    status          VARCHAR(16) NOT NULL DEFAULT 'pending',
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error      TEXT,
    in_progress_at  TIMESTAMPTZ,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at   TIMESTAMPTZ,

    CONSTRAINT chk_outbox_status CHECK (status IN
        ('pending', 'in_progress', 'dispatched', 'failed'))
);

CREATE INDEX IF NOT EXISTS ix_outbox_dispatch_ready
    ON signal_outbox (next_attempt_at, id)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS ix_outbox_in_progress
    ON signal_outbox (in_progress_at)
    WHERE status = 'in_progress';
CREATE INDEX IF NOT EXISTS ix_outbox_interaction
    ON signal_outbox (interaction_id);

-- ──────────────────────────────────────────────────────────────────────────────
-- 7. dead_letter_queue — terminal failures awaiting human review
-- ──────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id               BIGSERIAL PRIMARY KEY,
    interaction_id   UUID,
    customer_id      UUID,
    trace_id         UUID,

    source           VARCHAR(32) NOT NULL,
    reason           VARCHAR(64) NOT NULL,
    error_history    JSONB NOT NULL,
    original_payload JSONB NOT NULL,

    status           VARCHAR(16) NOT NULL DEFAULT 'pending_review',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at      TIMESTAMPTZ,
    reviewed_by      VARCHAR(128),
    review_notes     TEXT,

    CONSTRAINT chk_dlq_status CHECK (status IN
        ('pending_review', 'replayed', 'ignored', 'escalated'))
);

CREATE INDEX IF NOT EXISTS ix_dlq_pending
    ON dead_letter_queue (created_at)
    WHERE status = 'pending_review';
CREATE INDEX IF NOT EXISTS ix_dlq_interaction
    ON dead_letter_queue (interaction_id);
CREATE INDEX IF NOT EXISTS ix_dlq_source_time
    ON dead_letter_queue (source, created_at DESC);
