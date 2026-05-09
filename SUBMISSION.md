# Post-Call Processing Pipeline — Design Document

**Author:** Shyan Roy Choudhury
**Date:** 2026-05-08

---

## 1. Assumptions

This section states the business, system, and environmental assumptions on which the rest of the design rests. They are explicit, testable, and we will defend them.

### A1. Hot vs cold is a business-defined classification, not an LLM output

A call is **hot** if its outcome triggers an action the business cares about completing in near-real time:

- `rebook_confirmed` — a customer confirmed a new appointment time
- `demo_booked` — a customer agreed to a product demo at a specific time
- `escalation_needed` — a customer demanded human escalation (the fixture explicitly commits to a 60-minute callback)
- `callback_requested` *with a specific time* — the customer named a return-call window

Everything else is **cold**: `not_interested`, `already_done`, `wrong_number`, vague "call me later", and any call that cannot be confidently classified by the cheap triage step.

The taxonomy is config-driven, not hard-coded. Customers can extend it via per-customer configuration without redeployment.

**Why this matters.** The classification drives lane assignment in the scheduler. A hot call enters a high-priority lane that draws from reserved TPM capacity; a cold call enters a fairly-shared bulk lane. If we made the classification the LLM's job, we would defeat the point — every call would consume LLM tokens before the system could decide how urgently to spend them.

### A2. The platform has one LLM provider relationship; rate limits are platform-wide hard ceilings

`LLM_TOKENS_PER_MINUTE` and `LLM_REQUESTS_PER_MINUTE` from `src/config.py` are shared by all customers. They cannot be raised by adding workers or distributing load. They can only be raised by negotiating with the provider.

This is what makes per-customer budgeting a real problem rather than a trivial one: every customer's allocation is a subdivision of the same finite pie.

### A3. Capacity is split 70% reserved / 30% burst; admission control gates new contracts

Up to 70% of platform TPM is sold as `reserved_tpm` to customers under contract. The remaining 30% is a shared burst pool, allocated by weighted fair queueing across active customers.

The 70/30 split aligns with published platform-engineering patterns: Kubernetes resource-request guidance (target 70–80% of node capacity), DynamoDB reserved-capacity breakeven (~70% sustained utilisation), and AWS general capacity-planning conventions. Stripe's 20/80 critical-traffic split solves a different problem (per-customer priority within a single tenant) and is not the right reference here. Telco-style 95% oversubscription does not apply, because voice-campaign duty cycles are *correlated* — everyone runs in business hours — which invalidates the statistical-multiplexing argument that justifies aggressive oversubscription elsewhere.

The 30% burst headroom is structurally load-bearing. It absorbs:
- upstream provider rate-limit jitter,
- retry bursts after transient errors,
- moments when several reserved customers simultaneously hit their ceiling.

**Admission rule.** The platform refuses to accept new reserved contracts if `Σ reserved_tpm + new_contract > 0.70 × platform_tpm`. An alert fires at 60% saturation — a capacity-planning trigger to negotiate higher upstream quota before the next contract.

**Customers without a reservation** are not starved. They draw from the burst pool with a minimum fair share, following Twilio's multi-tenant subaccount pattern.

### A4. Latency SLAs

| Tier | Target |
|------|--------|
| Hot | p95 end-to-end (call_end webhook → signal_jobs fired with real analysis) **< 20 seconds** |
| Cold | p95 **< 30 minutes** during sustained 100K-burst load; p99 within the 8-hour campaign window |
| Recording | Best-effort; failures are observable and alertable, never silent |

The 20s hot p95 is chosen deliberately. A human waiting on a confirmation message starts to wonder at ~15s and gets frustrated at ~30s. 20s gives p95 headroom over the LLM's own tail latency (typically 6–8s p95) without requiring heroics. p99 will breach 20s; that is acceptable, and breaches are logged for review rather than treated as outages.

### A5. The design point is 100K calls per 8-hour campaign window

The system is designed to drain 100K calls within 8 hours under normal load while respecting upstream rate limits. Beyond that scale, the platform sheds load by **extending cold-call latency**, not by dropping work and not by surfacing 429s to upstream callers.

### A6. The dialler is external; this design exposes a backpressure signal it consumes

We do not design or implement the dialler. We replace the dead binary circuit breaker with a continuous `platform_pressure` gauge (0.0–1.0) published to a documented Redis key with refresh semantics specified in §4.9. The dialler's response curve to that signal is its own concern; we promise the signal is fresh, accurate, and tied to the actually-constrained resource (TPM utilisation and queue depth, not RPM as the current implementation tracks).

### A7. No analysis result is ever permanently lost

Every interaction terminates in exactly one of three states:

- `analyzed` — full LLM analysis completed and persisted
- `analysis_skipped` — pre-LLM gate matched (turn count < 4 OR strong negative-keyword match such as `wrong_number` / `do_not_call`); `interaction_events.matched_rules` records the specific reason
- `dead_lettered_for_review` — retries exhausted, payload preserved in a DLQ table, alert fired

The third state is **not a silent drop**. It produces an alertable, human-actionable event with the full payload retained. Replay is a single SQL operation away.

### A8. Transcripts contain PII and are treated as sensitive data

Conversation transcripts contain names, phone numbers, financial intent, location references, and free-form natural-language identity disclosure. They are treated as sensitive throughout:

- **At rest** — encrypted. Column-level encryption on JSONB transcript fields with KMS-wrapped keys; recordings in S3 with SSE-KMS.
- **In logs** — redacted. Logs carry interaction IDs and structured metadata only, never transcript content.
- **In transit** — TLS-only between services and to external providers.
- **On human read** — access-logged. An engineer pulling a transcript for debugging produces an audit row.

### A9. Tokens are the unit of currency for budgeting and billing

Every LLM call attributes its `tokens_used` to a `(customer_id, campaign_id, interaction_id)` triple, persisted in a billing-grade ledger. This is non-negotiable — both budget enforcement and customer billing depend on it. Token accounting must remain correct even when retries, partial completions, and worker crashes occur.

### A10. Triage operates on English transcripts in v1

The fixtures include Hinglish and other multi-language transcripts; the v1 classifier is English-only. Any transcript that does not match an English keyword routes to the **hot lane by default**, where the full LLM handles language-aware classification. Multi-language keyword support is a known extension — the classifier interface accepts a `language` field but uses only the English keyword set in v1. This is the conservative choice: a non-English transcript pays full LLM cost rather than risking misclassification.

---

## 2. Problem Diagnosis

The codebase in `src/` is approximately 1,140 lines of business logic across six files. It worked correctly at ~1,000 calls per day. At the 100K-calls-per-8-hour-campaign target, several distinct failure modes compound. This section walks each module's responsibility and what is broken about that responsibility. Fixes are described in §3 onward.

### 2.1 API / Webhook layer (`src/api/endpoints.py`)

**Role.** Receive Exotel's call-end webhook, classify short vs long, hand off heavy work to Celery, return 200 within Exotel's 5s timeout.

- The endpoint fires `signal_jobs` and `update_lead_stage` with **empty payloads before Celery has run any analysis**. Celery later fires the real ones. Downstream systems (CRMs, WhatsApp) receive two events for one call — the first wrong. This is a correctness bug, not a performance bug, and it ships today.
- Both the short-path side effects and the long-path empty-payload side effects use `asyncio.create_task()` on the FastAPI event loop. A server restart between returning 200 and these tasks completing drops them with no record.
- No correlation/trace ID is generated. Cross-module debugging relies on joining log lines by `interaction_id` only, which loses the timing relationship between stages.

### 2.2 Task orchestration (`src/tasks/celery_tasks.py`, `src/tasks/celery_app.py`)

**Role.** Run the post-call processing pipeline: recording, analysis, signal jobs, lead stage.

- One monolithic task on a single shared queue means a "wrong number" hangup and a confirmed booking sit at the same priority in the same FIFO line.
- Workers are synchronous with `prefetch_multiplier=1`. Each worker process handles one I/O-bound task at a time, idle through ~45 seconds of recording wait and ~3.5 seconds of LLM latency. At 10 workers the 100K backlog drains in ~10 hours — longer than the campaign window.
- Two retry mechanisms (Celery's `self.retry()` and the bespoke `retry_queue`) fire on the same failure with no coordination, so a single failure can trigger two retries and double-spend the LLM budget.

### 2.3 Recording pipeline (`src/services/recording.py`)

**Role.** Fetch the call recording from Exotel and upload to S3.

- `asyncio.sleep(45)` is unconditional. Recordings ready in 5s waste 40s; recordings ready at 60s are silently dropped.
- The "not available" log line is at DEBUG level, invisible at production INFO. An ops engineer investigating "why is there no recording for interaction X?" finds nothing. Constraint 4 of the README explicitly forbids this.
- Recording fetch and LLM analysis are coupled in series despite being logically independent (the LLM reads the transcript text, not the audio). Every call waits the full recording window before any analysis begins.
- If the S3 upload succeeds but the DB write of `recording_s3_key` fails, the file is in S3 with no row pointing at it. There is no reconciliation job.

### 2.4 LLM analysis (`src/services/post_call_processor.py`)

**Role.** Call the LLM, extract `call_stage` / `entities` / `summary`, persist the result.

- Every long transcript receives the same ~1,500-token full analysis. A "wrong number" call burns the same budget as a confirmed rebook. The transcript fixtures explicitly tag each outcome with `expected_lane: hot|cold|skip` — the assignment expects this differentiation, and the current code does not implement it.
- Rate-limit awareness is reactive, not proactive. The Redis RPM counter is incremented *after* deciding to fire. At burst load, requests fire freely until 429s come back; retries pile up at the back of a 100K queue and worsen the backlog.
- There is no per-customer accounting anywhere. `tokens_used` is logged but not written to any aggregate counter. "How many tokens did Customer X consume this hour?" cannot be answered without grepping logs.
- Analysis results are written to the `interaction_metadata` JSONB column on the `interactions` row. This is the only copy. A retry write overwrites the previous result with no history.

### 2.5 Rate-limit / capacity protection (`src/services/circuit_breaker.py`)

**Role.** Protect the LLM provider from overload; signal the dialler to slow down when capacity is constrained.

- The entire module is dead code. `check_capacity()` and `_trip()` have **zero callers** in the repository. The dialler that was supposed to call them does not exist in `src/`. The "binary 30-minute freeze" failure mode described in the README is aspirational — actual current behaviour is no rate-limit protection at all.
- Even if wired up, the breaker tracks RPM (`requests/min`) while the LLM provider rate-limits on TPM (`tokens/min`). It would trip on the wrong gauge.
- Granularity is `agent_id`, but the rate limit is platform-wide. A single noisy customer would freeze every agent across every customer.
- Semantics are binary — at 89% capacity, full speed; at 90%, complete stop for 1,800 seconds. There is no middle gear.

### 2.6 Retry & failure handling (`src/services/retry_queue.py`)

**Role.** Catch task failures, schedule retries, alert on permanent loss.

- `dequeue_ready()` has **zero callers**. Failed interactions are pushed onto the retry list and never drained. The "retry mechanism" only enqueues.
- State keys have no TTL. Every exhausted interaction leaves a `postcall:retry_state:{id}` key in Redis indefinitely.
- The dequeue logic (which never runs) is non-atomic — `LPOP + check + RPUSH-back` is three separate Redis operations, so two pollers can pop the same entry and double-process.
- The retry queue lives in the same Redis as the Celery broker. One Redis bounce loses both the in-flight work and the retry record of that work.
- After max retries are exhausted, the payload is logged once and dropped. There is no DLQ; replay requires finding the original payload in error logs.

### 2.7 Downstream side-effects (`src/services/signal_jobs.py`, lead-stage updates)

**Role.** After analysis, fire WhatsApp confirmations, CRM pushes, lead-stage transitions, callback bookings.

- Fire-and-forget execution. Exceptions are caught and logged at WARNING — a CRM push failure produces a log line and nothing else. The downstream action is permanently lost.
- For long calls these are invoked twice: once from the endpoint with `analysis_result={}` and `call_stage="processing"`, once from Celery with the real values. Whether downstreams handle this idempotently is undocumented and outside the platform's control.

### 2.8 Metrics & observability (`src/services/metrics.py`)

**Role.** Track per-interaction timing and per-call token usage so ops can answer questions about backlog, spend, and tail latency.

- `tokens_used` is the exact value from the LLM provider on every call, logged but never aggregated. There is no per-customer Redis counter, no real-time TPM gauge.
- Per-interaction start time has a 1-hour TTL on its Redis key — shorter than realistic backlog drain time. Long-running interactions lose their start-time anchor and their wall-time metric is wrong.
- There is no interaction-level audit trail. "Reconstruct what happened to interaction X three days ago" is an exercise in `grep -r interaction_id /var/log`.

### 2.9 Data model (`src/models/interaction.py`)

**Role.** Persist the durable state of every interaction.

- Single `interactions` table; the `interaction_metadata` JSONB column is the only place analysis results live, and retries overwrite it.
- The schema has no concept of lane, recording status, trace ID, classifier verdict, outbox, DLQ, event log, or per-customer config. None of the patches in §3 can be implemented without schema support.

### 2.10 Cross-cutting themes

Three problems recur across modules and are worth naming separately:

1. **The durability boundary is in the wrong place.** Today, Redis stores in-flight work, retry state, rate counters, and short-call side-effect tasks. Postgres holds only the interaction row. A single Redis bounce loses everything in flight. The fix is to move durable state to Postgres and treat Redis as a coordination cache only.

2. **Dead and unwired code creates the illusion of safety.** The circuit breaker, the retry queue's dequeue side, and the metrics aggregation comments all suggest protective mechanisms exist that in fact do nothing. The first job is honest: delete the dead code, then rebuild what was supposed to be there.

3. **Side effects are invoked before they have inputs.** The endpoint fires `signal_jobs(analysis_result={})` before any analysis has run. This is a correctness bug; the fix has to treat side effects as a consequence of analysis completing, not as a parallel concern.

---

## 3. Architecture Overview

### 3.1 Framing: patch path, not rewrite

The total business logic in `src/` is ~1,140 lines across six files. After audit, the patch path requires:

- Deleting two modules entirely (`circuit_breaker.py`, `retry_queue.py`) — 315 LOC removed.
- Modifying ~150 LOC across six existing files.
- Adding five small new modules (TPM ledger, triage classifier, pressure gauge, outbox dispatcher, event-log writer) totalling ~700 LOC.
- One additive schema migration (8 new columns on `interactions`, 6 new tables; no destructive changes, no backfill).

Total churn is comparable to a rewrite, but the **external contract is unchanged** (Exotel webhook URL, request/response shape, dashboard read path), the **infrastructure is unchanged** (FastAPI, Celery, Redis, Postgres, S3 — no new components), and the **commit sequence is staged** (each module's patch is a focused, reviewable PR). The proposal is presented as an evolution of the existing architecture rather than a replacement.

### 3.2 Post-patch end-to-end flow

```
   Exotel webhook
        │
        ▼
   API endpoint
   (assigns trace_id, writes interaction_event[ENDED], enqueues Celery task)
        │
        ▼
   Triage task ─► [skip] terminal, no LLM ──────────────────────────────►
        │
        ├─ [hot]  enqueue → hot-lane queue
        └─ [cold] enqueue → cold-lane queue
        │
        ▼
   ┌───────────────────────┐                ┌─────────────────────────────┐
   │ Recording poller      │  (parallel)    │ Analyse task (lane-aware)    │
   │ exp-backoff polling   │                │ ├─ acquire(customer, est_tk) │
   │ writes recording_status│               │ │   from TPM ledger          │
   │ alerts on terminal    │                │ ├─ if denied: defer + reschedule
   │ failure               │                │ ├─ LLM call                  │
   └───────────────────────┘                │ ├─ release(actual_tokens)    │
                                            │ ├─ idempotent result write   │
                                            │ └─ outbox row insert         │
                                            └─────────────────────────────┘
                                                          │
                                                          ▼
                                               Outbox dispatcher worker
                                               ├─ retries with backoff
                                               └─ terminal failure → DLQ table + alert

   Pressure gauge (continuous, 0–1)  ── computed from TPM utilisation + queue depth
                                     ── published to Redis; (external) dialler reads
```

### 3.3 Key design decisions

1. **Postgres becomes the durability boundary; Redis is a coordination cache.** Every state transition writes to Postgres (the existing `interactions` row, plus new `interaction_events`, `signal_outbox`, `dead_letter_queue` tables). Redis holds only ephemeral data: the TPM ledger sliding window, scheduler reservations, and the pressure gauge. Redis loss causes a brief blip; no analysis, side effect, or audit row is lost.

2. **Triage runs before the LLM, not as part of it.** A cheap rule-based classifier (keyword + regex + minimum-turn-count + presence-of-entities) routes every long call into `skip`/`hot`/`cold` lanes. Skips never spend a token. Hot calls draw from a small reserved capacity pool with strict latency targets. Cold calls share burst capacity and may defer.

3. **LLM calls are gated proactively, not reactively.** A token reservation is acquired *before* the request goes out, against both the platform-wide TPM ceiling and the customer's per-customer budget. If denied, the task defers and reschedules at a deadline derived from the ledger window. The system never surfaces 429s upstream.

4. **Per-customer budgets are subdivisions of the shared platform TPM, governed by one algorithm.** Customers contract for `reserved_tpm` (guaranteed); total reserved is capped at 70% of platform TPM. The remaining 30% is a burst pool, allocated by weighted fair queueing across active customers. **Unreserved customers are modelled as `reserved_tpm = 0` with a default `burst_weight`** — they participate in the same WFQ as any reserved customer's burst overflow. The system has one budgeting algorithm, not two. A `MIN_FAIR_SHARE` floor and an optional per-customer `max_tpm` cap prevent both starvation and monopolisation. Concrete formulas and worked examples in §5.

5. **Recording fetch is decoupled from analysis.** A separate Celery task polls Exotel with bounded backoff (`[5, 10, 30, 60, 120, 240]` seconds, ~7.5-minute total ceiling). Three terminal states (`uploaded`, `unavailable`, `fetch_error`) are persisted as a `recording_status` enum on the row. Terminal-bad states emit alertable structured log events.

6. **Side effects fire from an outbox, not inline.** When analysis completes, the result write and outbox-row inserts happen in the same Postgres transaction. A separate `outbox_dispatcher` worker drains the outbox with retries; terminal failures move to the DLQ table where they are replayable. Endpoint-level fire-and-forget is removed entirely.

7. **The binary circuit breaker is replaced with a continuous pressure gauge.** The dialler (external) reads a 0–1 float from Redis. There is no trip state, no freeze. The dialler applies its own response curve. The signal is computed from real constraints (TPM utilisation, queue depth), not the wrong proxy (RPM).

8. **Trace IDs thread through every module.** Generated at the endpoint, included in the Celery payload, recorded in every `interaction_events` row, written into every structured log line via the `extra` dict. One query reconstructs an interaction's full journey three days later.

### 3.4 What stays unchanged

- The Exotel webhook URL and request/response contract.
- The dashboard's read path against `interaction_metadata`.
- The infrastructure: FastAPI, Celery, Redis, Postgres, S3 — no new dependencies.
- The basic five-step business logic (recording, analysis, metadata write, signal jobs, lead stage). What changes is *how they are orchestrated*, not *what happens*.

### 3.5 Patch order (commit sequence)

| # | Module | Justification |
|---|--------|---------------|
| 1 | Schema additions (Module 9) | Every other patch depends on the new tables/columns existing |
| 2 | Delete circuit breaker + add pressure gauge (Module 5) | Smallest, fully independent, removes dead code |
| 3 | Delete retry_queue + add DLQ table (Module 6) | Independent; closes the silent-drop bug |
| 4 | TPM ledger + triage classifier (Module 4) | Central piece — AC1, AC2, AC8 all depend on this |
| 5 | Recording poller (Module 3) | Independent of LLM path; satisfies AC4 |
| 6 | Lane routing + worker model (Module 2) | Depends on Module 4's lane assignment being decided |
| 7 | Outbox + dispatcher (Module 7) | Depends on Module 9's outbox table existing |
| 8 | Endpoint cleanup (Module 1) | Depends on Module 7 — only safe to delete dual-fire once outbox path works |
| 9 | Observability rollup (Module 8) | Cross-cutting; finalised once all events exist |

This ordering doubles as the **commit sequence** — each step is a focused, reviewable change with a clear blast radius. The README explicitly cites clean commit history as part of the submission.

---

## 4. Rate Limit Management

This section specifies the TPM ledger — the central mechanism by which every LLM call is gated proactively against per-customer and platform-wide budgets.

### 4.1 Two-layer architecture

The TPM ledger has two layers serving distinct purposes:

| Layer | Purpose | Storage | Loss tolerance | Latency budget |
|-------|---------|---------|----------------|----------------|
| Enforcement counter | Pre-flight check — "may this LLM call fire now?" | Redis | Tolerable; rebuildable | < 5 ms per acquire |
| Durable ledger | Billing-grade attribution | Postgres `token_ledger` (§10) | Zero loss | Async; query latency irrelevant |

Conflating these is what produces fragile rate limiters. Redis serves enforcement at sub-millisecond latency but is not durable. Postgres is durable but cannot serve every LLM call's pre-flight check at the required latency. Both exist; they have different jobs.

Every successful LLM call writes a row to `token_ledger` (Postgres) AND increments a Redis counter. The Postgres write is async (after the call completes); the Redis check is sync (before the call fires). On Redis loss, the last 2 minutes of state is replayable from Postgres (§4.8).

### 4.2 Acquire / commit lifecycle

Every LLM call goes through three operations:

```
ACQUIRE(customer_id, interaction_id, est_tokens, lane)
   → granted (reservation_id) | denied (retry_after_ms, reason)
       ↓ if granted
LLM CALL
       ↓
COMMIT(reservation_id, actual_tokens)
   → writes Postgres ledger row, releases reservation
```

**Estimating tokens.** At acquire time we don't know completion length, so we reserve `est = prompt_tokens + max_completion_tokens`. Prompt tokens are exact (we tokenize the prompt ourselves); `max_completion` is a ceiling we set on the request. So `actual ≤ est` always. The over-reservation is refunded on commit.

**Why acquire and commit are separate.** The LLM call takes ~3.5s on average. During that window the tokens are *promised but not yet consumed*. If we counted tokens only after the response arrived, multiple concurrent acquires could each see "budget available" and all proceed — over-committing the budget by `concurrency × call_duration`. Reservation closes that gap.

### 4.3 Sliding window: two-bucket smoothed

A 60-second sliding window per customer (and per platform, for the global rate-limit check). Two buckets per customer — current and previous minute — combined as a weighted blend at read time:

```
elapsed_in_current = now_seconds % 60
weight_for_prev    = (60 − elapsed_in_current) / 60
window_used        ≈ weight_for_prev × prev_minute_total + current_minute_total
```

Redis keys:

```
tpm:cust:{customer_id}:{minute_epoch}     # committed-tokens bucket, TTL 120s
tpm:global:{minute_epoch}                  # platform-wide bucket, TTL 120s
```

Two GETs per acquire vs. one for a naive single-bucket scheme. The benefit is no minute-boundary cliff — a single-bucket implementation lets a customer effectively double their budget by timing requests across the boundary. This is the same scheme used by GitHub's API rate limiter, Cloudflare, and Stripe's published rate-limiter design.

The approximation assumes consumption was uniformly distributed within the previous minute. For high call volume dispersed across each minute, this holds. The exact alternative (per-call timestamps in a Redis sorted set) is ~10× slower at our throughput.

### 4.4 Pending counter

A separate key per customer:

```
pending:cust:{customer_id}                 # int — sum of est_tokens across active reservations
```

Goes up on acquire, down on commit or lease-expiry. Saturation at acquire time:

```
saturation = window_used + pending
grant if saturation + est ≤ effective_budget(customer_id)   # budget formula in §5
```

The pending counter is a single integer per customer — no minute buckets, no sliding window. It's a strict in-flight gauge that always reflects "what's currently reserved by living workers."

The pressure gauge (§4.9) reads `window_used + pending` to compute the platform-wide saturation signal.

### 4.5 Reservation leases and the sweeper

Each reservation is a Redis hash:

```
reservation:{reservation_id} = {
    customer_id:  ...
    tokens:       est_tokens
    lease_until:  now + 60s
    ...
}
```

To enable efficient sweep, reservations are also indexed in a sorted set by `lease_until`:

```
ZADD reservations_by_lease  lease_until  reservation_id
```

A Celery beat task running every 10 seconds sweeps expired reservations:

```
expired = ZRANGEBYSCORE reservations_by_lease 0 now
for rid in expired:
    res = HGETALL reservation:{rid}
    DECR pending:cust:{res.customer_id} BY res.tokens
    DEL reservation:{rid}
    ZREM reservations_by_lease rid
    log structured event "reservation_expired" with rid, customer_id, tokens
```

Lease duration is 60s. The constraint is `lease > LLM_REQUEST_HARD_TIMEOUT` (currently 30s); 60s is comfortably above any plausible legitimate call duration. Without the lease, a single worker crash would permanently consume budget until manual cleanup.

### 4.6 Commit and the late-commit path

```python
async def commit(reservation_id, actual_tokens):
    reservation = await redis.hgetall(f"reservation:{reservation_id}")

    if reservation:
        # Normal path — reservation still alive
        # Atomic Lua script:
        #   DECR pending:{customer} BY est_tokens
        #   INCR tpm:cust:{customer}:{current_minute} BY actual_tokens
        #   INCR tpm:global:{current_minute}        BY actual_tokens
        #   DEL  reservation:{rid}
        #   ZREM reservations_by_lease rid
        await redis.eval(LUA_COMMIT, ...)
    else:
        # Late path — sweeper already refunded pending
        await redis.incrby(f"tpm:cust:{customer}:{current_minute()}", actual_tokens)
        await redis.incrby(f"tpm:global:{current_minute()}", actual_tokens)
        logger.warning("commit_after_lease_expiry", extra={...})

    # Durable record (async, non-blocking the hot path)
    await postgres.execute(
        "INSERT INTO token_ledger (interaction_id, customer_id, campaign_id, "
        "tokens_used, model, occurred_at) VALUES (...)"
    )
```

This is correct in both paths. The known-acceptable gap (15s typical, 60s worst case) between sweeper-expire and late-commit can briefly under-count one stranded reservation; bounded and self-correcting on the next acquire's accounting.

### 4.7 Denial: defer and reschedule

When acquire denies, the response includes a `retry_after_ms` so the calling task knows when to reschedule:

```python
@dataclass
class AcquireResult:
    granted: bool
    reservation_id: str | None
    retry_after_ms: int | None     # set if denied
    reason: str | None             # 'global_full' | 'customer_full' | 'burst_starved'
```

`retry_after_ms` is computed conservatively as the time until the most-saturated relevant bucket ages out:

```
retry_after_ms = max(
    seconds_until_current_minute_ends × 1000,
    config.MIN_RETRY_BACKOFF_MS
)
```

The Celery analyse task on denial:

```python
result = await ledger.acquire(customer_id, interaction_id, est_tokens, lane)
if not result.granted:
    raise self.retry(
        countdown=result.retry_after_ms / 1000,
        max_retries=config.MAX_DEFER_ATTEMPTS,
    )
```

This is how AC1 ("never trigger unhandled 429 errors") is satisfied: the LLM is not called until acquire grants. Denied tasks defer and reschedule; the upstream provider never sees a request that would produce a 429.

`MAX_DEFER_ATTEMPTS` (default 50, ~50 minutes worst case) bounds the defer loop. If a task defers beyond that, it goes to the DLQ (§8) for human review — likely indicating the customer's budget is misconfigured or platform capacity has been undersold.

### 4.8 Redis-loss recovery

If Redis is wiped (failover, cluster restart, AOF corruption), enforcement counters are gone but committed tokens are still in Postgres. A small recovery worker on Redis reconnect:

```sql
SELECT customer_id,
       date_trunc('minute', occurred_at) AS bucket,
       SUM(tokens_used) AS total
FROM token_ledger
WHERE occurred_at > NOW() - INTERVAL '2 minutes'
GROUP BY customer_id, bucket;
```

Pump these values into `tpm:cust:{customer_id}:{minute_epoch}` keys. Pending counters are not replayed — in-flight reservations are also lost on Redis failure, and the affected workers will fail their commits and re-acquire on retry. Enforcement is consistent again within seconds.

Worst-case impact: brief window (tens of seconds) during which up to ~2 minutes of prior consumption may be under-enforced. Acceptable for the rare Redis-failure case.

### 4.9 The pressure gauge (replacement for the binary circuit breaker)

A continuous 0.0–1.0 float published to Redis at a documented key, recomputed every 5 seconds by a small worker:

```python
async def compute_platform_pressure() -> float:
    tpm_used   = read_global_window()                         # § 4.3 + § 4.4
    tpm_limit  = settings.LLM_TOKENS_PER_MINUTE
    # Cold-lane queue depth is the Celery broker queue length;
    # Celery uses Redis as its broker, so we read it directly.
    queue_dep  = await redis.llen("postcall_cold")

    pressure = max(
        tpm_used / tpm_limit,
        queue_dep / settings.TARGET_COLD_QUEUE_DEPTH,
    )
    return min(1.0, max(0.0, pressure))

# Publish:
await redis.set("platform_pressure", f"{pressure:.4f}", ex=15)
```

Properties:

- **Continuous, not binary.** No trip state, no freeze. Dialler reads a float and applies its own response curve.
- **Tied to actual constraints.** TPM utilisation (the real provider limit) and cold-lane queue depth (the real backlog signal). Not RPM, which the legacy implementation tracks but the provider does not enforce on.
- **Stale-safe.** 15s TTL. If the publisher dies, the key disappears within 15s and the dialler can detect the missing signal and apply a sensible default (full-stop or full-go is configurable on its side).
- **Cheap.** Two reads + one write every 5s. Zero impact on the hot acquire path.

### 4.10 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC1 (never surfaces 429s) | §4.7 — acquire gates pre-flight; denied tasks reschedule rather than calling the LLM |
| AC2 (per-customer budget) | §5 (the policy) using §4.4's enforcement |
| AC7 (no binary freeze) | §4.9 — continuous pressure gauge replaces the circuit breaker |
| AC3 (no task lost on restart) | §4.5 lease + sweeper + §4.8 recovery + §8 DLQ |

---

## 5. Per-Customer Token Budgeting

§4 specifies *how* budgets are enforced. This section specifies *what those budgets are* — how the platform's TPM is sliced across customers.

### 5.1 The unified algorithm

Reserved customers and unreserved customers run through the **same algorithm**. The only thing that distinguishes them is values in their `customer_config` row (schema in §10):

| Customer type | `reserved_tpm` | `burst_weight` | `max_tpm` |
|---------------|---------------|----------------|-----------|
| Reserved enterprise | 10,000 | 5 | 30,000 (optional) |
| Reserved standard | 2,000 | 2 | NULL |
| Trial / unreserved | 0 | 1 | 5,000 (recommended) |
| Internal / test | 0 | 0 | NULL — runs only when burst is fully idle |

A customer with `reserved_tpm = 0` is just a degenerate reserved customer at the zero end of the continuum. Same WFQ pool, same acquire path, same enforcement. The system has one budgeting algorithm, not two.

### 5.2 Effective budget formula

At acquire time, the ledger computes the customer's effective TPM ceiling at this moment:

```
effective_budget(customer) =
    reserved_tpm                                              # contracted, always granted
  + (burst_weight / Σ_active_burst_weights) × burst_pool      # WFQ share of overflow
  
  clamp upper to customer.max_tpm if set
  clamp lower to MIN_FAIR_SHARE if customer.burst_weight > 0
```

Components:

- **`reserved_tpm`** — guaranteed, regardless of platform load. Cannot be taken by other customers.
- **Burst share** — proportional to `burst_weight`, divided by the sum of *active* burst weights at this moment.
- **`burst_pool`** — 30% of platform TPM (e.g., 27,000 if platform is 90,000).
- **`Σ_active_burst_weights`** — sum of weights for customers who have demanded burst capacity within the last 30s. Tracked in Redis with TTL-based decay; updated on each acquire that demanded burst.
- **`MIN_FAIR_SHARE`** — `config.py` constant (default 50 TPM); prevents pathological starvation when many customers are active.
- **`max_tpm`** — optional per-customer hard ceiling preventing one customer from monopolising the pool when others are quiet.

### 5.3 Why active-only weighting

If `Σ_burst_weights` summed over *all* customers (not just active ones), an inactive customer would still claim its share — the burst pool would sit idle while one active customer was allocated only a sliver. By weighting over active customers only, idle customers contribute 0 weight and the pool is fully utilised; as more customers become active, each one's share shrinks gracefully.

This is the same property TCP fair queueing has: capacity flows to demand. Twilio's multi-tenant subaccount allocation uses the same approach.

Activeness is observable: each acquire that demanded burst sets `active_burst:{customer_id} = burst_weight` with a 30s TTL. The sum is maintained in `active_burst:total`, kept consistent by the same Lua script that does the read/write.

### 5.4 Worked example: normal load

Platform TPM = 90,000. Reserved pool = 63,000 (70%). Burst pool = 27,000 (30%).

State right now:

| Customer | `reserved_tpm` | `burst_weight` | Currently using | Burst demand |
|----------|---------------|----------------|-----------------|--------------|
| A | 10,000 | 5 | 8,000 of reservation | +4,000 |
| B | 5,000 | 2 | 5,000 of reservation | 0 |
| C | 0 | 1 | — | 2,000 |
| D | 0 | 1 | — | 500 |

Active burst participants: A (bursting above reservation), C, D. **Σ active weights = 5 + 1 + 1 = 7.**

Effective budgets:

- A: `10,000 + (5/7) × 27,000 = 10,000 + 19,285 = 29,285` TPM
- B: `5,000` (no burst demand) = 5,000 TPM
- C: `0 + (1/7) × 27,000 = 3,857` TPM
- D: `0 + (1/7) × 27,000 = 3,857` TPM

All four can be served. A's burst demand (4K) is well below their burst share (19K). C's 2K and D's 0.5K are below their 3.9K shares. B uses only their reservation. **27K of burst pool is fully allocated; nothing is wasted.**

### 5.5 Worked example: saturation

Same platform, but suppose 50 unreserved customers (`reserved_tpm = 0`, `burst_weight = 1` each) all wake up simultaneously, each demanding 1,000 TPM.

- **Σ active weights = 50.**
- Each customer's share: `(1/50) × 27,000 = 540 TPM`.
- Each customer is allowed 540 TPM but demands 1,000.

Each of the 50 customers gets its 540 TPM (about one LLM call every 3 seconds). The remaining demand queues in the cold-lane Postgres queue and reschedules via §4.7. Cold-lane p95 stretches because the platform is genuinely saturated for unreserved demand. Reserved contracts continue to be honoured at full capacity. The burst pool is fully utilised. Nothing 429s.

This is correct behaviour. We cannot serve 50K TPM of unreserved demand from a 27K burst pool. The platform's job is to honour reserved contracts and serve unreserved best-effort.

### 5.6 Admission control

The 70% reserved cap is enforced at contract sign-up, not at runtime:

```sql
-- Pseudocode for adding a new reserved contract
SELECT SUM(reserved_tpm) INTO current_committed FROM customer_config;

IF current_committed + new_reserved_tpm > 0.70 * platform_tpm THEN
    RAISE EXCEPTION 'capacity exhausted; refuse contract';
ELSE
    INSERT INTO customer_config (...);
END IF;
```

An alert fires at 60% saturation — a capacity-planning trigger to negotiate higher upstream provider TPM before the next contract is signed. This is the only piece of operational policy the runtime enforces by code; the rest (which customer is "enterprise" vs "standard") is purely data.

### 5.7 Floor and cap tunables

Two safety knobs:

- **`MIN_FAIR_SHARE = 50`** — minimum effective TPM granted to any active customer with `burst_weight > 0`. Prevents the "1,000 active unreserved customers each get 27 TPM" pathological case from making each share unusably small. When `MIN_FAIR_SHARE × N_active > burst_pool` (extreme case), the floor becomes best-effort and excess demand queues. Acceptable trade-off.
- **`MAX_UNRESERVED_TPM = 5,000`** (recommended default for `customer_config.max_tpm` on unreserved customers) — per-customer ceiling on burst share. Prevents one unreserved customer from temporarily monopolising the pool when others are quiet, then disappointing them when others wake up.

These are tunables, not architectural. They live in `config.py` defaults and per-customer rows in `customer_config`. Customer-level overrides take precedence.

### 5.8 Per-campaign overrides

A customer running a high-stakes campaign may want to direct their reserved budget at it. This is supported via the `campaign_config` lookup at acquire time (schema §10):

```
effective_reserved =
    customer.reserved_tpm × (campaign.reserved_share or 1.0)
```

A customer can split their reservation across concurrent campaigns: e.g., "campaign A gets 70% of my reserved capacity, campaign B gets 30%." Default share is 1.0 (single-campaign customer or fully shared). This is data, not code — no deployment required to change.

### 5.9 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC2 (Customer A's budget doesn't consume Customer B's allocation) | §5.2 — `reserved_tpm` is contractual; never available to other customers |
| AC7 (proportional backpressure, not binary) | §5.5 — saturation gracefully extends cold-lane latency rather than freezing the dialler |
| AC9 (assumptions explicit) | §5 makes the 70/30 split, the WFQ algorithm, and the unified-customer model explicit and testable |

---

## 6. Differentiated Processing

The platform's primary cost-saving lever is *not processing every call the same way*. A "wrong number" hangup and a confirmed rebook should follow radically different paths. This section specifies the triage mechanism that routes every long call into one of three lanes — `skip`, `hot`, `cold` — before any LLM call is considered.

### 6.1 Overview

```
endpoint  ─►  triage task (rule-based classifier, ~1ms)
                   │
                   ├─ skip:  synthesise result, no LLM, enqueue outbox
                   ├─ hot:   enqueue analyse on hot-lane queue (reserved capacity)
                   └─ cold:  enqueue analyse on cold-lane queue (burst share)
```

Triage is a rule-based classifier — keyword matching plus turn count. No ML, no LLM. Sub-millisecond per call. The classifier interface is pluggable so future versions (embedding-based, small-LLM fallback) can swap in without changing callers.

### 6.2 The classifier

```python
@dataclass
class TriageVerdict:
    lane: Literal["skip", "hot", "cold"]
    suggested_call_stage: str | None    # used directly on skip; hint to LLM on hot/cold
    matched_rules: list[str]            # for ops debugging and tuning
```

Keyword sets are platform-wide constants in `src/config.py`. Rule order matters:

```python
SKIP_TURN_RULE = lambda turn_count: turn_count < 4

SKIP_NEGATIVE_RULES = {
    "wrong_number":  ["wrong number"],
    "do_not_call":   ["stop calling", "don't call again", "do not call",
                      "remove my number", "take me off your list"],
}

HOT_RULES = {
    "rebook_confirmed":  ["confirmed", "booked your slot", "scheduled for",
                           "i've booked"],
    "demo_booked":       ["demo is booked", "calendar invite",
                           "looking forward to"],
    "escalation_needed": ["manager", "complaint", "unacceptable",
                           "file a complaint", "escalate", "senior executive"],
}

COLD_RULES = {
    "not_interested":     ["not interested"],
    "already_done":       ["already booked", "already done", "already purchased"],
    "callback_requested": ["call back later", "call me later", "call me back"],
}

def classify(text: str, turn_count: int) -> TriageVerdict:
    text = text.lower()

    # 1. Skip — short transcripts
    if SKIP_TURN_RULE(turn_count):
        return TriageVerdict("skip", "short_call", ["min_turns"])

    # 2. Skip — strong negative keywords (no entities for the LLM to extract)
    for stage, keywords in SKIP_NEGATIVE_RULES.items():
        if any(kw in text for kw in keywords):
            return TriageVerdict("skip", stage, [f"skip_negative:{stage}"])

    # 3. Hot before Cold (revenue protection on collisions)
    for stage, keywords in HOT_RULES.items():
        if any(kw in text for kw in keywords):
            return TriageVerdict("hot", stage, [f"hot:{stage}"])

    for stage, keywords in COLD_RULES.items():
        if any(kw in text for kw in keywords):
            return TriageVerdict("cold", stage, [f"cold:{stage}"])

    # 4. Ambiguous — default hot (see §6.5)
    return TriageVerdict("hot", None, [])
```

Three properties worth flagging:

1. **Order — Skip → Hot → Cold → default Hot.** Hot is checked before Cold so a transcript containing both "confirmed" and "not interested" routes hot — the hot signal in the same call suggests value worth investigating fully.
2. **Single language (per A10).** Only English keywords in v1. Hinglish or other-language transcripts fall through to default-hot, where the LLM handles language-aware classification.
3. **Ops tunability.** Keyword lists live in `config.py`. Updating them requires a deploy. Per-customer overrides are a known extension (§15.1).

### 6.3 The skip path — no LLM at all

When the verdict is `skip`, the triage task synthesises an analysis result directly and writes it to the same `interactions.interaction_metadata` column the LLM path writes to:

```python
synthetic_result = AnalysisResult(
    call_stage  = verdict.suggested_call_stage,        # "short_call" / "wrong_number" / "do_not_call"
    entities    = {},
    summary     = f"Auto-classified: {verdict.suggested_call_stage}",
    tokens_used = 0,
    provider    = "classifier",
    model       = "rules-v1",
)

await persist_analysis_result(interaction_id, synthetic_result)
await event_log.write(interaction_id, "ANALYZED",
                      source="classifier", matched_rules=verdict.matched_rules)
await outbox.insert_signal_jobs_row(interaction_id, synthetic_result)
await outbox.insert_lead_stage_row(lead_id, synthetic_result.call_stage)
await update_interaction_status(interaction_id, "analysis_skipped")
```

The downstream pipeline (outbox dispatcher, signal jobs, lead stage) does not know or care that a classifier produced the result instead of an LLM. Same Postgres rows, same outbox, same dispatch. This is the AC8 path — short transcripts AND clear-skip transcripts never consume LLM quota.

### 6.4 The hot and cold paths

Both lanes enqueue an `analyse_task` with the suggested call_stage as a hint:

```python
await analyse_task.apply_async(
    args=[interaction_id, verdict.suggested_call_stage],
    queue="postcall_hot" if verdict.lane == "hot" else "postcall_cold",
)
```

The hint is passed to the LLM prompt as context, not as an instruction:

```
Pre-classifier suggested this call's outcome may be: {suggested_call_stage}.
You may use this hint, but verify against the transcript and override if wrong.
```

The LLM still does full analysis (entities, summary, final call_stage). Discrepancies between the classifier's suggestion and the LLM's verdict are logged as a tuning signal — over time, frequent disagreements highlight keyword lists that need refinement.

The lanes differ in §4's TPM-ledger acquire behaviour:

- **Hot lane** acquires from the customer's `reserved_tpm` first. Only if reservation is exhausted does it consume burst share.
- **Cold lane** can only acquire from burst share. When burst is contested, cold tasks defer and reschedule via §4.7 — this is what stretches cold-lane latency under load while preserving the hot-lane SLA.

### 6.5 Default-hot on ambiguity

When no rule matches, the verdict is `(hot, None, [])`. Rationale (also stated in A1): the cost of misclassifying a hot call as cold is sales-loss-shaped — a customer waits longer for their WhatsApp confirmation, the agent's next step depends on the result. The cost of misclassifying a cold call as hot is LLM tokens spent against the hot lane's reserved capacity, capped by §5's `max_tpm`. Asymmetric cost, asymmetric default.

If default-hot causes the hot lane to consistently saturate, that surfaces in §4.9's `platform_pressure` gauge and the dialler responds. A tunable problem (extend keyword lists, downgrade ambiguous to cold), not a correctness one.

### 6.6 Where it runs

Triage is a Celery task on a dedicated `triage` queue. The webhook endpoint (§12) enqueues it and returns 200 immediately. Triage workers are independently scalable from analyse workers — keyword matching is CPU-bound and trivially parallel; analyse is I/O-bound and rate-limit-bound.

```
endpoint  ─►  triage queue  ─►  triage workers   (CPU-bound, ~1ms per call)
                                     │
                                     ├─►  skip path: persist synthetic result, outbox
                                     │
                                     ├─►  postcall_hot  ─►  analyse workers (I/O-bound)
                                     │                       acquire from reserved capacity
                                     │
                                     └─►  postcall_cold ─►  analyse workers (I/O-bound)
                                                            acquire from burst share
```

Triage queue depth is itself a useful operational signal — if it grows, more campaigns are starting than triage workers can handle. Triage workers are cheap to scale (Python processes, no GPU, no rate limit), so this is rarely the bottleneck.

### 6.7 Cost projection at 100K calls

Conservative estimate for a typical campaign mix:

| Lane | Share of calls | LLM tokens spent |
|------|---------------|------------------|
| Skip — turn count < 4 | ~10% | 0 |
| Skip — wrong_number / do_not_call | ~10% | 0 |
| Hot (matched + ambiguous default-hot) | ~30% | 30K × 1500 = 45M |
| Cold | ~50% | 50K × 1500 = 75M |
| **Total** | **100K** | **120M** |

vs current system (every long call gets full analysis): **150M tokens.** Net savings ~20% per campaign, mostly from negative-keyword skips. Cold-lane deferrals via §4 compound this further when burst capacity is contested.

### 6.8 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC8 (short transcripts skip LLM) | §6.3 — turn count < 4 produces synthetic result without LLM |
| AC1 (never surfaces 429s) | §6.4 — both lanes acquire via §4 before any LLM call |
| AC2 (per-customer budget enforced) | §6.4 — hot uses reserved, cold uses burst, both via §5's effective-budget formula |
| AC7 (proportional backpressure) | §6.5 — default-hot interacts with §4.9 pressure gauge for graceful degradation |

---

## 7. Recording Pipeline

The recording pipeline replaces the unconditional `asyncio.sleep(45s)` blocking call with a bounded polling loop running in its own Celery task, parallel to triage and analysis.

### 7.1 Decoupling from analysis

The transcript is in `interactions.conversation_data` **before the webhook fires** — it is written by the voicebot during the call, not delivered by Exotel at end-of-call. Exotel is the audio plumber; they do not transcribe. The LLM analysis reads transcript text from the DB. **Analysis has no dependency on the audio recording.** They are two parallel data streams that never needed to be sequential.

```
endpoint  ─┬─►  recording task  ──►  poll Exotel, upload to S3   (own queue, own workers)
           │
           └─►  triage task   ──►  analyse task  ──►  signal_jobs / lead_stage
```

Removing the sequential coupling improves end-to-end analysis latency by ~45 seconds per call (the unconditional sleep), while leaving recording to take whatever time Exotel needs in a non-blocking lane. The recording's terminal state has no impact on the analysis path — the dashboard shows the analysis result regardless of whether the recording is still pending, uploaded, or failed.

### 7.2 Poll schedule

Bounded schedule with jitter:

```python
POLL_SCHEDULE_SECONDS = [5, 10, 30, 60, 120, 240]   # 6 attempts, ~7.5 min total
JITTER_FRACTION = 0.20                              # ±20% on each interval
```

| Attempt | Wait before | Cumulative | Catches |
|--------:|------------:|-----------:|---------|
| 1 | 5s | 5s | Already-ready (rare) |
| 2 | 10s | 15s | Fastest realistic case |
| 3 | 30s | 45s | Common case (p50–p75) |
| 4 | 60s | 105s | Slow-but-normal (p95) |
| 5 | 120s | 225s | Long tail (p99) |
| 6 | 240s | 465s (~7.5 min) | Last chance before declaring lost |

Each interval is multiplied by `random.uniform(1 - JITTER_FRACTION, 1 + JITTER_FRACTION)` to prevent thundering-herd retries when many calls end simultaneously.

The schedule is shaped to match Exotel's actual delivery distribution: most recordings ready in 15–30s, long tail to ~3 min, beyond ~7 min realistically lost. Front-loaded with two cheap polls (catch the rare fast case), then escalating more slowly than pure geometric to avoid spending the entire budget on small intervals.

### 7.3 Terminal states

A `recording_status` enum on the `interactions` row:

| State | Meaning | Alertable |
|-------|---------|-----------|
| `pending` | Recording task scheduled, not yet terminal (initial value) | No |
| `uploading` | Fetched URL, currently streaming to S3 | No |
| `uploaded` | S3 upload completed; `recording_s3_key` populated | No |
| `unavailable` | All polls returned 404; recording never produced by Exotel | Yes (rate-based) |
| `fetch_error` | Persistent HTTP error or network failure across all polls | Yes (rate-based) |

Two distinct failure states because they have different operational meanings:

- **`unavailable`** — Exotel cleanly told us no recording exists. Often legitimate (call disconnected before recording started, sub-second connections).
- **`fetch_error`** — We could not reach Exotel or their API misbehaved. Operational issue, not a data issue.

A 5% `unavailable` rate is normal; a 5% `fetch_error` rate is an Exotel outage that needs to escalate.

### 7.4 Error classification per poll

Each poll attempt classifies the result:

| HTTP / network outcome | Classified as | Action |
|------------------------|---------------|--------|
| 200 + recording URL | `ready` | Move to upload step |
| 404 | `not_ready` | Wait for next poll interval |
| 5xx | `transient_error` | Wait for next poll interval |
| Network timeout / connection refused | `transient_error` | Wait for next poll interval |
| 401 / 403 | `permanent_error` | Fail immediately, alert (config issue) |
| 4xx other than 404, 401, 403 | `permanent_error` | Fail immediately, alert |

After the schedule is exhausted:
- Last poll was `not_ready` → terminal state `unavailable`
- Last poll was `transient_error` → terminal state `fetch_error`
- A `permanent_error` at any point ends the loop early with `fetch_error`

### 7.5 The poller

The recording task is a Celery task on a dedicated `recording` queue, enqueued by the webhook endpoint in parallel with triage. The poller uses Celery's `countdown=` parameter to schedule the next poll, **not** in-process `time.sleep()`. This frees the worker between polls — at 100K calls × ~6 polls × ~30s avg interval, in-process sleeping would tie up enormous worker capacity for nothing.

```python
@celery.task(bind=True, max_retries=6, queue="recording")
def poll_recording(self, interaction_id, attempt=1):
    result = fetch_exotel_recording(interaction_id)

    if result.status == "ready":
        upload_to_s3_and_finalize(interaction_id, result.url)
        return

    if result.status == "permanent_error":
        finalize_terminal(interaction_id, status="fetch_error",
                          reason=result.error_detail)
        alert("recording_permanent_error", interaction_id)
        return

    # not_ready or transient_error — retry per schedule
    if attempt >= len(POLL_SCHEDULE_SECONDS):
        terminal = ("unavailable" if result.status == "not_ready"
                    else "fetch_error")
        finalize_terminal(interaction_id, status=terminal)
        return

    delay = jittered(POLL_SCHEDULE_SECONDS[attempt])
    raise self.retry(countdown=delay, kwargs={"attempt": attempt + 1})
```

### 7.6 Reconciliation for partial failures

The S3-uploaded-but-DB-write-failed case is handled with a **state-before-action** pattern:

```python
async def upload_to_s3_and_finalize(interaction_id, recording_url):
    # Mark as in-progress BEFORE the upload
    await update_status(interaction_id, "uploading")

    try:
        s3_key = await stream_upload(recording_url, interaction_id)
    except Exception:
        await update_status(interaction_id, "fetch_error")
        raise

    # Atomic write of s3_key + status
    await update_recording(interaction_id, s3_key=s3_key, status="uploaded")
```

If the process crashes between `stream_upload` returning and the final DB write, the row stays in `uploading` indefinitely. A reconciliation Celery beat task runs every 5 minutes:

```sql
SELECT id FROM interactions
WHERE recording_status = 'uploading'
  AND updated_at < NOW() - INTERVAL '2 minutes'
LIMIT 100;
```

For each match, the job HEAD-checks the expected S3 key:
- If present → set `status = 'uploaded'` with the s3_key (recovers the lost write)
- If absent → set `status = 'fetch_error'`, re-enqueue the poll task (recovers from a partial upload)

The reconciliation surfaces real problems instead of leaving silent orphans.

### 7.7 Alert conditions

Single-call failures are not pageable. Patterns are.

| Condition | Alert level | Action |
|-----------|-------------|--------|
| Single `unavailable` or `fetch_error` | Structured log only | None |
| `unavailable` rate > 10% in a 5-min window | WARN | Investigate Exotel-side issue |
| `fetch_error` rate > 2% in a 5-min window | PAGE | Likely Exotel outage or bad API key |
| Any `auth_failure` (401 / 403) | PAGE | Config issue; immediate |
| `recording` queue depth > 10,000 | WARN | Workers under-provisioned |
| Reconciliation finds > 50 stuck rows | WARN | Upload pipeline instability |

All thresholds are config tunables.

### 7.8 Schema additions (detail in §10)

```sql
ALTER TABLE interactions
    ADD COLUMN recording_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    ADD COLUMN recording_attempt_count INT NOT NULL DEFAULT 0,
    ADD COLUMN recording_last_attempt_at TIMESTAMPTZ,
    ADD COLUMN recording_terminal_at TIMESTAMPTZ;

CREATE INDEX ix_interactions_recording_pending
    ON interactions (recording_status)
    WHERE recording_status IN ('pending', 'uploading');
```

The partial index keeps the index small: terminal states dominate the table over time, but the reconciliation job and ops queries only filter on non-terminal states.

### 7.9 Cost comparison

| Metric | Today | After |
|--------|-------|-------|
| Wall time per call (analysis-blocking) | 45s + 3.5s ≈ **48s** | **~5s** (recording in parallel) |
| Recording API calls per 100K-call campaign | 100K (one-shot) | ~250K (avg 2.5 polls per call) |
| Recordings lost when ready after 45s | unmeasured but non-zero | 0 within 7.5-min budget; alertable beyond |
| Worker time spent sleeping | 100K × 45s = 4.5M worker-seconds | 0 (Celery countdown frees workers between polls) |

Yes, total Exotel API calls increase ~2.5×. Per Exotel's documentation and the existing codebase comments, the recording status endpoint is not rate-limited; the cost is acceptable.

### 7.10 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC4 (recording poller retries with backoff; never silently skips) | §7.2 schedule + §7.3 terminal states + §7.7 alerts |
| AC6 (every error path emits structured log with interaction_id) | §7.4 classification + §7.7 alert events all emit structured logs keyed on interaction_id |
| AC3 (no task lost on restart) | §7.6 reconciliation closes the S3-uploaded-but-DB-failed gap; Celery `acks_late` covers the worker-crash gap |
| Constraint 4 (recording failures produce observable events) | §7.7 — every terminal failure is alertable and tracked in `recording_status` |

---

## 8. Reliability & Durability

This section consolidates the durability story implied by §4 (TPM ledger), §6 (triage), and §7 (recording), and specifies the two patterns that close the remaining gaps: the **outbox** for downstream side effects and the **dead-letter queue** for terminal failures.

### 8.1 The durability invariant

> **No analysis result, side effect, or audit row is ever lost. Every interaction terminates in a state that is either persisted in Postgres or visible in the DLQ.**

Postgres is the system of record. Redis is a coordination cache. Every state transition that matters writes a row to Postgres before acknowledging success. A complete Redis loss causes a brief blip but no permanent data loss; Postgres loss is a real outage with RPO bounded by backup frequency.

The current system violates this invariant in five places, which the patches in §3.5 address:

| Current gap | Patch |
|---|---|
| `signal_jobs` and `lead_stage` fire-and-forget; failures swallowed | Outbox pattern (§8.2–§8.5) |
| Retry queue's dequeue side never runs; failed tasks accumulate forever | Replace with Celery's native retry + DLQ (§8.6) |
| Two retry mechanisms (Celery `self.retry` + `enqueue_retry`) compete | Drop the bespoke queue; Celery only |
| In-flight `asyncio.create_task` calls lost on FastAPI restart | Endpoint-level side effects deleted entirely (§12) |
| Reservations stranded by worker crashes | Lease + sweeper (§4.5) |

### 8.2 The outbox pattern

The current code fires WhatsApp / CRM / lead-stage updates inline with analysis. If `signal_jobs` raises, a warning is logged and the task moves on — the downstream action is permanently lost. If it succeeds and `lead_stage` then crashes, the customer received a message but the lead row is inconsistent.

The fix: side effects are not fired by the analysis task. The analysis task **inserts rows into `signal_outbox` in the same transaction** as the analysis result write. A separate dispatcher reads the outbox and fires the downstream calls.

```
┌──────────────────────────────────────────────────────────────┐
│  Analyse task                                                  │
│                                                                │
│  ┌─ BEGIN TRANSACTION ────────────────────────────────────┐   │
│  │  UPDATE interactions SET interaction_metadata = ...    │   │
│  │  INSERT INTO interaction_events  (ANALYZED, ...)       │   │
│  │  INSERT INTO signal_outbox  (signal_jobs, ...)         │   │
│  │  INSERT INTO signal_outbox  (lead_stage, ...)          │   │
│  └─ COMMIT ───────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│  Outbox beat (every 1s)                                       │
│  → fetch batch with FOR UPDATE SKIP LOCKED                    │
│  → enqueue dispatch_one per row on `outbox` queue             │
│                                                                │
│  Outbox workers                                                │
│  → mark in_progress                                            │
│  → invoke dispatch handler (WhatsApp / CRM / lead_stage)      │
│  → on success: status='dispatched'                             │
│  → on retryable: schedule retry per backoff                   │
│  → on max retries: move to dead_letter_queue                  │
└──────────────────────────────────────────────────────────────┘
```

Either both the analysis result and the outbox rows commit, or neither does. There is no observable middle state.

### 8.3 Outbox schema

```sql
CREATE TABLE signal_outbox (
    id              BIGSERIAL PRIMARY KEY,
    interaction_id  UUID NOT NULL,
    customer_id     UUID NOT NULL,
    trace_id        UUID NOT NULL,

    dispatch_type   VARCHAR(32) NOT NULL,
    payload         JSONB NOT NULL,
    idempotency_key UUID NOT NULL DEFAULT gen_random_uuid(),

    status          VARCHAR(16) NOT NULL DEFAULT 'pending',
    attempt_count   INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error      TEXT,
    in_progress_at  TIMESTAMPTZ,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at   TIMESTAMPTZ
);

CREATE INDEX ix_outbox_dispatch_ready
    ON signal_outbox (next_attempt_at, id)
    WHERE status = 'pending';
```

Statuses: `pending`, `in_progress`, `dispatched`, `failed`. The partial index keeps the working set small — `dispatched` rows dominate over time but only `pending` rows are queried.

`dispatch_type` values in v1: `signal_jobs`, `lead_stage`, `crm_push`. Adding new dispatch types is data-only — register a handler in `DISPATCH_HANDLERS` and start writing rows.

### 8.4 Dispatcher mechanics

Two-stage: a beat task that pulls a batch and enqueues per-row tasks; a pool of worker processes that execute the dispatches.

```python
@celery.task(name="outbox_beat")
def outbox_beat():
    rows = pg.fetch("""
        SELECT id FROM signal_outbox
        WHERE status = 'pending' AND next_attempt_at <= NOW()
        ORDER BY next_attempt_at LIMIT 200
        FOR UPDATE SKIP LOCKED
    """)
    for row in rows:
        dispatch_one.apply_async(args=[row.id], queue="outbox")

@celery.task(bind=True, queue="outbox")
def dispatch_one(self, row_id):
    row = pg.fetch_outbox(row_id)
    if row.status != 'pending':
        return                          # someone else got it

    pg.update_outbox(row_id, status='in_progress', in_progress_at=now())

    try:
        DISPATCH_HANDLERS[row.dispatch_type](
            payload=row.payload,
            idempotency_key=row.idempotency_key,
        )
    except RetryableError as e:
        next_attempt = row.attempt_count + 1
        if next_attempt >= MAX_OUTBOX_RETRIES:
            move_to_dlq(row, source='outbox',
                        reason='max_retries_exhausted', error=str(e))
        else:
            pg.update_outbox(row_id,
                status='pending',
                attempt_count=next_attempt,
                next_attempt_at=now() + OUTBOX_RETRY_SCHEDULE[next_attempt],
                last_error=str(e))
    except PermanentError as e:
        move_to_dlq(row, source='outbox',
                    reason='permanent_error', error=str(e))
    else:
        pg.update_outbox(row_id, status='dispatched', dispatched_at=now())
```

`FOR UPDATE SKIP LOCKED` lets multiple beat workers safely fetch non-overlapping batches. Tick frequency: **1 second** — this bounds dispatch latency at ~500ms average from outbox insert to handler invocation, eating < 5% of the hot-lane 20s SLA budget.

**Retry schedule** (exponential):

| Attempt | Wait before next attempt |
|--------:|--------------------------|
| 1 (initial) | — |
| 2 | 30s |
| 3 | 2 min |
| 4 | 10 min |
| 5 | 1 hr |
| > 5 | → DLQ |

### 8.5 Idempotency

The dispatcher must be idempotent. Two-pronged:

**1. State-before-action in the DB.** The dispatcher writes `status='in_progress'` *before* invoking the downstream service. If it crashes between the downstream call and the final `dispatched` write, the row stays `in_progress`. A reconciliation beat task scans every 5 minutes for rows `in_progress` for > 2 minutes and pushes them back to `pending`. Same pattern as §7.6 recording reconciliation.

**2. Idempotency key on downstream calls.** Every outbox row carries an `idempotency_key UUID`. We pass it to downstream services in the appropriate header (`Idempotency-Key` for Stripe-style services, `messageId` for some CRMs, etc.). A duplicate dispatch produces a no-op on the downstream — the side effect lands exactly once observable, even when our side retries.

Combined: even with worker crashes, beat-tick races, and dispatcher restarts, side effects fire **exactly once observable on the downstream**.

### 8.6 The dead-letter queue

When retries cannot help any further, the row moves to `dead_letter_queue`. This is the **terminal store for items needing human intervention**. It is alertable and replayable.

DLQ sources:

| Source | When |
|--------|------|
| `outbox` | Dispatch retries exhausted |
| `analysis` | Non-retryable exception OR `MAX_DEFER_ATTEMPTS` (§4.7) exceeded |
| `triage` | Crash with unhandlable exception (rare) |

What does **not** go to the DLQ: recording failures (§7.3 captures them inline as `recording_status`), short-circuit skips (§6.3 — they are successful terminal states, not failures).

### 8.7 DLQ schema

```sql
CREATE TABLE dead_letter_queue (
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
    review_notes     TEXT
);

CREATE INDEX ix_dlq_pending ON dead_letter_queue (created_at)
    WHERE status = 'pending_review';
CREATE INDEX ix_dlq_interaction ON dead_letter_queue (interaction_id);
```

`status` values: `pending_review`, `replayed`, `ignored`, `escalated`. `reviewed_by` and `review_notes` close the loop on every entry.

### 8.8 Replay

The point of the DLQ is that replay is a single function call:

```python
def replay(dlq_id, replayed_by):
    entry = pg.fetch_dlq(dlq_id)
    if entry.status != 'pending_review':
        raise AlreadyHandled()

    if entry.source == 'outbox':
        pg.insert_outbox(
            interaction_id=entry.interaction_id,
            dispatch_type=entry.original_payload['dispatch_type'],
            payload=entry.original_payload['payload'],
        )
    elif entry.source == 'analysis':
        analyse_task.apply_async(args=[entry.original_payload],
                                 queue='postcall_hot')
    elif entry.source == 'triage':
        triage_task.apply_async(args=[entry.original_payload],
                                queue='triage')

    pg.update_dlq(dlq_id, status='replayed',
                  reviewed_by=replayed_by, reviewed_at=now())
```

Exposed as an admin script in v1; admin endpoint is future work (§15). Customer-facing replay is explicitly out of scope.

### 8.9 Retention

| DLQ status | Retention |
|---|---|
| `pending_review` | Indefinite (until a human acts) |
| `replayed` / `ignored` / `escalated` | **30 days** then archived |

30 days is sufficient for incident reviews to look at last month's failures.

### 8.10 Alerts

| Condition | Alert level |
|---|---|
| Single DLQ insert | INFO log; included in daily ops digest |
| DLQ inserts > 0.1% of recent traffic in 5 min | WARN |
| DLQ inserts > 1% in 5 min | PAGE |
| DLQ depth > 1000 `pending_review` | PAGE — backlog growing faster than ops can handle |

### 8.11 Worker-crash recovery, end-to-end

What happens when an analyse worker crashes at each possible stage:

| Stage of crash | Recovery |
|---|---|
| After `acquire`, before LLM call returns | Reservation lease (§4.5) expires within 60s; sweeper refunds pending tokens. Celery `acks_late` redelivers the message; new worker re-acquires from the ledger. |
| After LLM returns, before `commit` | Late-commit path (§4.6) tolerates missing reservation; result is still correctly recorded. Idempotent result write prevents double-counting on redelivery. |
| After analysis logic, before transaction commit | Transaction rolls back. Celery `acks_late` redelivers. Idempotent `interaction_metadata` write (`WHERE analyzed_at IS NULL`) prevents double-write. |
| After commit, before outbox dispatcher picks up rows | Outbox rows are already durable. Dispatcher picks them up regardless of which worker committed. |
| Mid-dispatch in outbox | Row stays `in_progress`; reconciliation beat resets to `pending` after 2 min; retried per schedule. |

No path through this table loses the analysis result or the side effect.

### 8.12 Redis-loss recovery

Cross-references §4.8. In summary: enforcement counters are rebuilt from `token_ledger` over the last 2 minutes; pending counters are not replayed (in-flight reservations are also lost, and affected workers fail their commits and re-acquire on retry). Brief window of stale enforcement; data correctness is bounded because Postgres holds the source of truth.

### 8.13 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC3 (no task lost on Redis or Celery worker restart) | §8.11 + §8.12 + §8.6 (DLQ as terminal store) |
| AC5 (every interaction has a complete audit trail) | Cross-cutting — every state transition writes an `interaction_events` row (§9) |
| Constraint 1 (no analysis result permanently lost) | A7 + §8.6 — every interaction terminates in `analyzed` / `analysis_skipped` / `dead_lettered_for_review` |

---

## 9. Auditability & Observability

This section ties together the cross-cutting observability story: trace IDs, the `interaction_events` audit table, structured logging discipline, alert thresholds (consolidated from §4, §7, §8), and operational dashboards. The acceptance test is concrete: an on-call engineer can debug a specific failed interaction three days later from SQL alone.

### 9.1 Trace ID propagation

Two distinct identifiers thread through the pipeline:

| ID | Lifetime | Generated at | Purpose |
|---|---|---|---|
| `interaction_id` | The conversation, forever | Pre-call (when the call is initiated) | Identifies the call across replays, retries, dashboards |
| `trace_id` | One processing run | Webhook endpoint, on each `/end` invocation | Distinguishes attempts when an interaction is replayed |

Why both: a DLQ-replayed interaction keeps the same `interaction_id` but gets a fresh `trace_id`. This lets us query "all events for interaction X" (by `interaction_id`) and "all events for the third attempt at processing X" (by `trace_id`) independently.

`trace_id` propagation rules:

- Generated at [endpoints.py](src/api/endpoints.py) as `uuid4()` on every webhook hit
- Included in the Celery task `payload` for every downstream task
- Written into every `interaction_events` row
- Carried as a column on `signal_outbox` and `dead_letter_queue`
- Included in every structured log line via `extra={"trace_id": ...}`

### 9.2 The `interaction_events` audit table

Append-only event log. One row per state transition. Schema in §10; structure here:

```sql
interaction_events (
    id, interaction_id, trace_id,
    stage,           -- the lifecycle event
    status,          -- SUCCESS | FAILED | DEFERRED | IN_PROGRESS
    source,          -- classifier | llm | recording_poller | outbox_dispatcher | sweeper
    metadata JSONB,  -- stage-specific structured fields
    occurred_at
)
```

Stages emitted across the pipeline:

| Stage | Written by | `metadata` contents |
|-------|-----------|---------------------|
| `ENDED` | endpoint | `{call_sid, duration_seconds, call_status}` |
| `TRIAGED` | triage task | `{lane, suggested_call_stage, matched_rules}` |
| `ANALYZE_ACQUIRED` | analyse task (after ledger grant) | `{customer_id, est_tokens, lane}` |
| `ANALYZE_DEFERRED` | analyse task (on ledger deny) | `{reason, retry_after_ms, attempt}` |
| `ANALYZED` | analyse task or triage skip | `{tokens_used, latency_ms, model, source}` |
| `OUTBOX_INSERTED` | analyse task | `{dispatch_types: [...]}` |
| `OUTBOX_DISPATCHED` | outbox worker | `{dispatch_type, idempotency_key, attempt}` |
| `OUTBOX_FAILED` | outbox worker | `{dispatch_type, error}` |
| `RECORDING_POLL` | recording task | `{attempt, poll_status}` |
| `RECORDING_TERMINAL` | recording task | `{terminal_status, attempt_count}` |
| `DEAD_LETTERED` | DLQ writer | `{source, reason, dlq_id}` |
| `REPLAYED` | DLQ replay function | `{dlq_id, replayed_by}` |

Volume: at 100K calls × ~10 events/call = 1M rows per campaign. Postgres handles this comfortably with the indexes in §10. Retention: **30 days hot in `interaction_events`**; archived to S3 (Parquet, partitioned by date) afterward. The hot table is queryable by ops; the archive is queryable by analytics.

### 9.3 Structured log discipline

Every log line MUST include:
- `interaction_id`
- `trace_id`
- `customer_id` (when known at the call site)
- `stage` (the same vocabulary as `interaction_events`)

Every log line MUST NOT include (per A8):
- Transcript content (`transcript_text`, `conversation_data`, individual turn `content`)
- Phone numbers (`lead_phone`, `call_sid` is OK as it's an opaque ID)
- Customer names
- Email addresses
- Any free-text field from `additional_data`

Discipline is enforced two ways:

**1. A redacting wrapper around the standard logger:**

```python
REDACTED_KEYS = {'transcript_text', 'conversation_data', 'lead_phone',
                 'lead_email', 'lead_name', 'transcript', 'content'}

class StructuredLogger:
    def info(self, event_name, *, interaction_id, trace_id, customer_id=None,
             stage=None, **extra):
        sanitised = {k: v for k, v in extra.items() if k not in REDACTED_KEYS}
        logger.info(event_name, extra={
            'interaction_id': str(interaction_id),
            'trace_id': str(trace_id),
            'customer_id': str(customer_id) if customer_id else None,
            'stage': stage,
            **sanitised,
        })
```

Calling `log.info("...", transcript_text=ctx.transcript_text)` silently drops the field — defence in depth, not a runtime crash.

**2. CI grep check.** A test that fails the build if any new code adds a log call referencing forbidden field names without going through the wrapper.

### 9.4 Alert thresholds (consolidated)

| Domain | Condition | Level |
|--------|-----------|------:|
| LLM rate limit | Global TPM utilisation > 90% sustained 1 min | WARN |
| LLM rate limit | Acquire denial rate > 5% sustained 1 min | WARN |
| LLM rate limit | Acquire denial rate > 20% sustained 1 min | PAGE |
| LLM rate limit | Any 429 surfaced from provider | **PAGE** (should never happen) |
| Recording | `unavailable` rate > 10% in 5-min window | WARN |
| Recording | `fetch_error` rate > 2% in 5-min window | PAGE |
| Recording | Any 401/403 from Exotel | PAGE (config issue) |
| Outbox | Dispatch latency p95 > 5s | WARN |
| Outbox | Reconciliation finds > 50 stuck `in_progress` rows | WARN |
| DLQ | Inserts > 0.1% of recent traffic in 5 min | WARN |
| DLQ | Inserts > 1% in 5 min | PAGE |
| DLQ | Depth > 1000 `pending_review` | PAGE |
| Triage | Queue depth > 10K | WARN |
| Pressure gauge | Publisher missing (key absent for > 30s) | PAGE |
| Customer budget | Effective budget < `MIN_FAIR_SHARE` sustained > 5 min | INFO log |

All thresholds live in `config.py` and are tunable per environment (dev / staging / prod). The PAGE-level alerts wake an on-call engineer; WARN-level alerts go to a Slack channel for review.

### 9.5 Operational dashboards

Five dashboards, each with a single purpose:

**Pipeline depth** — gauges for triage / hot / cold / recording / outbox queue depths. Plus the `platform_pressure` gauge value. At a glance: is the system keeping up?

**Throughput** — calls/sec by lane; LLM calls/min; tokens/min global; tokens/min top-10 customers (multi-line); skip rate by reason (stacked bar). At a glance: what's running through the system?

**Latency** — end-to-end p50/p95/p99 by lane; LLM call p50/p95/p99; outbox dispatch p50/p95/p99; recording poll-to-upload p50/p95. At a glance: are we meeting SLAs?

**Failures** — DLQ insert rate by source; DLQ pending_review count; recording terminal state breakdown; outbox failure rate by dispatch_type. At a glance: what's broken?

**Customer health** — per-customer TPM consumption vs reserved; per-customer DLQ rate; per-customer cold-lane queue wait p95. At a glance: which customers are affected when something is wrong?

### 9.6 Walk-through: debugging a 3-day-old interaction

Customer Support pings ops:
> *"Customer X says they confirmed a rebook on Tuesday but never got the WhatsApp confirmation. Interaction ID `abc-123`."*

Engineer's investigation, all from SQL:

**Step 1 — What stages did this interaction reach?**

```sql
SELECT stage, status, source, occurred_at, metadata
FROM interaction_events
WHERE interaction_id = 'abc-123'
ORDER BY occurred_at;
```

Returns the chronological event list. Engineer sees:

```
ENDED              T+0       success  endpoint
TRIAGED            T+0.5s    success  classifier   {lane: hot, suggested: rebook_confirmed}
ANALYZE_ACQUIRED   T+1s      success  ledger
ANALYZED           T+5s      success  llm          {tokens_used: 1380, model: gpt-4o}
OUTBOX_INSERTED    T+5.1s    success  analyse      {dispatch_types: [signal_jobs, lead_stage]}
OUTBOX_DISPATCHED  T+6s      success  dispatcher   {dispatch_type: lead_stage}
OUTBOX_FAILED      T+6.5s    failed   dispatcher   {dispatch_type: signal_jobs, error: "Twilio 429"}
OUTBOX_FAILED      T+36.5s   failed   dispatcher   {dispatch_type: signal_jobs, attempt: 2}
... (5 attempts total) ...
DEAD_LETTERED      T+1h2m    success  dispatcher   {source: outbox, dlq_id: 42}
```

Already enough to diagnose: signal_jobs dispatch failed five times, ended up in DLQ. Lead stage updated correctly; only WhatsApp is missing.

**Step 2 — Confirm in DLQ:**

```sql
SELECT id, reason, error_history, status
FROM dead_letter_queue
WHERE interaction_id = 'abc-123';
```

Returns the DLQ entry with full payload preserved.

**Step 3 — Was this an isolated failure or a pattern?**

```sql
SELECT date_trunc('hour', created_at) AS hour, COUNT(*)
FROM dead_letter_queue
WHERE source = 'outbox'
  AND original_payload->>'dispatch_type' = 'signal_jobs'
  AND created_at BETWEEN '2026-05-06 00:00' AND '2026-05-09 00:00'
GROUP BY hour ORDER BY hour;
```

Returns hourly distribution. Engineer sees a 47-row spike in one hour on Tuesday — confirms a Twilio rate-limit incident, not a single-call bug.

**Step 4 — Replay the DLQ entry:**

```python
replay(dlq_id=42, replayed_by='engineer@example.com')
```

A new outbox row is inserted; the dispatcher picks it up; customer receives the WhatsApp ~5 seconds later.

**Step 5 — Update the DLQ row's `review_notes`** with the incident summary.

Total elapsed: under 10 minutes. The current system has none of these queries available — replay would require finding the original payload in error logs (if log retention is even sufficient), and pattern detection would not be possible at all.

### 9.7 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC5 (every interaction has complete audit trail) | §9.2 — `interaction_events` + trace_id propagation; §9.6 walk-through is the proof |
| AC6 (every error has structured log with interaction_id) | §9.3 — structured logger enforces required fields; CI grep prevents regressions |
| AC10 (sensitive data identified, protection stated) | §9.3 — log redaction strategy; full security treatment in §11 |
| Constraint 4 (recording failures alertable) | §9.4 — recording-domain rows in the alert table |

---

## 10. Data Model

This section consolidates the schema additions referenced throughout §4–§9 into a single migration script. Every change is **additive** — new tables, new columns with sensible defaults — so the migration is reversible and requires no backfill or downtime.

### 10.1 Migration philosophy

- **Additive only.** No `DROP TABLE`, no `ALTER COLUMN ... DROP`, no destructive renames.
- **No backfill required.** Every new column has a default value compatible with existing rows. Pending data uses `NULL` or sensible defaults (e.g., `recording_status='pending'` does not back-process old recordings; old rows simply have a default value forever).
- **Reversible.** Each step has a corresponding `DOWN` migration.
- **Indexes use `CREATE INDEX CONCURRENTLY`** in the production migration to avoid table locks.

### 10.2 Changes to `interactions`

```sql
-- New columns supporting triage, recording, and audit
ALTER TABLE interactions
    ADD COLUMN trace_id                  UUID,
    ADD COLUMN lane                      VARCHAR(8),                  -- 'hot' | 'cold' | 'skip'
    ADD COLUMN classifier_verdict        JSONB,                       -- {lane, suggested_call_stage, matched_rules}
    ADD COLUMN analyzed_at               TIMESTAMPTZ,                 -- for idempotent result writes
    ADD COLUMN recording_status          VARCHAR(32) NOT NULL DEFAULT 'pending',
    ADD COLUMN recording_attempt_count   INT NOT NULL DEFAULT 0,
    ADD COLUMN recording_last_attempt_at TIMESTAMPTZ,
    ADD COLUMN recording_terminal_at     TIMESTAMPTZ;

-- Status enum gains values for the new lifecycle states
-- (Implementation note: the existing column is `Enum(InteractionStatus)`. For
-- migration simplicity we change to VARCHAR(32) with a CHECK constraint, which
-- avoids ALTER TYPE complexity across worker rolling restarts.)
ALTER TABLE interactions
    ALTER COLUMN status TYPE VARCHAR(32),
    ADD CONSTRAINT chk_status_value CHECK (status IN (
        'INITIATED', 'RINGING', 'IN_PROGRESS', 'ENDED', 'FAILED',
        'PROCESSING',           -- legacy; deprecated, retained for back-compat
        'ANALYZING',            -- analyse task is running
        'ANALYZED',             -- LLM analysis completed
        'ANALYSIS_SKIPPED',     -- triage skip path; no LLM consumed
        'DEAD_LETTERED'         -- analysis terminally failed
    ));

-- Indexes
CREATE INDEX CONCURRENTLY ix_interactions_trace
    ON interactions (trace_id);
CREATE INDEX CONCURRENTLY ix_interactions_recording_pending
    ON interactions (recording_status)
    WHERE recording_status IN ('pending', 'uploading');
CREATE INDEX CONCURRENTLY ix_interactions_status_lane
    ON interactions (status, lane);
```

**Note on `analyzed_at`.** This column is the idempotency anchor for analysis result writes. The analyse task uses `WHERE analyzed_at IS NULL` to prevent double-write on Celery redelivery (§8.11).

**Note on `classifier_verdict` as JSONB.** Stored as a single JSONB so the classifier's output structure can evolve without further migrations. Trade-off: harder to index than separate columns, but classifier verdict is read for debugging, not for hot-path queries.

### 10.3 New table: `token_ledger`

The durable, billing-grade record of every successful LLM call.

```sql
CREATE TABLE token_ledger (
    id              BIGSERIAL PRIMARY KEY,
    interaction_id  UUID NOT NULL,
    trace_id        UUID NOT NULL,
    customer_id     UUID NOT NULL,
    campaign_id     UUID,
    tokens_used     INT NOT NULL,
    model           VARCHAR(64) NOT NULL,
    provider        VARCHAR(32) NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_tokens_nonneg CHECK (tokens_used >= 0)
);

CREATE INDEX ix_ledger_customer_time  ON token_ledger (customer_id, occurred_at DESC);
CREATE INDEX ix_ledger_campaign_time  ON token_ledger (campaign_id, occurred_at DESC);
CREATE INDEX ix_ledger_recent_global  ON token_ledger (occurred_at DESC);
CREATE INDEX ix_ledger_interaction    ON token_ledger (interaction_id);
```

Append-only. Never updated, never deleted (until retention archive, §10.10).

The `recent_global` index supports the §4.8 Redis-rebuild query *(SELECT … WHERE occurred_at > NOW() - INTERVAL '2 minutes')* and time-window dashboards.

### 10.4 New table: `customer_config`

Per-customer budgeting parameters consumed by §5's effective-budget formula.

```sql
CREATE TABLE customer_config (
    customer_id    UUID PRIMARY KEY,
    reserved_tpm   INT NOT NULL DEFAULT 0,    -- guaranteed TPM (0 = unreserved)
    burst_weight   INT NOT NULL DEFAULT 1,    -- WFQ weight on burst pool (0 = burst-excluded)
    max_tpm        INT,                       -- optional hard ceiling; NULL = no cap

    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_reserved_nonneg CHECK (reserved_tpm >= 0),
    CONSTRAINT chk_weight_nonneg   CHECK (burst_weight >= 0),
    CONSTRAINT chk_max_positive    CHECK (max_tpm IS NULL OR max_tpm > 0)
);
```

A customer with no row is treated as `(reserved_tpm=0, burst_weight=1)` at the application layer. The first acquire for an unknown customer creates the row via `INSERT ... ON CONFLICT (customer_id) DO NOTHING`.

### 10.5 New table: `campaign_config`

Per-campaign override of how much of a customer's reserved capacity is directed at this campaign (§5.8).

```sql
CREATE TABLE campaign_config (
    campaign_id        UUID PRIMARY KEY,
    customer_id        UUID NOT NULL,
    reserved_share     NUMERIC(4,3) NOT NULL DEFAULT 1.000,
                                              -- fraction of customer's reserved_tpm; 0..1
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_share_range CHECK (reserved_share >= 0 AND reserved_share <= 1)
);

CREATE INDEX ix_campaign_customer ON campaign_config (customer_id);
```

Default `1.000` means "give this campaign the customer's full reserved capacity" — appropriate for a single-campaign customer.

### 10.6 New table: `interaction_events`

Append-only audit log. Schema sketched in §9.2; full SQL here.

```sql
CREATE TABLE interaction_events (
    id              BIGSERIAL PRIMARY KEY,
    interaction_id  UUID NOT NULL,
    trace_id        UUID NOT NULL,

    stage           VARCHAR(32) NOT NULL,    -- ENDED | TRIAGED | ANALYZE_ACQUIRED | ...
    status          VARCHAR(16) NOT NULL,    -- SUCCESS | FAILED | DEFERRED | IN_PROGRESS
    source          VARCHAR(32),             -- classifier | llm | recording_poller | ...
    metadata        JSONB NOT NULL DEFAULT '{}',

    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX ix_events_interaction_time  ON interaction_events (interaction_id, occurred_at);
CREATE INDEX ix_events_trace              ON interaction_events (trace_id);
CREATE INDEX ix_events_stage_time         ON interaction_events (stage, occurred_at DESC);
```

The `stage_time` index supports queries like *"all `OUTBOX_FAILED` events in the last hour"* used by ops dashboards.

### 10.7 New table: `signal_outbox`

The durable side-effect queue (§8.3). Repeated here for completeness:

```sql
CREATE TABLE signal_outbox (
    id              BIGSERIAL PRIMARY KEY,
    interaction_id  UUID NOT NULL,
    customer_id     UUID NOT NULL,
    trace_id        UUID NOT NULL,

    dispatch_type   VARCHAR(32) NOT NULL,    -- 'signal_jobs' | 'lead_stage' | 'crm_push'
    payload         JSONB NOT NULL,
    idempotency_key UUID NOT NULL DEFAULT gen_random_uuid(),

    status          VARCHAR(16) NOT NULL DEFAULT 'pending',
                                              -- 'pending' | 'in_progress' | 'dispatched' | 'failed'
    attempt_count   INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error      TEXT,
    in_progress_at  TIMESTAMPTZ,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at   TIMESTAMPTZ,

    CONSTRAINT chk_outbox_status CHECK (status IN
        ('pending', 'in_progress', 'dispatched', 'failed'))
);

CREATE INDEX ix_outbox_dispatch_ready
    ON signal_outbox (next_attempt_at, id)
    WHERE status = 'pending';
CREATE INDEX ix_outbox_in_progress
    ON signal_outbox (in_progress_at)
    WHERE status = 'in_progress';
CREATE INDEX ix_outbox_interaction
    ON signal_outbox (interaction_id);
```

The `in_progress` partial index supports the stuck-row reconciliation job (§8.5).

### 10.8 New table: `dead_letter_queue`

Terminal failures awaiting human review (§8.7). Repeated here for completeness:

```sql
CREATE TABLE dead_letter_queue (
    id               BIGSERIAL PRIMARY KEY,
    interaction_id   UUID,
    customer_id      UUID,
    trace_id         UUID,

    source           VARCHAR(32) NOT NULL,    -- 'outbox' | 'analysis' | 'triage'
    reason           VARCHAR(64) NOT NULL,    -- 'max_retries_exhausted' | 'permanent_error' | ...
    error_history    JSONB NOT NULL,          -- [{attempt, error_msg, occurred_at}, ...]
    original_payload JSONB NOT NULL,

    status           VARCHAR(16) NOT NULL DEFAULT 'pending_review',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at      TIMESTAMPTZ,
    reviewed_by      VARCHAR(128),
    review_notes     TEXT,

    CONSTRAINT chk_dlq_status CHECK (status IN
        ('pending_review', 'replayed', 'ignored', 'escalated'))
);

CREATE INDEX ix_dlq_pending      ON dead_letter_queue (created_at)
    WHERE status = 'pending_review';
CREATE INDEX ix_dlq_interaction  ON dead_letter_queue (interaction_id);
CREATE INDEX ix_dlq_source_time  ON dead_letter_queue (source, created_at DESC);
```

### 10.9 Index summary

All new indexes:

| Index | Table | Purpose |
|-------|-------|---------|
| `ix_interactions_trace` | interactions | Lookup by trace_id |
| `ix_interactions_recording_pending` | interactions | Recording reconciliation (partial) |
| `ix_interactions_status_lane` | interactions | Dashboard queries by lifecycle state |
| `ix_ledger_customer_time` | token_ledger | Per-customer billing queries |
| `ix_ledger_campaign_time` | token_ledger | Per-campaign billing queries |
| `ix_ledger_recent_global` | token_ledger | Redis recovery + global TPM trends |
| `ix_ledger_interaction` | token_ledger | Lookup by interaction (debugging) |
| `ix_campaign_customer` | campaign_config | List campaigns per customer |
| `ix_events_interaction_time` | interaction_events | Audit-trail walk-through (§9.6) |
| `ix_events_trace` | interaction_events | Per-attempt event reconstruction |
| `ix_events_stage_time` | interaction_events | Stage-specific dashboards |
| `ix_outbox_dispatch_ready` | signal_outbox | Dispatcher batch fetch (partial) |
| `ix_outbox_in_progress` | signal_outbox | Stuck-row reconciliation (partial) |
| `ix_outbox_interaction` | signal_outbox | Lookup by interaction |
| `ix_dlq_pending` | dead_letter_queue | Pending-review queue (partial) |
| `ix_dlq_interaction` | dead_letter_queue | Replay lookup |
| `ix_dlq_source_time` | dead_letter_queue | Source-specific incident analysis |

**Partial indexes** (predicates with `WHERE`) are used wherever the working set is a small fraction of the table. They keep the index small, improve query plans, and reduce write amplification.

### 10.10 Retention strategy

| Table | Hot retention (Postgres) | Archive |
|-------|-------------------------|---------|
| `interactions` | Indefinite (system of record) | — |
| `interaction_events` | 30 days | S3 Parquet, partitioned by date |
| `token_ledger` | 90 days | S3 Parquet (for billing audit; possibly longer per customer compliance) |
| `signal_outbox` (`dispatched` rows) | 7 days | Truncate |
| `signal_outbox` (`pending`/`in_progress` rows) | Never pruned | — |
| `dead_letter_queue` (`pending_review`) | Never pruned (needs human action) | — |
| `dead_letter_queue` (`replayed` / `ignored` / `escalated`) | 30 days | S3 Parquet |

A daily archive job (Celery beat) handles the moves and prunes. `interactions` itself is kept indefinitely because it's the system of record for the conversation; PII deletion for GDPR / right-to-be-forgotten is a separate workflow that deletes specific rows (§11).

### 10.11 Partitioning (deferred to v2)

At sustained 100K calls/campaign × multiple campaigns/day, `interaction_events` and `token_ledger` will accumulate millions of rows per month. Both are time-keyed and append-only — ideal candidates for monthly partitioning.

Partitioning is **not** in v1 because:
- Postgres handles tens-of-millions-row unpartitioned tables fine with the indexes above
- Partition pruning matters for very large tables (>100M rows), which is a 6+ month horizon
- Adding partitioning later is straightforward (online with `pg_partman`)

Tracked in §15 as future work.

### 10.12 Single migration script

For local dev (`docker-compose up`), a single migration file applies all of §10.2 through §10.8 in order. The file is reversible — each `CREATE TABLE` has a matching `DROP TABLE` and each `ALTER TABLE` has matching column drops.

Filename: `migrations/20260509_postcall_pipeline_v2.sql`.

### 10.13 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC2 (per-customer budget) | §10.4 `customer_config` |
| AC3 (no task lost) | §10.7 outbox + §10.8 DLQ — all durable |
| AC5 (audit trail) | §10.6 `interaction_events` |
| AC6 (structured errors with `interaction_id`) | All tables include `interaction_id` for cross-table joins |
| Constraint 3 (LLM spend attributable) | §10.3 `token_ledger` keyed on `(interaction_id, customer_id, campaign_id)` |

---

## 11. Security

This section states a pragmatic security posture — protections proportional to data sensitivity, no over-engineering. Cross-references A8 (sensitive data assumption) and §9.3 (log redaction).

### 11.1 Data classification

| Category | Examples | Sensitivity | Treatment summary |
|----------|----------|-------------|-------------------|
| Transcript content | `interactions.conversation_data`, summaries, DLQ payloads carrying transcripts | **High** — PII + business intent | Encrypted at rest; redacted from logs |
| Lead PII | name, phone, email (existing `leads` table, out of scope of this redesign) | **High** | Encrypted at rest; redacted from logs |
| Recordings | S3 audio files | **High** — biometric + content | SSE-KMS; non-guessable keys; signed URLs |
| Secrets | LLM API key, S3 credentials, Twilio tokens, DB password | **Critical** — system access | Secrets manager in prod; never in env vars |
| Tenant IDs | `customer_id`, `campaign_id`, `interaction_id`, `trace_id` | **Low** — opaque UUIDs | No special handling required |
| Operational metadata | timestamps, attempt counts, token counts, lane assignment | **Low** — no business meaning | No special handling |

The principle: **encrypt the high-sensitivity columns; protect secrets centrally; leave operational data alone.**

### 11.2 At rest

**Transcripts** (`interactions.conversation_data` JSONB).
Application-side envelope encryption: a per-row Data Encryption Key (DEK) is generated, used to encrypt the JSONB content, then the DEK itself is wrapped by a Customer Master Key (CMK) held in a KMS service (AWS KMS, GCP KMS, etc.). The wrapped DEK is stored alongside the encrypted blob. Reading a transcript requires a KMS unwrap call.

Trade-off: harder to query the JSONB inside Postgres. Acceptable because transcripts are read whole (by the LLM analyse task, by ops debugging) and never searched in-place. Column-level Postgres encryption (pgcrypto) is the simpler alternative; envelope encryption is preferred when KMS is already in the stack.

**DLQ original payloads** (`dead_letter_queue.original_payload`). Same envelope encryption — DLQ payloads can contain transcript content from re-enqueued tasks.

**Recordings** (S3). Bucket policy:
- Server-side encryption with KMS (`SSE-KMS`)
- Non-guessable object keys: `recordings/{interaction_id}.mp3` — UUIDs are unguessable; no sequential IDs
- Bucket-level: `BlockPublicAcls`, `BlockPublicPolicy`, `IgnorePublicAcls`, `RestrictPublicBuckets` all true
- Access via signed URLs (15-minute TTL) only — no direct GET

**Operational tables** (`interaction_events`, `token_ledger`, `signal_outbox`, `customer_config`, `campaign_config`). Not encrypted at rest beyond Postgres's at-rest disk encryption (which is a deployment concern, not a schema concern). They contain no PII or transcript content. The `interaction_events.metadata` JSONB is checked in code review — if a stage ever needs to write PII into metadata, that field gets the same envelope-encryption treatment.

### 11.3 In transit

- **All external HTTP**: TLS-only. Exotel webhook (incoming), LLM provider (outgoing), S3 (outgoing), Twilio / CRM downstreams (outgoing).
- **Database**: Postgres connection requires `sslmode=require`.
- **Redis**: TLS-enabled connection (`rediss://` URL).
- **Internal service-to-service**: network-level isolation (VPC + security groups + private subnets) in v1. mTLS is appropriate when the service mesh grows; not justified for v1's small surface.

### 11.4 Secrets management

| Environment | Source |
|-------------|--------|
| Local dev (`docker-compose up`) | `.env` file, gitignored |
| Production | Secrets manager (AWS Secrets Manager / GCP Secret Manager / Vault) |

The application reads secrets at startup via the secrets manager SDK; secrets do not live in container environment variables in production. Rotation cadence:

- LLM API key: quarterly (provider-recommended)
- S3 credentials: managed via IAM roles, not long-lived keys
- Twilio / CRM tokens: per provider's rotation schedule
- DB password: quarterly

The current `LLM_API_KEY = os.getenv("LLM_API_KEY", "sk-mock-key-for-assessment")` ([config.py:28](src/config.py#L28)) is fine for the assessment; the production wiring replaces the env var with a secrets-manager fetch.

### 11.5 In logs

Already specified in §9.3. Summary: every log line goes through a `StructuredLogger` wrapper that drops a known set of redacted field names (`transcript_text`, `conversation_data`, `lead_phone`, `lead_email`, `lead_name`, `transcript`, `content`). A CI test fails the build if any new code logs forbidden fields.

### 11.6 Access controls

**Service accounts**: backend services authenticate to AWS / GCP via IAM roles scoped to the resources they need. The analyse worker has read on `interactions`, write on `interaction_events`, etc. — least privilege.

**Human read of transcripts**: when an engineer pulls a transcript for debugging (rare; happens during incident review), the read goes through a small admin tool that writes a structured event to `interaction_events` with `stage='TRANSCRIPT_ACCESS'` and `metadata={accessed_by, reason}`. Reusing the existing event log avoids a new table and keeps all human-trail data in one place.

Direct SQL `SELECT conversation_data FROM interactions ...` is discouraged by access policy; the admin tool is the official route. This is a v1 audit control — not perfect (a determined engineer with DB credentials can bypass it), but every legitimate access is recorded.

### 11.7 Deletion (GDPR / right to be forgotten)

The deletion workflow is **not implemented in v1**. When implemented, it will be a single function that for a given `customer_id`:

1. **Scrubs PII columns from `interactions`**: `UPDATE interactions SET conversation_data = NULL, recording_s3_key = NULL WHERE customer_id = $1`. The row is retained (referenced by ledger and events) but the sensitive content is gone.
2. **Deletes recording objects from S3** for those interactions.
3. **Scrubs `interaction_events.metadata`** of any field that could be PII (in practice, none — but defensive).
4. **Keeps `token_ledger`** rows intact (numeric only; required for billing audit).
5. **Records the deletion** as a structured `interaction_events` entry with `stage='CUSTOMER_DELETED'` and metadata including reason, timestamp, and a checksum of what was deleted.

Default retention before mandatory deletion: **7 years** (financial-services-style compliance default). Customer-specific contracts may shorten this. The schema requires no changes to support this workflow when it is added — `interaction_events` is the home for the deletion audit row.

### 11.8 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC10 (sensitive data identified, protection stated) | §11.1 classification + §11.2–§11.6 protections at each layer |
| A8 (PII assumption) | Operationalised across all subsections |

---

## 12. API Interface

### 12.1 The external contract is unchanged

`POST /session/{session_id}/interaction/{interaction_id}/end` keeps the same URL, the same request body, the same response shape, and the same SLA (200 within Exotel's 5-second timeout).

```python
class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    additional_data: Optional[Dict[str, Any]] = None

class InteractionEndResponse(BaseModel):
    status: str               # "ok"
    interaction_id: str
    message: str
```

**Why kept identical:** Exotel is the upstream caller and not under our control. Changing the contract would require a coordinated release with the telephony provider — operationally expensive, and unnecessary because the problems with the current implementation are *internal* to what the endpoint does after receiving the webhook, not in the shape of the contract itself.

The contract being preserved also means this redesign can ship behind a feature flag without any visible change to Exotel — strictly internal cutover.

### 12.2 What changed inside the endpoint

The current endpoint does five things, three of which are wrong:

| Current behaviour | Status |
|---|---|
| Load interaction from DB | ✅ Kept |
| Mark `status = 'ENDED'` | ✅ Kept (now via `interaction_events(ENDED)` write) |
| Detect short transcripts and fast-path them | ⚠️ Moved (now lives in triage task; endpoint just enqueues) |
| Fire `signal_jobs` and `update_lead_stage` with empty payloads | ❌ **Deleted entirely** |
| Enqueue Celery task for full processing | ✅ Kept (now enqueues triage + recording in parallel) |

The deletion of the pre-Celery `signal_jobs` and `lead_stage` calls is the single most consequential change — it eliminates the correctness bug (§2.10 #3) where downstream systems received empty-payload events before analysis ran.

### 12.3 The new endpoint flow

```python
@router.post("/session/{session_id}/interaction/{interaction_id}/end")
async def end_interaction(session_id, interaction_id, request):
    trace_id = uuid4()                         # mint trace ID for this attempt

    interaction = await load_interaction(interaction_id)
    if not interaction:
        raise HTTPException(404, "Interaction not found")

    await update_interaction_status(
        interaction_id, status='ENDED',
        ended_at=datetime.utcnow(),
        duration=request.duration_seconds,
        call_sid=request.call_sid,
        trace_id=trace_id,
    )

    await event_log.write(
        interaction_id, trace_id,
        stage='ENDED', status='SUCCESS', source='endpoint',
        metadata={'call_sid': request.call_sid, 'duration_seconds': request.duration_seconds},
    )

    # Enqueue triage and recording in parallel — both run independently
    triage_task.apply_async(
        args=[str(interaction_id), str(trace_id)],
        queue='triage',
    )
    poll_recording.apply_async(
        args=[str(interaction_id), str(trace_id), 1],   # attempt=1
        queue='recording',
    )

    return InteractionEndResponse(
        status='ok',
        interaction_id=str(interaction_id),
        message='Interaction ended, processing enqueued',
    )
```

The endpoint is now ~30 lines (vs. ~150 in the current implementation), does no business logic, and returns within milliseconds.

### 12.4 What the endpoint no longer does

- **No `asyncio.create_task()`**. All work that needs to outlive the request is enqueued to Celery on a durable queue. There is no fire-and-forget that survives only in process memory.
- **No short-call detection**. The triage task handles it (§6.2). The endpoint stays generic.
- **No transcript flattening or payload assembly**. The triage and analyse tasks read what they need from the DB.
- **No call to `signal_jobs` or `update_lead_stage`**. Side effects flow exclusively through the outbox after analysis completes (§8.2).

### 12.5 Error handling

| Failure mode | Response |
|---|---|
| Interaction not found in DB | `404 Not Found` |
| Triage task enqueue fails (Celery broker unreachable) | `503 Service Unavailable` + structured `endpoint_enqueue_failed` event with `trace_id` — Exotel will retry the webhook |
| Recording task enqueue fails (same broker issue) | Logged as warning; the analysis path still proceeds; recording will not happen for this interaction (terminal state remains `pending` and an alert fires from §9.4 if the rate exceeds threshold) |
| Unexpected exception | `500 Internal Server Error` + structured `endpoint_error` event with `trace_id` — Exotel will retry |

Every non-200 response writes a structured event with the `trace_id`. The on-call engineer can reconstruct what the endpoint saw without grep-ing across log files.

### 12.6 What this satisfies

| AC | Mechanism |
|----|-----------|
| AC6 (every error has structured log with `interaction_id`) | §12.5 — every error path writes a structured event |
| AC8 (short transcripts skip LLM) | §12.2 — gate moved to triage task; endpoint stays generic |
| AC3 (no task lost) | §12.4 — no `asyncio.create_task`; everything goes through Celery |

---

## 13. Trade-offs & Alternatives Considered

Each entry below names a real choice made elsewhere in this document and the alternative(s) considered against it. Nothing speculative — every "considered" option was actually weighed in the design.

### 13.1 Architectural / strategic

| # | Decision (§) | Alternative considered | Why we chose what we did |
|--:|---|---|---|
| 1 | Patch path, not rewrite (§3.1) | Full greenfield rewrite | External contract preserved; infrastructure unchanged; LOC churn comparable but risk profile much lower; staged commit sequence enables feature-flagged cutover |
| 2 | Postgres as the durability boundary (§3.3 #1, §8.1) | Continue with Redis-backed durability for retry state | Current Redis-only design loses both broker and retry state on a single Redis bounce; Postgres gives RPO bounded by backup frequency |
| 3 | No new infrastructure (§3.4) | RabbitMQ (priority queues), Kafka, service mesh | Operational cost not justified at 100K-call scale; Postgres + Celery + Redis + S3 is sufficient when used correctly |
| 4 | API contract unchanged (§12.1) | Add fields, version the endpoint, return richer payload | Exotel is the upstream caller and not under our control; the problems are *internal* to what the endpoint does, not in its shape |

### 13.2 Rate limiting and budgeting

| # | Decision (§) | Alternative considered | Why we chose what we did |
|--:|---|---|---|
| 5 | 70/30 reserved/burst split (A3, §5) | Pure WFQ (no reservation), Stripe-style 20/80 critical-traffic split, telco-style 95% oversubscription | Voicebot duty cycles are correlated (campaigns run in business hours), invalidating telco-style statistical multiplexing; 70/30 matches published Kubernetes / DynamoDB capacity guidance and gives contracted customers a guaranteed floor |
| 6 | Unified algorithm: unreserved customers = `reserved_tpm = 0` (§5.1) | Separate code paths for reserved vs unreserved | One algorithm is easier to reason about and test; customers genuinely exist on a continuum, not in two discrete classes |
| 7 | Two-bucket smoothed sliding window (§4.3) | Single fixed bucket per minute (simpler), per-call timestamps in Redis sorted set (exact) | Single-bucket has a cliff at minute boundaries that lets a customer effectively double their budget; sorted set is ~10× slower at our throughput |
| 8 | Separate `pending` counter key (§4.4) | Combined pending + committed in a single key | Refund on commit becomes a clean DECR/INCR pair (no negative deltas); ops can read "pending" and "committed" as distinct numbers; lease expiry doesn't need minute-bucket awareness |
| 9 | 60-second reservation lease (§4.5) | Shorter (≈30s — race risk during slow LLM calls); longer (≈5min — slow recovery from worker crash) | Constraint is `lease > LLM_REQUEST_HARD_TIMEOUT` (30s); 60s gives ~10× p99 LLM latency headroom while bounding crash-recovery time |
| 10 | Refund on commit using `actual` tokens (§4.6) | Charge full `est_tokens` regardless of `actual` | A9 requires billing-grade attribution; over-charging by `max_completion_tokens` would be a meaningful customer overcharge |

### 13.3 Triage and classification

| # | Decision (§) | Alternative considered | Why we chose what we did |
|--:|---|---|---|
| 11 | Pure rules classifier (§6.1) | Embedding-based classifier (sentence-transformers), small LLM (Haiku / gpt-4o-mini), hybrid rules + LLM fallback | Fixtures show ~80% of cases are cleanly keyword-separable; pure rules are free, explainable, and ops-tunable; classifier interface is pluggable so we can upgrade later without changing callers |
| 12 | Single language (English) in v1 (A10) | Multi-language keyword sets at launch | Hinglish handling adds complexity; default-hot fallback ensures non-English transcripts still get full LLM analysis — no correctness loss, only cost |
| 13 | Default-hot on classifier ambiguity (A1, §6.5) | Default-cold (cheaper) | Cost of a missed hot is sales-loss-shaped; cost of paid cold is bounded LLM tokens; asymmetric cost → asymmetric default |
| 14 | Hot-before-Cold rule order (§6.2) | Cold-first, alphabetical, last-match-wins | A transcript containing both "confirmed" and "not interested" should route hot — the hot signal suggests value worth investigating fully |
| 15 | Negative-keyword skip rules (§6.2) | Only `turn_count < 4` skips; everything else gets full LLM | "Wrong number" / "do not call" calls have nothing for the LLM to extract; ~10% additional token savings with no information loss |
| 16 | Triage as its own Celery task (§6.6) | Synchronous in the endpoint, first step inside the analyse task | Endpoint stays thin (Exotel's 5s timeout); triage workers scale independently from analyse workers (CPU-bound vs I/O-bound) |

### 13.4 Recording, outbox, and durability

| # | Decision (§) | Alternative considered | Why we chose what we did |
|--:|---|---|---|
| 17 | Recording decoupled from analysis (§7.1) | Sequential in a single Celery task (current behaviour) | Recording is for compliance / audit, not LLM input; coupling forces 45s latency on every call for an artifact the analysis doesn't need |
| 18 | Recording poll schedule `[5, 10, 30, 60, 120, 240]` with ±20% jitter (§7.2) | Pure geometric `[5, 10, 20, 40, 80, 160]`, fixed-interval polling, single attempt | Pure geometric tops out at ~5min (misses long tail); fixed-interval wastes API calls; the chosen schedule front-loads cheap polls and then escalates more slowly to match Exotel's actual delivery distribution |
| 19 | Outbox pattern for side effects (§8.2) | Inline fire-and-forget (current), async-only with no durability, Kafka topic | Transactional consistency with the analysis result write; durable across worker crashes; supports replay; Kafka is overkill at our scale |
| 20 | DLQ scope — recording failures stay inline (§8.6) | All failures go to DLQ uniformly, including recording | Recording terminal state is already captured on the `interactions` row (`recording_status`); DLQ would duplicate without adding value |
| 21 | State-before-action *and* idempotency key for outbox dispatch (§8.5) | Either alone (just state machine, just idempotency key) | Defense in depth: state-before-action handles our worker crashes; idempotency key handles downstream duplicate-suppression; downstream services vary in their idempotency support |

### 13.5 Data model

| # | Decision (§) | Alternative considered | Why we chose what we did |
|--:|---|---|---|
| 22 | Keep `interactions.interaction_metadata` JSONB; add `analyzed_at` for idempotency (§10.2) | Introduce a new `analysis_results` table with one row per LLM attempt | Dashboard read path unchanged; auditability comes from `interaction_events` instead of duplicating it in a separate table; less migration surface |

### 13.6 Scope deferrals

| # | Decision (§) | Alternative considered | Why we chose what we did |
|--:|---|---|---|
| 23 | Per-customer `classifier_overrides` deferred to v2 (§15.1) | Ship per-customer overrides in v1 | Not on the AC list; adds operational tooling burden (validating customer-supplied keywords); schema is additive — can be added later without migration |

---

## 14. Known Weaknesses

This section enumerates real gaps in the design — not humblebrags. Each entry names a weakness, what mitigates it (if anything), and what fixing it would look like. The intent is to surface what an interviewer might push on, and to be honest about which trade-offs we accepted vs. which we punted.

### 14.1 Lease window allows brief budget under-counting

**The gap.** Between sweeper-expire and a late commit (the §4.6 race), an in-flight call's tokens are briefly refunded by the sweeper but then committed by the late worker. During this window — typically 15s, worst case 60s — parallel acquires from the same customer might over-grant by up to `est_tokens` worth of slack.

**Mitigation in design.** Bounded by lease duration; self-correcting on the next commit. Acceptable for our SLA targets.

**What fixing it looks like.** Either shorten the lease (worsens worker-crash recovery — bad trade) or hold the reservation until commit instead of relying on lease expiry (defeats the purpose of having a lease). We chose to accept the bounded slack and document it.

### 14.2 Two-bucket smoothing assumes uniform distribution

**The gap.** §4.3's smoothed sliding window assumes consumption was uniformly distributed within the previous minute. A bursty customer who spent all of last minute's tokens in the final 5 seconds will have their burst under-counted by the smoothing.

**Mitigation in design.** None — this is an explicit approximation we accept. For high call volumes dispersed across each minute, the assumption holds well enough.

**What fixing it looks like.** Replace with per-call timestamps in a Redis sorted set (exact, but ~10× slower at our throughput). Not justified at our scale; revisit if a customer with extreme burstiness needs sub-second-accurate enforcement.

### 14.3 Pure-rules classifier degrades on sarcasm, negation, and non-English

**The gap.** A transcript saying *"I'm SO interested in your product"* (sarcastic) routes hot via the "interested" keyword. *"I don't want to escalate this"* still matches the `escalate` keyword. Hinglish or other-language transcripts default to hot lane (per A10).

**Mitigation in design.** Default-hot ensures correctness even when classification is wrong — the LLM does the real work. The cost is wasted hot-lane budget, not lost calls.

**What fixing it looks like.** §15 entry for embedding-based or hybrid LLM-fallback classifier. The classifier interface in §6.2 is pluggable — the upgrade path doesn't require changing callers.

### 14.4 Default-hot can saturate the hot lane if rules drift

**The gap.** If keyword rules become stale (a customer's vocabulary shifts, new disposition types emerge), more transcripts fall through to default-hot. The hot lane has reserved-but-finite capacity — uncontrolled drift could starve genuine hot calls.

**Mitigation in design.** §4.9's `platform_pressure` gauge surfaces the symptom; the dialler responds by slowing. §9.5's customer-health dashboard shows per-customer cold-lane queue depth.

**What fixing it looks like.** Add a "default-hot rate > 30% sustained over 1 hour" alert in §9.4. Operations response is to extend keyword lists. Longer term: per-customer overrides (§15.1) let customers tune their own rules without a deploy.

### 14.5 No partitioning on `interaction_events` and `token_ledger`

**The gap.** §10.11 explicitly defers monthly partitioning. At sustained 100K calls/campaign × multiple campaigns/day × 365 days, both tables exceed 100M rows within a year. Above that range, Postgres performance on indexed queries starts to degrade and autovacuum overhead grows.

**Mitigation in design.** Indexes are sized appropriately for tens-of-millions-of-rows scale; the partial indexes keep working sets small.

**What fixing it looks like.** Switch to `pg_partman` for monthly time-based partitioning when row counts approach ~50M. Routine ops task, no downtime. Tracked in §15.

### 14.6 Redis-loss recovery has a stale-enforcement window

**The gap.** §4.8 rebuilds enforcement counters from `token_ledger` after a Redis loss. During the rebuild (tens of seconds), enforcement is stale — up to ~2 minutes of recent consumption may be temporarily under-enforced. A customer could briefly over-burst.

**Mitigation in design.** Bounded duration; pending counters are not replayed (in-flight reservations are also lost, and affected workers re-acquire on retry).

**What fixing it looks like.** Run Redis as a replicated pair (primary + replica) so failover is sub-second instead of a full rebuild. Adds infrastructure cost; not in v1 scope. The rebuild path remains as the bottom-of-the-stack recovery.

### 14.7 No webhook signature verification

**The gap.** The Exotel endpoint accepts any well-formed POST. A malicious actor with knowledge of the URL pattern could forge `interaction_id` values and trigger spurious processing — wasting LLM budget, polluting analytics, or attempting to disrupt other customers' work.

**Mitigation in design.** None.

**What fixing it looks like.** Verify Exotel's signature header (HMAC-SHA256 of body with shared secret) at the FastAPI middleware layer. Standard pattern; should be added before this design ships to production. Not in v1 scope because it's not on the AC list and the assignment doesn't simulate adversarial traffic — but it would be the first thing on a pre-prod hardening checklist.

### 14.8 Outbox dispatcher polling adds latency

**The gap.** §8.4's 1-second beat poll means outbox row insert → dispatch start has ~500ms average latency. For a tighter hot-lane SLA (e.g., < 5s), this becomes meaningful.

**Mitigation in design.** Hot-lane SLA is 20s (A4); 500ms is < 2.5% of the budget — acceptable at our target.

**What fixing it looks like.** Switch to Postgres LISTEN/NOTIFY for sub-second dispatch trigger. Adds a long-running listener process and increases ops surface; trade-off is operational complexity vs. latency. Not justified at our current SLA.

### 14.9 `MAX_DEFER_ATTEMPTS = 50` is heuristic, not measured

**The gap.** §4.7 caps defer-and-reschedule at 50 attempts (~50 minutes worst case). The number is a guess — chosen to be longer than any plausible TPM-recovery window and shorter than the 8-hour campaign budget. We have no production data to validate it.

**Mitigation in design.** Beyond the cap, the analyse task moves to DLQ — alertable, replayable, not lost.

**What fixing it looks like.** Tune from production data once we have it. Track the distribution of defer counts per call; if many calls are hitting 50 legitimately, raise it. If most calls succeed within 10 attempts, lower it to fail misconfigured customers fast. Default is intentionally conservative.

---

## 15. What I Would Do With More Time

_Specific, prioritised list — not a generic wishlist._

1. **Per-customer classifier overrides.** The triage classifier (§6) currently uses platform-wide keyword lists in `src/config.py`. Customers cannot extend or override these without a deploy. Add a `classifier_overrides JSONB` column to `customer_config` that the classifier merges with base rules at classify time. Deferred from v1 because (a) it adds operational tooling for validating customer-supplied keywords and (b) it is "nice to have" on the README's list, not required. The schema is additive — adding it later does not require migration.
