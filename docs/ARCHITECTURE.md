# Architecture

This document records the high-level architecture of the notification system and the reasoning behind each major decision. It is the source of truth for design intent. Implementation details (schemas, topic names, exact backoff values, etc.) are deliberately deferred to follow-up documents so the shape of the system stays legible.

The system is event-driven. PostgreSQL holds all mutable state and is the single source of truth. Apache Kafka carries events between components. Five single-purpose processes are choreographed via Postgres state and one Kafka transport. The transactional outbox pattern is concentrated in a single relay process so business-logic components never perform a Postgres↔Kafka dual-write. The rate limiter sits at the worker, immediately before each provider call, so the brief's per-channel cap is enforced on the actual rate-limited resource. The dispatcher and reaper consult Kafka consumer-group lag for backpressure, which keeps attempt counters honest during worker outages. Unprocessable Kafka messages are routed to a per-channel dead-letter queue atomically with terminal-failing the corresponding notification, so the database never falls out of sync with the transport and a single bad message cannot block its partition.

---

## 1. Scope and constraints

- **What we're building**: an event-driven notification system that accepts requests over HTTP, processes them asynchronously, and delivers them through pluggable channels (SMS, Email, Push) with priority, rate limiting, retries, and observability.
- **Stated scale**: millions of notifications per day, with bursts (flash sales, breaking news).
- **Per-channel rate cap**: 100 messages per second per channel, enforced on **provider calls** (the actual side effect), not on internal pipeline events.
- **Provider for the assessment**: `webhook.site` simulates the external delivery target.
- **Production-grade framing**: we optimize for correctness, observability, and graceful degradation. We accept that some choices favor demonstrability over absolute scale.

---

## 2. Core decisions

These are locked in. The reasoning for each is in section 4.


| Decision                     | Choice                                                                                                                                                            |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Repository layout            | Single repository                                                                                                                                                 |
| Binary layout                | One binary, multiple run modes (subcommands)                                                                                                                      |
| Architectural shape          | Five single-purpose processes, choreographed via Postgres state + one Kafka transport                                                                             |
| Source of truth              | PostgreSQL                                                                                                                                                        |
| Transport                    | Apache Kafka                                                                                                                                                      |
| Coordination style           | Event-driven choreography (no central orchestrator)                                                                                                               |
| Outbox tables                | One shared `outbox` table, drained by a single relay process                                                                                                      |
| Dual-write strategy          | Producers write only to Postgres (atomically with outbox row); the relay handles the residual Postgres↔Kafka dual-write with at-least-once + idempotent consumers |
| Rate limit enforcement point | The worker, immediately before each provider call                                                                                                                 |
| Backpressure signal          | Kafka consumer-group lag on `send.<channel>`                                                                                                                      |


---

## 3. Runtime topology

Five run modes, all from the same Go binary:

```
   HTTP
    │
    ▼
  [api] ──────────────────► Postgres (notifications)
                                │
                                │  poll for eligible rows
  [dispatcher] ◄────────────────┤  (lag-aware: pause if consumer lag is high)
        │                       │  claim atomically (FOR UPDATE SKIP LOCKED)
        │                       │  insert outbox row in same tx
        ▼                       │
     Postgres ──► [relay] ──► Kafka(send.<channel>)
                                       │
                                       ▼
                                  [worker] ──► provider (webhook.site)
                                       │       (rate-limited via Redis token bucket)
                                       │       writes delivery_attempts
                                       │       writes outbox row(events.notification)
                                       ▼
                                   Postgres ──► [relay] ──► Kafka(events.notification)
                                       ▲                              │
                                       │                              ▼
                                  [reaper] writes               downstream consumers
                                  outbox row on T10              (audit, analytics, …)
                                  (terminal-fail)

  [reaper] runs continuously: resets stuck DISPATCHED rows back to PENDING,
           and terminal-fails rows past max_attempts (emitting one
           events.notification per failed row via the same outbox+relay path).
           Skips both passes while consumer lag is high (so worker outages
           don't silently burn the attempt counter).
```

### 3.1 Component responsibilities


| Mode                   | One-line job                                                                                                                                                                                              | Reads                                                       | Writes                                         |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | ---------------------------------------------- |
| `api`                  | Accept and validate requests, persist notifications, serve queries, handle cancellation                                                                                                                   | HTTP, `notifications`, `delivery_attempts`                  | `notifications`, `outbox` (cancel T3)          |
| `dispatcher`           | Claim eligible rows from Postgres, queue them for sending. Pauses claiming when downstream consumer lag is high.                                                                                          | `notifications`, Kafka admin (lag)                          | `notifications`, `outbox`                      |
| `relay`                | Bridge `outbox` to Kafka for every topic                                                                                                                                                                  | `outbox`                                                    | Kafka, `outbox.published_at`                   |
| `worker` (per channel) | Consume `send.<channel>`. Rate-limit via Redis. Call provider. Record attempt + outcome. Emit terminal event. Route unprocessable messages to `send.<channel>.dlq` and terminal-fail their notifications. | `Kafka(send.<channel>)`, `notifications`                    | `delivery_attempts`, `notifications`, `outbox` |
| `reaper`               | Recover stuck rows. Lag-aware: skips resets when consumer lag is high. Emits a terminal-fail event per row it terminates.                                                                                 | `notifications`, Kafka admin (lag)                          | `notifications`, `outbox` (terminal-fail T10)  |


Every producer (api, dispatcher, worker, reaper) writes to **only Postgres** in any single transaction — the api at T3 cancel, the reaper at T10 terminal-fail, the dispatcher at every claim, the worker at every outcome. The relay is the only component that performs the Postgres↔Kafka bridge.

### 3.2 Why the binary is one process with run modes

Single repository keeps types, migrations, configuration, and CI together. One binary means shared code (store, kafka, ratelimit, observability) is reused across all roles without packaging overhead. Run modes give us independent, scalable processes at deployment time, which is the real production property we need. We get the dev ergonomics of a monolith and the runtime topology of microservices.

Subcommands are: `./notifications api`, `./notifications dispatcher`, `./notifications worker --channel=sms`, `./notifications relay`, `./notifications reaper`.

---

## 4. Decision reasoning

### 4.1 Why five processes

The five processes are: api, dispatcher, worker, relay, reaper. Each has one responsibility and a clearly bounded set of inputs and outputs.

- **api** is the synchronous entry point. It validates requests and persists notifications. It does not know about Kafka, providers, or rate limits.
- **dispatcher** decides which notifications are eligible to send right now and atomically transitions them out of `PENDING`. It is the only component that interprets priority and `eligible_at`.
- **worker** is the only component that talks to providers. Holding rate limiting, attempt accounting, and result classification in one process lets `delivery_attempts INSERT + notifications UPDATE + outbox INSERT` happen in a single Postgres transaction. Splitting the post-call pipeline into separate Kafka-coordinated services would add ceremony without buying anything: classification has no external dependencies, and persistence is one transaction either way.
- **relay** is a small, well-understood process that drains a single `outbox` table to Kafka. Concentrating the unavoidable Postgres↔Kafka dual-write here means business-logic components (api, dispatcher, worker) can each commit their state changes atomically against Postgres alone.
- **reaper** runs continuously to reset stuck claims and to terminal-fail notifications that have exhausted attempts.

### 4.2 Why one shared outbox table

A single `outbox` table with a `topic` column gives:

- **Backlog visibility**: `SELECT count(*) FROM outbox WHERE topic LIKE 'send.%' AND published_at IS NULL` answers "how far behind is the send pipeline?".
- **Small index**: a partial index on `(id) WHERE published_at IS NULL` only covers the unpublished hot working set, regardless of which topic the row is for or how large the historical table grows.
- **Operational simplicity**: one migration, one set of metrics, one relay loop.
- **Easy split later**: if one topic dominates outbox lag dashboards, splitting that topic into its own table is a non-breaking schema change.

The single shared outbox is the simpler default; per-stage tables are a future optimization with a clear trigger.

### 4.3 Why rate limiting belongs at the worker

The brief says "maximum 100 messages per second per channel." That cap is on **provider calls** — the actual external side effect — not on Postgres claims or Kafka publishes.

If the rate limiter sits at the dispatcher, it caps the rate of new claims. But during recovery from a worker outage, the worker has a backlog of Kafka messages and **no rate limit**. It would burn through the backlog as fast as Kafka delivers and the provider responds, easily exceeding 100/s and hammering the provider.

Putting the rate limiter at the worker — directly before the provider call — guarantees the cap holds **regardless of the upstream backlog state**. It is the only place where the rate limit can be enforced on the actual rate-limited resource (the provider).

### 4.4 Why backpressure is a single signal: consumer-group lag

With the rate limiter at the worker, the dispatcher needs a different mechanism to avoid runaway claiming during a worker outage. The natural signal is **Kafka consumer-group lag on `send.<channel>`**:

- Lag low → workers are keeping up → dispatcher claims at full speed (bounded only by `LIMIT $batch_size` per tick).
- Lag high → workers are falling behind → dispatcher trips a circuit breaker and pauses claiming.

The same signal makes the **reaper lag-aware**: when lag is high, the reaper skips resets so worker outages don't silently burn the attempt counter on notifications that never reached a worker.

This collapses three concerns — rate limiting, outage backpressure, and attempt-counter integrity — into one rule:

- **Rate limiting** is enforced at the worker (one signal: Redis tokens).
- **Backpressure and integrity** are enforced at the dispatcher and reaper (one signal: consumer lag).

### 4.5 Why Postgres is the source of truth and Kafka is only transport

The system has mutable state per notification (status transitions: `PENDING` → `DISPATCHED` → `DELIVERED` / retry / `FAILED`) and per attempt (raw response, classification, outcome). Mutable state requires query-by-id, update-in-place, and predicate queries — all native to a database, none native to Kafka. Kafka topics are append-only commit logs; they can carry events but cannot store state.

Putting state in Kafka would mean either projecting topics back into a database anyway (reinventing this design with extra ceremony) or committing to event sourcing (a much larger architectural commitment than this system has signed up for).

### 4.6 Why we use the transactional outbox pattern (and what it actually does)

The popular framing — "the outbox pattern eliminates the dual-write problem" — is a simplification. The outbox pattern does not eliminate dual-writes. It cannot: there is no atomic transaction spanning Postgres and Kafka.

What the outbox pattern actually does:

1. **Removes** dual-writes from every business-logic component. Each producer (api, dispatcher, worker, reaper) writes only to Postgres, atomically committing its state change and an outbox row in the same transaction.
2. **Concentrates** the unavoidable Postgres↔Kafka coordination in one component (the relay).
3. **Tames** the failure mode there: the relay uses publish-then-mark ordering, which guarantees at-least-once delivery (duplicates possible, losses impossible).
4. **Compensates** for duplicates by requiring all consumers to be idempotent.

The dangerous failure mode (a producer crashing between a state change and a Kafka publish, leaving the system in an inconsistent state that requires manual reconciliation) is gone. The remaining failure mode (the relay crashing between a Kafka publish and the outbox UPDATE, causing a duplicate publish on the next tick) is harmless: idempotent consumers no-op on duplicates.

### 4.7 Why we accept at-least-once delivery instead of seeking exactly-once

Atomic commit across Postgres and Kafka is not possible without distributed transactions (2PC, XA), which Kafka does not support and which are operationally fragile. Every real-world system using both Postgres and Kafka accepts at-least-once delivery and makes consumers idempotent.

This is not a workaround. It is the actual answer.

### 4.8 Why Postgres is a better priority queue than Kafka here

Kafka has no native priority — every "priority" implementation is a workaround (separate-topic-per-priority with weighted consumption, bucket priority pattern, KIP-349 which is unimplemented). Each workaround introduces consumer-side complexity, partition-rebalancing concerns, or starvation risk.

Postgres expresses priority natively: `ORDER BY priority DESC, eligible_at ASC`. One line, correct by construction, and combined into the same statement that does eligibility filtering and atomic claim. At our stated scale this is cheaper and more correct than the Kafka equivalent.

### 4.9 Why we still use Kafka given Postgres could carry the whole pipeline

A legitimate alternative architecture exists: replace every Kafka topic with Postgres state transitions and `LISTEN/NOTIFY`. No outbox, no relay, no Kafka — just rows changing status, with each stage polling for its trigger state.

That design would work at our scale. We retain Kafka because:

1. **The brief explicitly specifies an event-driven Kafka architecture.** Demonstrating the patterns (consumer groups, partitioning, outbox, idempotent consumers, lag monitoring) is part of what's being assessed.
2. **External fan-out** for `events.notification` (websocket, audit, analytics) needs pub/sub. Even in the Postgres-only design, this last topic would still exist.
3. **Independent worker scaling** is more natural with Kafka consumer groups than with Postgres polling.

---

## 5. Design principles

These principles are the rules we apply when we encounter ambiguous decisions later. They are not aspirational; each one earned its place through a specific trade-off.

### 5.1 Topics for events, tables for state

State has identity (look up by id), mutates over time, and is queryable by predicate. Events are immutable historical facts that happened in order. Kafka topics are for events; Postgres tables are for state. If we find ourselves wanting to look up "the current state of X" by reading a topic, we have put state where events should be.

Every topic in this system carries events. `Kafka(send.<channel>)` is "this notification is ready to send" — a command-event. `Kafka(events.notification)` is "this notification reached terminal state" — a fact-event for downstream consumers. No topic represents queue depth or current status.

### 5.2 Truth lives in Postgres

Every fact about a notification has exactly one authoritative location: the row(s) in `notifications` and `delivery_attempts`. Kafka messages are transient instructions or event records; if a Kafka message and a Postgres row appear to disagree, the row is correct.

### 5.3 Each producer writes to one system in one transaction

No producer (api, dispatcher, worker) performs a Postgres write and a Kafka publish in the same logical operation. State changes go to Postgres atomically alongside an outbox row. The relay is the only component that performs the Postgres↔Kafka bridge.

### 5.4 Rate limiting protects the side effect, not the pipeline

The rate limit lives where the rate-limited action happens (provider calls), not where the work is queued. Internal pipeline events are bounded only by the resources they consume; external side effects are bounded by the contractual rate limit.

### 5.5 Backpressure is a downstream signal, not an upstream guess

The dispatcher and reaper do not have to guess whether the worker is healthy. They consult **consumer-group lag**, which directly answers "are workers keeping up?" When lag is high, both pause. When lag drains, both resume. No timeouts, no health-check RPCs, no shared coordination state beyond Kafka itself.

### 5.6 Retries are scheduling

A failed notification is not a special object. It is a notification with a future `eligible_at` and an incremented `attempt`. The same dispatcher query that picks up brand-new notifications picks up retries. This collapses retry handling, scheduled-send handling, and burst-recovery handling into one mechanism.

### 5.7 Cancellation is an UPDATE

Cancellation flips `status` to `CANCELLED`. The dispatcher's `WHERE status='PENDING'` clause naturally excludes cancelled rows. There is no dedicated cancellation topic and no cancellation race against an in-flight Kafka message — except in the narrow window where a row is mid-pipeline (`DISPATCHED`). A cancellation against a `PENDING` row does emit one `events.notification` outbox row (current_status=`CANCELLED`, previous_status=`PENDING`) so downstream consumers see the terminal transition on the same topic as every other terminal outcome; a cancellation against a `DISPATCHED` row emits nothing, because the worker is about to resolve the attempt and any terminal event we emitted now would be a possibly-false claim about the realized outcome.

In that `DISPATCHED` window cancellation is genuinely **best-effort**. If the worker's Layer-1 state guard reads the row after the cancel API commits, it sees `status='CANCELLED'` and acks the message without calling the provider. If the cancel API commits after the state guard has already passed but before the worker's outcome transaction, the worker records the resolved outcome (`DELIVERED`, `FAILED`, or `PENDING` for a scheduled retry) and the attempt-guarded `UPDATE` overwrites `CANCELLED` with the realized status — the provider call is an irrecoverable external side effect, and a single status field cannot truthfully represent both "user cancelled" and "provider was called" at once. The state machine reflects what the worker resolved for that attempt rather than the user's last expressed wish. The API contract documents this best-effort semantics.

### 5.8 Failure recovery is automatic and idempotent

For every failure mode, the recovery path is code that runs continuously and is safe to re-run. Stuck claims are recovered by the lag-aware reaper. Duplicate Kafka messages are absorbed by the worker's state guard and `ON CONFLICT DO NOTHING`. Lost result events are republished by the relay automatically. No failure mode requires manual intervention.

### 5.9 Every failure has a defined disposition

No failure mode is allowed to result in unbounded retries, head-of-line blocking, or silent loss. Each error path either advances the notification toward a terminal state, schedules a future attempt, or routes the offending Kafka message to the dead-letter queue while terminating the notification. A reviewer should be able to ask "what happens if X fails?" for any X in the pipeline and find a defined answer in this document, not in the implementer's head.

---

## 6. Key mechanics

### 6.1 The outbox table

```sql
CREATE TABLE outbox (
  id            BIGSERIAL PRIMARY KEY,
  topic         TEXT NOT NULL,
  partition_key TEXT,
  headers       JSONB,
  payload       JSONB NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at  TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx
  ON outbox (id) WHERE published_at IS NULL;
```

A single shared table for all outbound Kafka messages from any producer. The `topic` column tells the relay where to publish. The partial index keeps the relay's working set small regardless of historical table size.

### 6.2 The dispatcher's claim-and-publish flow

Per channel, every poll tick (~100 ms):

1. Check Kafka consumer-group lag on `send.<channel>`. If lag exceeds the threshold → trip the in-process circuit breaker, skip this tick.
2. Otherwise, in one Postgres transaction, atomically claim eligible rows and insert one outbox row per claimed notification. The shape below shows the design intent; the executed SQL (`internal/store/notifications.go: ClaimDispatchable` + a per-row `InsertOutboxRow`) returns the claimed rows from the CTE and builds the JSON payload in Go so the payload schema is type-checked at compile time, but the atomicity (single tx, single commit) is identical:
  ```sql
   BEGIN;

   WITH claimed AS (
     UPDATE notifications
     SET status = 'DISPATCHED', attempt = attempt + 1
     WHERE id IN (
       SELECT id FROM notifications
       WHERE status = 'PENDING'
         AND channel = $1
         AND eligible_at <= now()
       ORDER BY priority DESC, eligible_at ASC
       FOR UPDATE SKIP LOCKED
       LIMIT $batch_size
     )
     RETURNING id, channel, recipient, content, template, template_data, priority, attempt
   )
   INSERT INTO outbox (topic, partition_key, payload)
   SELECT 'send.' || channel, id::text, jsonb_build_object(
     'version',       1,
     'id',            id,
     'attempt',       attempt,
     'channel',       channel,
     'recipient',     recipient,
     'content',       content,
     'template',      template,
     'template_data', template_data,
     'priority',      priority
   )
   FROM claimed;

   COMMIT;
  ```
   `updated_at` is maintained by the `notifications_set_updated_at` BEFORE-UPDATE trigger, so producers never write it explicitly.
3. The relay drains the outbox to Kafka asynchronously.

The dispatcher does **not** rate-limit. Backpressure comes from the lag check. Throughput is bounded by `LIMIT $batch_size` per tick and by how fast workers can drain `send.<channel>`.

### 6.3 The worker's flow

```
loop:
  msg = kafka.Poll('send.<channel>')

  # Layer 1 — State guard: skip stale messages from earlier attempts
  # (and terminal-state rows, e.g. cancelled before dispatch).
  state = SELECT status, attempt FROM notifications WHERE id = msg.id
  if state.status in {DELIVERED, FAILED, CANCELLED}:
      ack and continue   # terminal — earlier worker or cancel API resolved it
  if state.status != 'DISPATCHED' or state.attempt != msg.attempt:
      ack and continue   # superseded — reaper reset and dispatcher re-claimed

  # Layer 2 — Idempotency token (separate, auto-committed tx). Runs
  # BEFORE the rate-limit acquire so that a Kafka redelivery duplicate
  # is rejected here cheaply, without burning a token.
  inserted = INSERT INTO delivery_attempts (notification_id, attempt, started_at)
             VALUES (msg.id, msg.attempt, $worker_clock)
             ON CONFLICT DO NOTHING
  if not inserted:
      ack and continue   # another worker instance already claimed this attempt

  # Rate limit at the side effect — directly before the provider call.
  ok, wait_ms = redis.TakeToken('rate:<channel>')
  while not ok:
      sleep(wait_ms + jitter)
      ok, wait_ms = redis.TakeToken('rate:<channel>')

  # External side effect
  response = provider.Send(msg)
  classification = classify(response)   # success / transient / permanent

  # Compute new_status / new_eligible_at in code so the SQL stays
  # parameter-shaped:
  #   success   → DELIVERED, eligible_at carried as $now (terminal; forensic only)
  #   permanent → FAILED,    failure_reason=permanent_error
  #   transient & attempt < max → PENDING,
  #              eligible_at = $now + equal_jitter(2^attempt seconds),
  #              i.e. uniform draw from [det/2, det]
  #   transient & attempt >= max → FAILED, failure_reason=max_attempts_exceeded

  # Tx B — Layer 3: attempt-guarded UPDATE + outbox emit, atomically.
  BEGIN;
    UPDATE delivery_attempts
       SET response       = $resp,
           classification = $cls,
           error_message  = $err,
           finished_at    = $worker_clock
       WHERE notification_id = msg.id AND attempt = msg.attempt;

    UPDATE notifications
       SET status         = $new_status,
           eligible_at    = $new_eligible_at,
           failure_reason = $new_failure_reason
       WHERE id = msg.id AND attempt = msg.attempt;

    INSERT INTO outbox (topic, partition_key, payload)
    VALUES ('events.notification', msg.id::text, jsonb_build_object(
      'version',         1,
      'id',              msg.id,
      'batch_id',        null,                  -- see note below
      'channel',         msg.channel,
      'attempt',         msg.attempt,
      'previous_status', 'DISPATCHED',
      'current_status',  $new_status,
      'classification',  $cls,
      'failure_reason',  $new_failure_reason,
      'occurred_at',     $worker_clock
    ));
  COMMIT;

  ack
```

`updated_at` on `notifications` is again written by the `notifications_set_updated_at` BEFORE-UPDATE trigger and never set explicitly.

`batch_id` on the worker's `events.notification` payload is always `null`. The `send.<channel>` payload (§6.2) does not carry `batch_id`, and the worker does not re-read the row to fetch it; consumers correlate batches via `id` alone (or via `GET /v1/batches/{id}` against the api). The cancel path (§5.7) and the reaper's terminal-fail path (§6.5) both have the row in scope and do propagate `batch_id`, so a downstream consumer of `events.notification` sees `batch_id` populated only on those two emit sites. Lifting `batch_id` into the worker's emit (either by widening the send payload or by adding a row-read to the worker) is straightforward future work.

Three idempotency mechanisms cooperate, each catching a different duplicate-source. They run in this order: state guard → `delivery_attempts` INSERT → rate-limit acquire → provider call → attempt-guarded UPDATE.

- **Layer 1 — State guard** before any state-mutating work: skips messages whose target row has reached a terminal state (`DELIVERED`, `FAILED`, `CANCELLED`) or whose attempt has been superseded (e.g. by the reaper resetting and the dispatcher re-claiming during a worker outage). A stale read here is safe — Layers 2 and 3 are the actual race-safety mechanisms; Layer 1 is a cheap short-circuit.
- **Layer 2 — `INSERT ... ON CONFLICT DO NOTHING` on `delivery_attempts`**: prevents two worker instances from both calling the provider for the same `(notification_id, attempt)` after a Kafka redelivery. Runs in its own auto-committed transaction so the conflict signal is visible to a peer instance immediately. Crucially this runs **before** the rate-limit acquire so that a duplicate is rejected without burning a token; the rate limiter sits "directly before the provider call" once Layer 2 has authorized this worker to make the call.
- **Layer 3 — Attempt-guarded UPDATE on `notifications`** (`WHERE id=msg.id AND attempt=msg.attempt`): prevents a slow or stale worker from clobbering a row whose attempt has been superseded — for example, when the reaper resets a slow worker's row and the dispatcher claims a new attempt while the original worker is still inside the provider call. The guard is on `attempt` only, not on `status`. Cancellation is **not** enforced here: if a cancellation arrived mid-flight and our attempt is still current, the worker's outcome wins and `status` reflects what actually happened to the message (per §5.7).

If during initial validation — before Layer 2's `INSERT` into `delivery_attempts` — the worker determines the message itself is unprocessable (corrupt JSON, envelope schema mismatch, missing required fields, decoding panic), it takes the dead-letter disposition described in §6.7 instead of calling the provider. Past that `INSERT` the worker is committed to producing a provider-derived outcome; structural defects discovered after that point are classified as `permanent`, with forensic detail captured in `delivery_attempts.error_message`.

### 6.4 The relay loop

```
loop forever:
  BEGIN
    SELECT id, topic, partition_key, payload
    FROM outbox
    WHERE published_at IS NULL
    ORDER BY id LIMIT 500
    FOR UPDATE SKIP LOCKED;

    for each row:
      kafka.Publish(row.topic, row.partition_key, row.payload)

    UPDATE outbox SET published_at = now() WHERE id = ANY($1)
  COMMIT
  sleep 50ms
```

**Ordering matters**: publish-then-mark, never mark-then-publish. Mark-then-publish would risk losing messages on relay crash; publish-then-mark risks at most a duplicate, which idempotent consumers handle.

`FOR UPDATE SKIP LOCKED` allows multiple relay instances to run safely for redundancy: they skip each other's locked rows rather than fighting for them.

### 6.5 The reaper

Runs every minute. Stuck threshold is `$stuck_seconds` (production default 120 s — long enough that a healthy worker mid-provider-call is not eligible, short enough that a dead worker's claim is recovered quickly).

```sql
-- 1. Skip the whole reap cycle if any send.<channel> consumer is far behind,
--    or if lag cannot be determined (broker unreachable, admin API error,
--    coordinator unavailable). Both branches fail-closed: skip the cycle.
--    (Pseudo-code: kafka admin call to get max lag across consumer groups.)
IF lag_query_failed OR max_consumer_lag > $lag_threshold THEN
    RETURN;
END IF;

-- 2. Reset stuck rows to PENDING with backoff. The deterministic
--    backoff floor lives in SQL; the loop then runs a second pass in
--    application code that overwrites eligible_at with the equal-jitter
--    value: uniform draw from [det/2, det] where
--    det = 2^min(attempt, $reaper_backoff_cap) seconds. Equal jitter
--    spreads contending resets so a herd of stuck rows does not all
--    re-claim at the same instant. The post-pass UPDATE is guarded by
--    status='PENDING' so a row the dispatcher has already re-claimed
--    between the two statements is left alone.
UPDATE notifications
SET status      = 'PENDING',
    eligible_at = now() + (interval '1 second' * pow(2, least(attempt, $reaper_backoff_cap)))
WHERE status      = 'DISPATCHED'
  AND updated_at <  now() - ($stuck_seconds * interval '1 second')
  AND attempt    <  $max_attempts
RETURNING id, attempt;   -- fed into the equal-jitter post-pass

-- 3. Terminal-fail rows that have exhausted attempts, and emit one
--    events.notification outbox row per terminated row (same tx).
WITH terminated AS (
  UPDATE notifications
  SET status         = 'FAILED',
      failure_reason = 'max_attempts_exceeded'
  WHERE status      = 'DISPATCHED'
    AND updated_at <  now() - ($stuck_seconds * interval '1 second')
    AND attempt    >= $max_attempts
  RETURNING id, batch_id, channel, attempt
)
INSERT INTO outbox (topic, partition_key, payload)
SELECT 'events.notification', id::text, jsonb_build_object(
  'version',         1,
  'id',              id,
  'batch_id',        batch_id,
  'channel',         channel,
  'attempt',         attempt,
  'previous_status', 'DISPATCHED',
  'current_status',  'FAILED',
  'classification',  NULL,
  'failure_reason',  'max_attempts_exceeded',
  'occurred_at',     now()
)
FROM terminated;
```

`updated_at` is again maintained by the `notifications_set_updated_at` trigger.

The lag check is what distinguishes "the worker is dead and we need to recover stuck claims" from "the worker is fine but slow; let it catch up." Without the check, a multi-minute worker outage would mark thousands of notifications `FAILED` despite zero actual delivery attempts.

Lag-query failure (broker unreachable, admin API errors, coordinator unavailable) is treated the same as lag-above-threshold: skip the cycle. This protects attempt-counter integrity during Kafka outages — if the reaper kept resetting `DISPATCHED` rows during a multi-minute Kafka outage, the dispatcher (which fail-opens; see §6.9) would re-claim each one and burn an attempt per cycle for messages that never reach a worker. Stuck rows wait out the outage and recover naturally when the broker returns and the outbox drains.

### 6.6 The rate limiter

A Redis token bucket per channel: `rate:sms`, `rate:email`, `rate:push`. Each bucket has refill rate **100 tokens/sec** and burst capacity **100 tokens**.

```lua
-- rate_limit.lua  (atomic in Redis)
local key      = KEYS[1]
local rate     = tonumber(ARGV[1])    -- 100
local capacity = tonumber(ARGV[2])    -- 100
local now_ms   = tonumber(ARGV[3])

local data    = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens  = tonumber(data[1]) or capacity
local last    = tonumber(data[2]) or now_ms

local elapsed_ms = math.max(0, now_ms - last)
tokens = math.min(capacity, tokens + elapsed_ms * rate / 1000)

if tokens >= 1 then
  tokens = tokens - 1
  redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now_ms)
  redis.call('EXPIRE', key, 60)
  return {1, 0}                       -- ok, no wait
else
  local wait_ms = math.ceil((1 - tokens) * 1000 / rate)
  redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now_ms)
  redis.call('EXPIRE', key, 60)
  return {0, wait_ms}                 -- wait this many ms before retrying
end
```

The script is atomic in Redis, so multiple worker instances share the bucket safely. When a worker can't get a token, it sleeps for the script-returned wait_ms (plus small jitter) and retries.

**Failure mode (Redis down)**: the worker's Redis call times out → it cannot rate-limit safely → it pauses processing. Optional: a local in-process bucket per worker as a fallback at a tighter cap (e.g. 100 / N for N workers), accepting brief over-rate during Redis outage.

### 6.7 Unprocessable messages and the dead-letter queue

Some Kafka messages cannot be processed at all — corrupt JSON, schema mismatch after a deploy, missing required fields, a panic on a specific payload shape. Without a defined disposition, the worker would fail to advance its consumer-group offset, and the bad message would block every subsequent message in the same partition until human intervention. This is **head-of-line blocking** and it is unacceptable for a system whose contract is "reliable delivery."

The disposition is a per-channel **dead-letter queue**: a separate Kafka topic (`send.<channel>.dlq`) that stores unprocessable messages for later inspection or replay. Crucially, this happens atomically with the notification's terminal-fail in Postgres, so the database remains the single source of truth and the corrupt message does not silently linger as a `DISPATCHED` row.

When the worker classifies a message as unprocessable and a target row can be identified, it runs one Postgres transaction (the "targeted" branch). The `ON CONFLICT DO NOTHING` on `delivery_attempts` keeps a Kafka redelivery of the same corrupt message harmless — without it, the redelivery's INSERT would hit the `(notification_id, attempt)` primary key and roll back the whole transaction, leaving the offset uncommitted forever:

```sql
BEGIN;

INSERT INTO delivery_attempts (
  notification_id, attempt, started_at, finished_at,
  classification, error_message
)
VALUES (msg.id, msg.attempt, $worker_clock, $worker_clock,
        'unprocessable', $err_code || ': ' || $err_details)
ON CONFLICT (notification_id, attempt) DO NOTHING;

UPDATE notifications
SET status         = 'FAILED',
    failure_reason = 'unprocessable_message'
WHERE id = msg.id AND attempt = msg.attempt;   -- attempt-guarded (Layer 3)

INSERT INTO outbox (topic, partition_key, payload)
VALUES ('send.' || channel || '.dlq', msg.id::text, jsonb_build_object(
  'version',          1,
  'notification_id',  msg.id,
  'channel',          channel,
  'attempt',          msg.attempt,
  'original_message', $rec_value_json,        -- or original_message_raw (base64) on decode_failed
  'error',            $err_code,
  'error_details',    $err_details,
  'failed_at',        $worker_clock
));

INSERT INTO outbox (topic, partition_key, payload)
VALUES ('events.notification', msg.id::text, jsonb_build_object(
  'version',         1,
  'id',              msg.id,
  'channel',         channel,
  'attempt',         msg.attempt,
  'previous_status', 'DISPATCHED',
  'current_status',  'FAILED',
  'classification',  'unprocessable',
  'failure_reason',  'unprocessable_message',
  'occurred_at',     $worker_clock
));

COMMIT;
```

Then the worker acks the original Kafka message, the offset advances, and the next message is processed. The DLQ entry exists for debugging; the notification's row reflects truth (subject to the attempt-guarded UPDATE matching); downstream consumers are notified of the terminal state. No part of the system is blocked, no message is lost, no row is left in limbo.

The DLQ publish reuses the same shared `outbox` table and the same relay as every other Kafka topic — the only thing that distinguishes a DLQ row from a regular send row is the value in the `topic` column. No extra infrastructure.

**Edge case: the message is too corrupt to identify a notification.** If the payload's `id` / `attempt` fields are missing or unparseable, the worker cannot guard the `UPDATE` on a specific row. In that case the message goes to the DLQ alone, with a metric incremented and a log line emitted; any orphaned `DISPATCHED` row will eventually `FAIL` via the reaper after the attempt counter saturates. The notification ID rides on the original Kafka **record key** (the dispatcher writes it as `partition_key` on the outbox row, which the relay forwards as `kgo.Record.Key`), so an operator inspecting the source `send.<channel>` partition can recover the routing-level identifier even when the structured `id` is unrecoverable. The DLQ payload itself does not currently surface that fallback — it stores the original message body but not the original record key — so cross-correlating to the notification id requires reading the source partition's record alongside the DLQ entry. Lifting the record-key fallback into the DLQ payload is straightforward future work.

**Replay**: the DLQ is a regular Kafka topic. A small operator tool (or an admin-only API endpoint) can consume from it and republish individual messages back onto `send.<channel>` after the underlying issue is fixed — for example, after a code deploy that handles a previously-unhandled payload shape. The worker's state guard ensures replays don't double-deliver: a notification already in `FAILED` state will be skipped.

**Retention**: DLQ topics keep messages on a longer window than the main pipeline (e.g. 30 days) because they exist precisely for human-paced investigation rather than real-time processing.

### 6.8 Behavior under a worker outage


| Component    | During outage                                                                                                          |
| ------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `api`        | Healthy. Continues accepting requests, persisting to Postgres, returning `201 Created`.                                |
| `dispatcher` | Detects high lag on `send.<channel>` → trips circuit breaker → pauses claiming. Backlog of `PENDING` rows accumulates. |
| `relay`      | Healthy. Backlog from before the lag spike drains to Kafka. `outbox` shrinks.                                          |
| `worker`     | Down.                                                                                                                  |
| `reaper`     | Detects high lag → skips reset cycle. Stuck `DISPATCHED` rows stay as-is. **Attempt counters are not burned.**         |
| `Postgres`   | Healthy. `PENDING` backlog grows linearly with submission rate.                                                        |


**On recovery**: workers reconnect to their consumer group, consumer-group lag begins to drop as messages are processed (gated by the worker's rate limiter at 100/s). The dispatcher's breaker waits for lag to drop below threshold, then resumes claiming. The reaper resumes its normal cycle. Some duplicate Kafka messages may still exist (created during the brief window before the dispatcher's breaker tripped); the worker's state guard skips them.

**What survives**: all submitted notifications (durable in Postgres), the API's availability, attempt-counter integrity, system-wide data consistency.

**What suffers**: end-to-end delivery latency for notifications submitted during the outage, by approximately the outage duration plus drain time at 100/s.

### 6.9 Behavior under a Kafka outage


| Component    | During outage                                                                                                                                                                                                               |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `api`        | Healthy.                                                                                                                                                                                                                    |
| `dispatcher` | Healthy. Continues claiming; `outbox` grows. (Lag is undefined when the broker is unreachable; the breaker treats unreachable-broker as "do not pause" so claiming continues for as long as Postgres can hold the backlog.) |
| `relay`      | Publish attempts fail. Rows stay unpublished. Relay backs off with exponential delay between attempts.                                                                                                                      |
| `worker`     | Idle but healthy. No messages to consume.                                                                                                                                                                                   |
| `reaper`     | Lag is unqueryable → fail-closes (skips the cycle, same as lag-above-threshold; see §6.5). Stuck `DISPATCHED` rows stay as-is through the outage and recover naturally when the broker returns and the outbox drains. **Attempt counters are not burned.** |
| `Postgres`   | Healthy. `outbox` and `PENDING` tables grow linearly with submission rate.                                                                                                                                                  |


**On recovery**: relay resumes, drains `outbox`, workers consume, system catches up.

---

## 7. Trade-offs accepted


| Trade-off                                               | What we gave up                                                                                                                                                                    | Why we accepted it                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A dedicated relay process                               | One more process to deploy and monitor                                                                                                                                             | Eliminates producer-side dual-writes. The relay is small, single-purpose, and can run with multiple instances for HA via `FOR UPDATE SKIP LOCKED`.                                                                                                                                                                                                                                                   |
| Single shared outbox vs per-stage outboxes              | Cannot have per-stage indexes or per-stage isolation by default                                                                                                                    | Equivalent observability via `WHERE topic LIKE ...`; equivalent index size via partial index. Operationally simpler. Easy to split later if one topic dominates.                                                                                                                                                                                                                                     |
| Worker holds the rate-limit token, not the dispatcher   | Dispatcher claims unboundedly between lag checks                                                                                                                                   | This is the only place the rate limit can be enforced on the actual rate-limited resource. The lag-based circuit breaker bounds claims indirectly via worker drain rate.                                                                                                                                                                                                                             |
| Backpressure is consumer-group lag, not a custom signal | Slight coupling between dispatcher and Kafka admin API                                                                                                                             | Lag is the operationally meaningful signal, already exported by every Kafka client. Avoids inventing a separate health-check protocol.                                                                                                                                                                                                                                                               |
| At-least-once delivery (relay's residual dual-write)    | Cannot guarantee exactly one Kafka message per state transition                                                                                                                    | Standard industry pattern. Idempotent consumers absorb duplicates. The alternative (2PC/XA) is operationally fragile and not supported by Kafka.                                                                                                                                                                                                                                                     |
| Postgres holds the burst backlog instead of Kafka       | A long sustained burst grows the `notifications` table large enough that vacuum and index maintenance cost real CPU                                                                | At the brief's stated scale, Postgres with partial indexes is well inside its comfort zone. Backlog visibility, queryability, and admin-action benefits outweigh the operational cost.                                                                                                                                                                                                               |
| Redis as a hard dependency for the rate limiter         | If Redis is down, the worker pauses processing                                                                                                                                     | The rate limiter is the single most stateful coordination primitive in the system. The defined disposition on Redis-down is to pause processing (the worker sleeps a short backoff and lets Kafka redeliver); a local in-process bucket at a tighter cap (e.g. 100 / N for N workers) is a documented future option, not currently implemented.                                                       |
| Polling-based dispatcher and relay have a latency floor | Notifications cannot be delivered faster than `dispatcher_interval + relay_interval + provider_RTT` (~150ms typical)                                                               | Within bounds for the brief's use cases. `LISTEN/NOTIFY` is a documented optimization path.                                                                                                                                                                                                                                                                                                          |
| Cancellation after dispatch is best-effort              | A notification cancelled while `DISPATCHED` may still call the provider, and its terminal status will reflect the actual outcome (`DELIVERED` / `FAILED`) rather than `CANCELLED`. | The provider call is an irrecoverable external side effect; a single status field cannot truthfully represent both "user cancelled" and "provider was called" at once. The state machine reflects reality. The same attempt-guard semantics apply on the DLQ path: a cancellation racing with an unprocessable-message disposition is also overwritten by `FAILED` if the attempt still matches. The asymmetry, instead, is at Layer 1: a cancellation that lands before the worker's state guard reads the row is honored cleanly (the worker acks without calling the provider). Documented in the API contract. |


---

## 8. Glossary

- **Mode**: a runtime role of the single binary, selected by subcommand (`./notifications api`, `./notifications worker --channel=sms`, etc.).
- **Eligible**: a notification whose `eligible_at <= now()` and `status = 'PENDING'`. The dispatcher considers eligible rows for claiming.
- **Claim**: the atomic operation that transitions a row from `PENDING` to `DISPATCHED`, taking ownership for delivery.
- **Outbox**: the single shared table of rows describing Kafka messages to be published. Inserted by producers in the same transaction as their state change. Drained by the relay.
- **Relay**: the process that drains `outbox` to Kafka. The only component performing the Postgres↔Kafka bridge.
- **Reaper**: the recurring SQL operation that resets rows stuck in `DISPATCHED` past a timeout back to `PENDING` (or terminal `FAILED` when attempts are exhausted). Lag-aware.
- **Channel**: a delivery medium (sms, email, push), each with its own provider, worker pool, send topic, and rate-limit bucket.
- **Provider**: the external service that performs the actual delivery (webhook.site for the assessment; Twilio, SES, FCM, etc., in production).
- **State guard**: the worker's Layer-1 pre-call check that the message's `attempt` matches the row's current `attempt` and the row is still `DISPATCHED`. Skips superseded messages and rows that have reached a terminal state (`DELIVERED`, `FAILED`, `CANCELLED`).
- **Token bucket**: the Redis rate-limiter primitive. One bucket per channel, refilled at the channel's per-second rate, drawn from one token per provider call.
- **Consumer-group lag**: the difference between the latest published offset on `send.<channel>` and the worker consumer group's committed offset. The single signal driving dispatcher and reaper backpressure.
- **Idempotent consumer**: a Kafka consumer whose effect is the same whether it receives a message once or many times. Enforced via state guard + `ON CONFLICT DO NOTHING` + guarded UPDATE.
- **At-least-once delivery**: the guarantee that every produced message will eventually be delivered to consumers, possibly more than once. The accepted contract for this system.
- **Dead-letter queue (DLQ)**: a per-channel Kafka topic (`send.<channel>.dlq`) holding messages the worker cannot process (corrupt payload, schema mismatch, missing required fields). Routed via the same shared `outbox` table as every other Kafka topic, atomically with the corresponding notification's transition to `FAILED`. Exists to prevent head-of-line blocking and to preserve the bad message for human investigation.
- **Head-of-line blocking**: the failure mode in which an unprocessable message at the head of a Kafka partition prevents every subsequent message from being consumed. Prevented in this system by the DLQ disposition.
