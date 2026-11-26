# Resumable webhook delivery for a SaaS integration customer

A small, self-contained implementation of the requirement, in Python + FastAPI
+ SQLite, with a **separate delivery process** and a controllable local HTTP
receiver for the demo.

```
┌────────────────┐   POST /events     ┌──────────────┐   claim (lease)   ┌────────────────────┐
│ customer /     │ ─────────────────▶ │  FastAPI     │ ◀──────────────── │  worker process(es)│
│ caller service │                    │  ingest API  │                   │  python -m …worker │
└────────────────┘ ◀── 200/replay/409 └──────┬───────┘                   └─────────┬──────────┘
                                              │ SQLite (WAL, BEGIN IMMEDIATE)      │ POST event
                                              ▼                                    ▼
                                       webhook_delivery.db                  receiver (2xx = ok)
```

Everything is in `webhook_delivery/`:

| file | role |
|---|---|
| `api.py` | FastAPI app: customers, subscriptions, event ingestion, read models, admin retry/skip |
| `store.py` | All state transitions, the only place that writes SQL; optimistic concurrency rules live here |
| `worker.py` | Standalone delivery process (`python -m webhook_delivery.worker`), leases, retries |
| `config.py` | `Settings` (back-offs, timeouts, lease, max attempts) and `Clock` / `FakeClock` |
| `receiver.py` | Controllable `http.server` receiver used by the demo and tests |
| `util.py` | IDs, canonical JSON, content hashing, URL validation, errors |

`demo.py` is the independent end-to-end demo; `tests/` holds the deterministic
suite (43 tests, ~8 s, no real waiting).

## Run it

```bash
./run_demo.sh     # installs deps into ./.pylibs on first run, then runs the demo
./run_tests.sh    # same bootstrap, then pytest
```

`run_demo.sh` starts an API subprocess, several worker subprocesses, and two
local receivers; it prints each assertion as it is verified.

## HTTP API

| method | path | notes |
|---|---|---|
| `POST` | `/customers` | `{name}` → `{id, name, created_at}` |
| `POST` | `/subscriptions` | `{customer_id, target_url}` → subscription; only events accepted **after** creation fan out to it |
| `GET` | `/subscriptions/{id}` | includes current `target_url` and `target_version` |
| `PATCH` | `/subscriptions/{id}` | `{customer_id, target_url, expected_target_version?}`; bumps `target_version` |
| `POST` | `/events` | `{customer_id, flow_id, caller_event_id, payload}`; returns the event, its per-flow `seq`, and one delivery per subscription |
| `GET` | `/events?customer_id&flow_id` | event log |
| `GET` | `/subscriptions/{id}/deliveries` | per-delivery status, target version, blocking reason |
| `GET` | `/deliveries/{delivery_id}` | full detail: every attempt with its target URL/version, admin actions, late results |
| `POST` | `/deliveries/{delivery_id}/retry` | `{reason, actor}`; only for `needs_attention` |
| `POST` | `/deliveries/{delivery_id}/skip` | `{reason, actor}`; reason required; unblocks the flow |

Errors are `{"error": {"code", "message"}}` with codes such as
`caller_event_conflict` (409), `invalid_target_url` (400),
`target_version_conflict` (409), `invalid_state` (409), `reason_required` (422).

## Semantics as implemented

**Event identity.** An event is unique on `(customer_id, flow_id,
caller_event_id)`. Re-posting the same triple with the same canonical JSON
returns the original event (`replayed: true`, same `id`, same `seq`, no new
deliveries). Re-posting it with different content returns 409 and creates
nothing. The same caller ID in a *different* flow is a different event.

**Sequence.** `seq` is allocated per `(customer_id, flow_id)` inside the same
`BEGIN IMMEDIATE` transaction that inserts the event, so concurrent ingestions
cannot collide. Subscription visibility uses a separate global ordinal, which
avoids "same clock tick" ambiguity when deciding whether an event happened
before or after a subscription was created.

**Per-subscription ordering.** A delivery is claimable only when every earlier
`seq` for the same `(subscription, flow)` is terminal (`succeeded` or
`administratively_skipped`). Different flows never wait on each other — in the
demo, `flow-b` completes while `flow-a` is backing off. A delivery is also
stored with `blocked_by`/`block_reason` so a blocked item's reason is queryable.

**Retries.** HTTP 2xx = success. Non-2xx (including 3xx — redirects are not
followed), timeouts, and connection errors are failures. Waits after failure
number 1, 2, 3 are 1 s, 4 s, 16 s; the 4th failure parks the delivery in
`needs_attention` and the block is never quietly released. An administrator can
retry it (fresh retry budget, same `delivery_id`) or skip it with a reason,
which releases the rest of the flow. The store enforces the budget itself; a
caller cannot pass a delay that would exceed `max_attempts`.

**delivery_id.** Allocated when the delivery is created and never changes —
across retries, lease recovery, and target changes. It is sent in the body and
as `X-Webhook-Delivery-Id` (plus `X-Webhook-Event-Id`, `X-Webhook-Flow-Id`,
`X-Webhook-Seq`, `X-Webhook-Target-Version`). Delivery is **at-least-once**:
a crash can produce the same `delivery_id` twice, and the receiver is expected
to de-duplicate on it. Nothing here claims network exactly-once.

**Two delivery processes.** Claiming is a single `BEGIN IMMEDIATE`
transaction that tests status, due time, predecessor terminality, and lease
expiry, then writes a fresh `lease_token`. Finalizing an attempt is guarded by
that token: a worker whose lease expired cannot commit a result, positively or
negatively. Such late replies are recorded in `late_results` for observability
and otherwise ignored (`test_stale_worker_cannot_overwrite_newer_result`,
`test_stale_success_cannot_override_newer_failure`).

**Changing the target.** Each attempt snapshots the target URL and subscription
`target_version`. On `PATCH`:
* succeeded deliveries are untouched ("already delivered is not resent");
* not-yet-succeeded work is repointed at the new URL;
* an in-flight attempt keeps its old URL snapshot, so when its reply arrives
  the version check fails and the result is recorded as `superseded_target` —
  a late 200 from the old address can never mark the new address's work as
  delivered. The delivery is re-queued against the new address and the
  superseded attempt is refunded from the retry budget.

**Crash recovery.** A lease that expires without a recorded result is marked
`expired` (outcome unknown) and the work is re-queued; a restarted worker
picks it up (`test_worker_restart_after_crash_retries_inflight_attempt`).
Scheduled retries live in the database, so a restart resumes them rather than
losing them (`test_worker_restart_picks_up_waiting_work`).

## Time control — no long waits

Two independent mechanisms:

1. **Injection (`FakeClock`)** — `tests/test_delivery.py` asserts the exact
   1 s / 4 s / 16 s gaps by advancing a fake clock, plus the 4th-failure
   parking. Nothing sleeps.
2. **CLI compression** — the demo runs workers with
   `--backoff 0.1,0.4,1.6 --request-timeout 0.5`, so real subprocesses,
   sockets, and a real SIGKILL complete in seconds. The same flags exist for
   manual experimentation with the production schedule.

## What the demo verifies

1. Pre-subscription events produce no deliveries; replay returns the original;
   changed content is rejected; per-flow `seq` increments.
2. 500 → 502 → timeout → 200: four attempts, compressed waits matching the
   back-off, stable `delivery_id` on every request, and `flow-b` completing
   while `flow-a` is still retrying.
3. Four failures park the delivery; the successor is blocked and never sent;
   admin retry re-queues it; admin skip (with reason) releases the successor.
4. Target switch: old endpoint answers 200 *late* → recorded as
   `superseded_target`; only the new endpoint's 200 succeeds.
5. SIGKILL mid-attempt → restart → `expired` attempt, retry with the same
   `delivery_id`; receiver de-duplicates on it.

## Limits / deliberate non-goals

* SQLite + WAL is the concurrency boundary; this is designed for one
  deployment sharing one database file, not a sharded multi-region fleet.
* No authentication/authorization — the API is a local demo surface.
* No admin UI; admin operations are HTTP endpoints.
* Ordering is strictly per `(subscription, flow)`. There is no global order,
  by design.
* `needs_attention` deliveries stay parked across target changes; an
  administrator must still explicitly retry them (they will then use the new
  target).
