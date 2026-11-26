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
| `api.py` | FastAPI app: customers, erasure receivers, subscriptions, event ingestion, topic withdrawal, erasure progress, tickets/evidence, read models, admin retry/skip |
| `store.py` | All state transitions, the only place that writes SQL; optimistic concurrency, withdrawal, erasure obligations/requests and retention rules live here |
| `worker.py` | Standalone delivery process (`python -m webhook_delivery.worker`), leases, send gate, retries; also claims and retries the independent erasure work queue |
| `cli.py` / `__main__.py` | `python -m webhook_delivery …` operations CLI for existing databases (withdraw / tickets / evidence) |
| `config.py` | `Settings` (back-offs, timeouts, lease, max attempts) and `Clock` / `FakeClock` |
| `receiver.py` | Controllable `http.server` receiver used by the demo and tests |
| `erasure_receiver.py` | Reference *participating* receiver: its own persistent library of managed copies, honours erasure requests with scope-isolation, a pre-withdrawal seal and idempotent acknowledgement |
| `util.py` | IDs, canonical JSON, content hashing, URL validation, errors |

`demo.py` is the delivery end-to-end demo; `demo_retraction.py` is the
topic-withdrawal / ticket-retention demo (`./run_retraction_demo.sh`);
`demo_erasure.py` is the external-replica erasure demo
(`./run_erasure_demo.sh`). `tests/` holds the deterministic suite (111 tests,
~30 s including real-socket, thread-contention and reference-receiver cases, no
long waits).

## Run it

```bash
./run_demo.sh              # installs deps into ./.pylibs on first run, then runs the delivery demo
./run_retraction_demo.sh   # self-checking topic-withdrawal/evidence demo
./run_erasure_demo.sh      # self-checking external-replica erasure demo
./run_tests.sh             # same bootstrap, then pytest
```

`run_demo.sh` starts an API subprocess, several worker subprocesses, and two
local receivers; it prints each assertion as it is verified.

## HTTP API

| method | path | notes |
|---|---|---|
| `POST` | `/customers` | `{name}` → `{id, name, created_at}` |
| `POST` | `/subscriptions` | `{customer_id, target_url, erasure_receiver_id?}` → subscription; only events accepted **after** creation fan out to it |
| `GET` | `/subscriptions/{id}` | includes current `target_url`, `target_version` and `erasure_receiver_id` |
| `PATCH` | `/subscriptions/{id}` | `{customer_id, target_url, expected_target_version?, erasure_receiver_id?, clear_erasure_receiver?}`; bumps `target_version` |
| `POST` | `/erasure-receivers` | `{customer_id, url}` → participating external receiver (fixed address); `201` |
| `GET` | `/erasure-receivers?customer_id=` · `/erasure-receivers/{id}` | registered erasure receivers |
| `GET` | `/topics/{topic_id}/erasure?customer_id=` | this withdrawal's remote-erasure confirmation progress |
| `GET` | `/erasure-requests?customer_id&status` · `/erasure-requests/{id}` | durable erasure work items |
| `POST` | `/erasure-requests/{id}/retry` | `{reason, actor}`; re-queue a parked erasure (same operation id) |
| `POST` | `/events` | `{customer_id, flow_id, caller_event_id, payload, topic_id?}`; returns the event, its per-flow `seq`, and one delivery per subscription |
| `GET` | `/events?customer_id&flow_id&topic_id` | event log; withdrawn topics return tombstones (`payload`/`content_hash` are `null`) |
| `GET` | `/events/{event_id}` | one event read model (content-safe after withdrawal) |
| `POST` | `/topics/{topic_id}/withdraw` | `{customer_id}` (or `?customer_id=`); idempotent irreversible withdrawal; `{"wait_for_inflight": true}` optionally waits for this topic's in-flight sends |
| `POST` | `/topics/withdraw` · `POST /withdrawals` · `DELETE /topics/{topic_id}` | aliases for the same operation |
| `GET` | `/topics?customer_id=` · `GET /topics/{topic_id}?customer_id=` | topic status |
| `POST` | `/tickets` | `{customer_id, title?, reason?}` → investigation ticket (`201`) |
| `GET` | `/tickets?customer_id=` · `GET /tickets/{id}` | ticket metadata incl. retained event ids and (open only) evidence |
| `POST` | `/tickets/{id}/events` | `{event_id}` or `{event_ids:[…]}`; retains received, non-withdrawn events (idempotent) |
| `GET` | `/tickets/{id}/events` | preserved evidence with original `payload`/hash; `409 ticket_closed` after closure |
| `POST` | `/tickets/{id}/close` | closes the ticket and purges its private evidence copy |
| `POST` | `/events/{event_id}/retain` · `POST /retentions` | retain from the event side / create-and-retain in one call |
| `GET/POST` | `/cases…`, `/holds…`, `/work-orders…` | vocabulary aliases for the ticket endpoints (`…/evidence`, `…/retain`, `…/close`) |
| `GET` | `/subscriptions/{id}/deliveries` | per-delivery status, target version, blocking reason |
| `GET` | `/deliveries/{delivery_id}` | full detail: every attempt with its target URL/version, admin actions, late results |
| `POST` | `/deliveries/{delivery_id}/retry` | `{reason, actor}`; only for `needs_attention` |
| `POST` | `/deliveries/{delivery_id}/skip` | `{reason, actor}`; reason required; unblocks the flow |

Errors are `{"error": {"code", "message"}}` with codes such as
`caller_event_conflict` (409), `invalid_target_url` (400),
`target_version_conflict` (409), `invalid_state` (409), `reason_required` (422),
`topic_withdrawn` / `event_withdrawn` / `ticket_closed` /
`delivery_withdrawn` (409), `ticket_not_found` / `event_not_found` (404).

## Topic withdrawal & ticket evidence retention

A customer can tag an event at ingestion with one customer-scoped
`topic_id` (at most one). Events without a topic — including every event
already in a database created by an older version — behave exactly as before.

**Tickets ("工单").** `POST /tickets` opens an investigation ticket.
Retaining an event (`POST /tickets/{id}/events`) copies its content into the
ticket inside the same kind of `BEGIN IMMEDIATE` transaction used everywhere
else. Retention only succeeds for events of the same customer that are
received and not yet withdrawn. The same event can be retained by any number
of tickets independently. An open ticket reads its copies via
`GET /tickets/{id}/events`. Closing (`POST /tickets/{id}/close`) rejects all
further reads and additions and purges that ticket's copies.

**Withdrawal.** `POST /topics/{topic_id}/withdraw` is irreversible and safe
to repeat (`already_withdrawn: true` on repeats). The committing transaction:

* replaces `events.payload_json` with a JSON `null` sentinel and clears
  `content_hash`, and marks the rows `status='withdrawn'` — `GET /events`
  and delivery/event read models then expose only id, flow, `seq`, timestamps
  and the `withdrawn` flag, never the JSON or its digest;
* keeps the `(customer, flow, caller_event_id)` tombstone, so reposting the
  original key is rejected (`event_withdrawn`/`topic_withdrawn`) — identical
  resend cannot restore content or allocations, and changed content cannot
  reuse the key;
* rejects every new event carrying that `topic_id` **before** sequence
  allocation or fan-out, so rejected events leave no seq gap and no
  delivery;
* turns every delivery not already `succeeded` or
  `administratively_skipped` into the terminal status `withdrawn`. Confirmed
  successes and admin-skipped facts (status, attempts, reasons/actions) are
  preserved. Withdrawn work is never claimed/retried, is not revived by a
  target change or admin retry, and counts as terminal for per-flow ordering,
  so the next non-withdrawn event in the same flow is unblocked;
* clears free-text diagnostic columns that may carry a receiver echo
  (`deliveries.last_error`, `attempts.error`, `late_results.error`);
* additionally, in the same transaction, writes one durable external-erasure
  request per participating receiver that was ever authorized for this topic
  (see [External replica erasure](#external-replica-erasure-participating-receivers)).

**Send fence and late results.** A worker transitions a claim through
`Store.mark_sending()` (a write transaction) immediately before invoking the
HTTP sender. Because both that transition and withdrawal are
`BEGIN IMMEDIATE` writers, their server-side commit order decides the race:

* withdrawal first → the fence fails and **no HTTP request is sent** for the
  claimed-but-unsent unit;
* fence first → the request is already in flight. Withdrawal may optionally
  wait for such attempts (`wait_for_inflight=true` / CLI `--wait`, bounded by
  `--wait-timeout`) but other topics are never held up. When the late reply
  lands, `finish_attempt` sees `status='withdrawn'` first: the delivery is
  never rewritten, and the sender's error text (which can contain a receiver
  echo of the payload) is deliberately not persisted.

**Retention vs withdrawal** is likewise decided by commit order: a retention
that commits first has its own content copy and stays readable while the
ticket is open; a withdrawal that commits first makes retention fail with
`event_withdrawn`. When the last open ticket's copy is purged by closure, no
original content remains in SQL-visible tables (verified by a full-table scan
in `tests/test_retraction.py`). A best-effort `PRAGMA wal_checkpoint(TRUNCATE)`
limits recent WAL frames; external receivers, historical backups and
disk-page forensics are deliberately out of scope.

**Multiple processes / restart.** All of the above is only database state, so
the API process, several worker processes and the CLI can share one SQLite
file exactly as before; a fresh `Store` after restart observes the same
withdrawals, tickets and fences. Expired leases belonging to withdrawn
deliveries are settled as `expired` attempts but never re-queued.

## External replica erasure (participating receivers)

Withdrawing a topic erases content everywhere *this service* stores it, but a
receiver that already holds a delivered payload outside our trust boundary is
not touched. This feature lets a customer register **participating erasure
receivers** and has the service ask exactly those receivers to erase their
copies of a withdrawn topic, then reports which ones acknowledged. It never
tries to control receivers that did not join the protocol, and adds no identity
system: the existing `customer_id` scope is used throughout.

**Registration and targeting.** `POST /erasure-receivers {customer_id, url}`
returns a stable `receiver_id`; a customer may register several, each bound to
its fixed HTTP address. A subscription selects one by `erasure_receiver_id`
(its `target_url` is pinned to that registration). `PATCH` switches to another
registration, or sets `clear_erasure_receiver: true` to go back to an ordinary
target. A registration can only be selected by its own customer.

**Which callbacks carry the protocol.** Only a *topic-bearing* event delivered
to a subscription with an `erasure_receiver_id` is in the protocol. Its request
carries the `customer_id`, `topic_id`, the original `delivery_id` (as always),
the registration id, and an `authorization_seq` — enough for the receiver to
manage replicas for precisely one `(customer, topic)`. Topic-less events and
ordinary targets are byte-for-byte the original behaviour, and a subscription
never chases callbacks delivered before it was created (or before it selected
that receiver).

**Scope = who was actually authorized, never who is merely configured.** The
erasure obligation for a receiver is written by `Store.begin_send()` — the same
`BEGIN IMMEDIATE` send fence the worker passes immediately before handing the
request to the HTTP sender — *before* any request leaves. It is written whether
the callback later succeeds, fails, times out, is superseded by a target change,
or is still in flight. Consequences that the tests pin down:

* a receiver the worker was ever cleared to send to is in the topic's erasure
  scope, success or failure notwithstanding;
* a receiver that was only configured (selected but never reached the send
  gate) is **not** notified;
* repointing a subscription to another address **cannot move** an obligation
  already produced — the receiver that actually received the callback stays in
  scope and the new target is not asked about the past.

**Durable requests, independent retries.** `withdraw_topic` still does all of
its original local work (erase `payload_json`/digest, tombstone, terminate
deliveries, reject new topic events, unblock the flow, retain/report tickets).
In the *same* transaction it writes one `erasure_requests` row per obligated
receiver. Because the rows are committed with the withdrawal, a crash right
after withdrawal still leaves every required erasure discoverable: a restarted
`Store`/`Worker` continues them. Erasure is a **separate work queue** from
delivery (`claim_due_erasures` / `finish_erasure`), so a slow or unreachable
receiver never blocks another receiver's erasure or any unrelated topic's
delivery. A request retries with the **same operation id**; it is
`acknowledged` only when that receiver returns an explicit positive
acknowledgement of exactly that id. A local withdrawal, the original callback's
result, or another receiver's acknowledgement never counts as this receiver's
confirmation. Exhausted retries park the row as `failed` (never acknowledged)
for `POST /erasure-requests/{id}/retry`.

**Protocol shape (no content).** An erasure request is
`POST <receiver url>` with a JSON body of
`{protocol, protocol_version, operation_id, customer_id, topic_id, receiver_id,
watermark, action}` and matching `X-Webhook-Erasure-*` headers. It contains no
event body, no digest and no excerpt, is not a redelivery of the original
event, and is not a delivery at all: it consumes no prepaid point and never
enters the retry budget / `needs_attention` machinery of deliveries. It is
`acknowledged` only if the receiver answers a 2xx whose JSON body is
`{"acknowledged": true, "operation_id": <this id>}`.

**The reference receiver** (`webhook_delivery/erasure_receiver.py`, runnable via
`python -m webhook_delivery.erasure_receiver --db …`) keeps managed copies in
its **own separate persistent SQLite file** and is the worked example of the
receiver side. It:

* deletes copies only for the exact `(customer_id, topic_id)` in the request —
  a same-named topic of another customer, or another topic of the same
  customer, is untouched;
* acknowledges only after the scope holds no managed copies **and** the scope
  is sealed at the request's `watermark`. Every callback carries its
  `authorization_seq`; once the seal is set, any callback with
  `authorization_seq <= watermark` is rejected without storing. Since the
  watermark is the highest sequence authorized before withdrawal, a callback
  that was in flight at withdrawal **cannot recreate the copy even if its HTTP
  request arrives after the erasure request** — no HTTP ordering is assumed;
* is idempotent: a repeated erasure request returns the same acknowledgement and
  deletes nothing extra; seals and copies are durable across restart, so a
  late pre-withdrawal callback is still rejected after a restart.

**Confirmation progress.** `GET /topics/{topic_id}/erasure?customer_id=` returns
`{total, acknowledged, pending, complete, receivers: [...], acknowledged_receiver_ids,
pending_receiver_ids}`. Only participants that were authorized for this topic
appear; receivers outside the protocol are not in this view and not counted.

The customer still withdraws (`POST /topics/{id}/withdraw`), reads tickets and
evidence, and can query this progress with the same customer scope as before.

### Using it on an existing database — no backfill

`Store.initialize()` (run by the API lifespan, the worker and every CLI
command) performs an additive migration: it adds `events.topic_id`,
`events.status`, `attempts.send_started_at`, `subscriptions.erasure_receiver_id`
and `deliveries.erasure_receiver_id` if missing, and creates the `topics`,
`tickets`, `ticket_evidence`, `erasure_receivers`, `erasure_obligations` and
`erasure_requests` tables. Existing rows get `topic_id = NULL` and
`erasure_receiver_id = NULL`, so old events keep working unchanged and old
subscriptions stay ordinary targets. There is nothing to backfill and no
behavioural change for topic-less or non-participating traffic.

```bash
# 1. update the code, then simply start the API/worker as usual — migration runs once
WEBHOOK_DB=/var/lib/webhook/webhook_delivery.db \
  uvicorn webhook_delivery.api:app --host 127.0.0.1 --port 8000
python -m webhook_delivery.worker --db /var/lib/webhook/webhook_delivery.db

# 2. or operate directly on the existing file with the CLI
export PYTHONPATH="$PWD/.pylibs:$PWD"
python -m webhook_delivery --db /var/lib/webhook/webhook_delivery.db init-db
python -m webhook_delivery --db … withdraw-topic --customer cus_acme --topic gdpr-42
python -m webhook_delivery --db … create-ticket --customer cus_acme --title "case 4821"
python -m webhook_delivery --db … retain --ticket tkt_… --event evt_… [--event evt_…]
python -m webhook_delivery --db … list-evidence --ticket tkt_…
python -m webhook_delivery --db … close-ticket --ticket tkt_…

# external replica erasure
python -m webhook_delivery --db … register-erasure-receiver --customer cus_acme --url https://partner/erase
python -m webhook_delivery --db … list-erasure-receivers --customer cus_acme
python -m webhook_delivery --db … erasure-progress --customer cus_acme --topic gdpr-42
python -m webhook_delivery --db … list-erasure-requests --customer cus_acme --status pending
python -m webhook_delivery --db … retry-erasure --erasure ers_… --reason "partner back up"

# the reference participating receiver (its own persistent library)
python -m webhook_delivery.erasure_receiver --db /var/lib/partner/receiver.db --port 8100
```

`./run_retraction_demo.sh` runs a self-checking local demo of the ticket,
erasure, ordering and receiver-echo scenarios on a throw-away database.
`./run_erasure_demo.sh` runs a self-checking demo of external-replica erasure
against two reference receivers, each with its own persistent database.

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

`demo_erasure.py` verifies the external-replica erasure behaviour:

1. several registered receivers per customer, selecting one and switching to
   another; topic-bearing callbacks let the receiver identify customer, topic
   and the original delivery, while topic-less events stay ordinary;
2. withdrawal notifies exactly the receivers actually authorized for the topic
   — a configured-only or repointed-away target is not notified;
3. per-operation confirmation progress (`acknowledged`/`pending`), counting
   only a matching per-id acknowledgement;
4. a failed/unreachable erasure stays pending and retries with the same id
   without blocking another receiver;
5. the reference receiver erases only the exact customer+topic and still
   rejects an in-flight pre-withdrawal callback whose HTTP request arrives
   after the erasure request;
6. crash after local withdrawal → restart → the required erasures still
   complete against the original receivers.

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
* External erasure is cooperative: only registered *participating* receivers
  are asked, and only they appear in the confirmation view. The service cannot
  force a non-participating receiver to erase anything; the reference receiver
  is one worked implementation of the receiver side. Erasures to an
  unreachable receiver accumulate as pending rows until it recovers (or an
  operator retries), by design — they are never silently dropped or marked
  confirmed.
