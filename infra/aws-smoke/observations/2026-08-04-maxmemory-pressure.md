# Maxmemory pressure and `verify-oom`

Date: 2026-08-04

Issue: #259

Artifact: `artifacts/2026-08-04-maxmemory-pressure.json`

This report records the first deterministic memory-pressure campaign for the
module's event capture path.

## Question

Can the module distinguish a successful source mutation from a generated
stream write that Redis refuses at the memory limit? What changes when
`eventstream.verify-oom` is disabled, and can an eviction policy remove stream
history after the module successfully captured it?

This is the maxmemory slice of issue #259. It tests deterministic pressure
against `noeviction`, `volatile-lru`, and `allkeys-lru`. It does not establish
safe production memory headroom, long-run fragmentation behavior, or retention
growth at larger data sizes.

## Method

- Redis 8.8.0 and the module built from commit `e4f2378`.
- One `c7i.large` Redis host and one `c7i.large` load generator in
  `us-west-2a`, each with a 16 GiB gp3 root volume configured for 3,000 IOPS
  and 125 MiB/s throughput.
- Persistence and replication were disabled so they could not change the
  maxmemory result.
- Each case started from a clean Redis process with a 50,000-key prefill using
  1,024-byte values. The limit was then set to 90% of the measured memory
  footprint, putting Redis under immediate deterministic pressure.
- The `noeviction` cases issued 15,000 unique source `DEL`s. Every source
  command succeeded; the probe reconciled those successes against canonical
  stream length, `forwarded`, `events_lost`, and `dropped_oom`.
- The eviction-policy cases issued 15,000 captured writes. Source prefill keys
  were expiring under `volatile-lru`; all keys were eligible under
  `allkeys-lru`.
- The `allkeys-lru` case first proved exact capture, disabled the module, then
  generated bounded memory churn until the already-written canonical stream
  disappeared. Disabling capture separated later eviction from a new capture
  failure.
- A one-repetition S0/S2 100,000 operations/s smoke preceded the probe to
  validate the exact artifact and host. It is context, not a new capacity
  claim.

## Results

| Case | Source successes | Forwarded | Counted lost | Stream after capture | Evictions | Result |
|---|---:|---:|---:|---:|---:|---|
| `noeviction`, verify yes | 15,000 | 9,805 | 5,195 | 9,805 | 0 | Exact accounted refusal and recovery |
| `noeviction`, verify no | 15,000 | 15,000 | 0 | 15,000 | 0 | Exact capture with preflight disabled |
| `volatile-lru`, verify yes | 15,000 | 15,000 | 0 | 15,000 | 2,654 | Stream protected; expiring data evicted |
| `allkeys-lru`, verify yes | 15,000 | 15,000 | 0 | 15,000, then 0 | 62,994 | Captured history later evicted silently |

Across the four pressure cases, 60,000 source operations completed without a
producer error. The module forwarded 54,805 events and counted exactly 5,195
lost events, all of them `dropped_oom` in the OOM-verified `noeviction` case.
The equality `forwarded + events_lost = successful source commands` held
exactly there. Every case passed its reconciliation checks.

The short S2 smoke also forwarded exactly 1,482,801 selected events with zero
loss or drops while achieving 98,762 operations/s. Its S0 control achieved
99,028 operations/s. These single observations only prove that the campaign
artifact behaved normally before the pressure probe.

## Observations

### Refused generated writes are observable and exactly countable

With `noeviction` and `eventstream.verify-oom yes`, Redis began above the new
limit. All 15,000 source `DEL`s succeeded, while 5,195 generated stream writes
were refused before the deletes freed enough memory for capture to resume.
The module reported exactly 5,195 `events_lost`, `dropped`, and `dropped_oom`,
then forwarded the remaining 9,805 events. The canonical stream length was
also 9,805.

This is the positive result for gap-aware best effort: the source operation is
not rolled back, but the module exposes an exact counter when it knows that the
corresponding event could not be appended.

### Disabling OOM verification chooses availability over admission safety

The matched `verify-oom no` case forwarded all 15,000 events with no reported
loss even though the case began above the configured memory limit. Disabling
the preflight therefore allows the generated module command to proceed while
ordinary OOM admission would refuse it.

That opt-out can preserve capture through a transient pressure window, but it
is not free memory. It should be documented as an explicit operational trade:
operators accept additional allocation pressure rather than a counted event
gap.

### A volatile-only policy protected non-expiring stream history

Under `volatile-lru`, Redis evicted 2,654 expiring source keys while the
non-expiring canonical stream retained all 15,000 entries. The module reported
zero loss or drops. This is reassuring for the tested policy shape, but it also
shows where the capacity cost lands: source cache data is sacrificed to keep
the event history.

### Allkeys eviction creates loss the module cannot count after capture

Under `allkeys-lru`, the module first forwarded all 15,000 events exactly. It
reported `eviction_risk=1`, correctly identifying the unsafe configuration.
After capture was disabled, one 50,000-key churn round brought cumulative
evictions to 62,994 and removed the canonical stream entirely. Forwarded,
loss, and drop counters did not change.

That behavior is not a failed `XADD`: Redis accepted every event and later
evicted the stream key. A periodic status stream generated by the same Redis
instance would be exposed to the same eviction policy, so it could not by
itself prove continuity. Consumers that need gap detection require an
external checkpoint or continuity protocol in addition to module counters.

## Practical takeaway

The useful contract is now sharper:

- The module provides best-effort event delivery, not an independent durable
  log.
- Known append failures are counted, and a consumer can alert on counter
  changes.
- `eviction_risk` can warn that an allkeys policy makes retained history
  unsafe.
- Successful history can still disappear later without incrementing a module
  loss counter.

For the current design, prefer `noeviction` or a volatile-only policy for the
event-stream keyspace, size `MAXLEN` and memory headroom deliberately, monitor
Redis evictions plus module loss counters, and keep an authoritative source of
truth for reconciliation. A future metadata/checkpoint stream is useful for
liveness and rates, but durable gap detection has to compare against state
outside the same eviction and failure domain.

## Remaining issue #259 work

This report leaves #259 open for:

- bounded, time-based, and unbounded retention growth;
- firehose write amplification;
- prolonged replica lag, backlog exhaustion, and full resynchronization;
- primary failure, promotion, and post-promotion capture behavior;
- larger datasets, fragmentation growth, and repeated pressure cycles; and
- longer runs and abrupt-failure durability windows.

The disposable lab was destroyed after artifact collection. Terraform state
is empty and both instances are terminated. Both delete-on-termination root
volumes entered AWS deletion; direct EC2 verification confirmed one deleted
and the other still in `deleting` immediately after teardown.
