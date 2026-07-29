# Forward streams to a webhook

The webhook sink example turns the module's Redis Streams into the capture edge
of a larger event pipeline:

```text
Redis keyspace event → module stream → consumer group → HTTP endpoint
```

It is a reference implementation for delivery semantics, not a general
integration platform. The code lives in `examples/webhook-sink`.

## Run the connector

Start Redis with the module loaded and capture the event types you need. Then:

```bash
cd examples/webhook-sink
cargo run --release -- \
  --url redis://127.0.0.1:6379 \
  --webhook https://example.test/redis-events
```

Use a group name unique to the downstream destination:

```bash
cargo run --release -- \
  --webhook https://example.test/redis-events \
  --group customer-index-webhook \
  --consumer sink-1
```

The connector creates each group at `0`, so retained entries are eligible even
when capture began before the connector. It periodically refreshes
`EVENTSTREAM.STREAMS` and adds newly observed event types.

## Delivery contract

For each entry the connector:

1. reads through `XREADGROUP`;
2. posts one JSON record;
3. accepts any `2xx` response as durable downstream delivery; and
4. issues `XACK` only after that response.

On startup it first drains entries already pending under its consumer name,
then uses `XAUTOCLAIM` to recover entries left idle by dead consumers. Network
errors and non-`2xx` responses are retried with capped exponential backoff.

The result is at-least-once handoff inside the Redis retention window. A crash
after the endpoint commits the request but before Redis receives `XACK` causes
redelivery. Every request therefore carries:

```text
Idempotency-Key: <stream>/<entry-id>
```

The endpoint must treat that value as its deduplication key. `--max-attempts 0`
retries forever; a finite value exits without acknowledging the failed record,
so another process can recover it.

## Event record

Keys are raw bytes in Redis. JSON cannot carry arbitrary bytes, so the
connector uses standard padded base64:

```json
{
  "type": "event",
  "stream": "events:set",
  "id": "1785349757949-0",
  "event": "set",
  "key_base64": "c2Vzc2lvbjo0Mg==",
  "db": 0
}
```

The connector supports the `fixed`, `minimal`, `verbose`, and `json` module
entry formats. It consumes per-event streams and deliberately skips
`<prefix>#firehose`, avoiding duplicate forwarding when both representations
are enabled.

## Gap record

The control stream is read through the same consumer group and acknowledgement
boundary. A marker becomes:

```json
{
  "type": "gap",
  "stream": "events:#control",
  "id": "1785349760000-0",
  "action": "disabled",
  "module_version": "0.3.0"
}
```

Downstream systems should retain these records and open or close reconciliation
windows using the rules in [Reliability and delivery](reliability.md). A
`flushed` marker also carries `db`.

## Retention and failure planning

The connector cannot claim or deliver an entry that Redis already trimmed.
Size each stream for the longest credible endpoint outage plus deployment and
recovery time:

```text
maxlen ≥ peak events/second × worst-case connector downtime
```

Alert on consumer-group lag before it reaches that window. `XAUTOCLAIM` reports
IDs trimmed while pending; the connector logs those as lost work because their
field values no longer exist.

## Scope and security

This example is standalone-first. A cluster connector needs node-local group
management, direct master discovery, topology refresh, and stream-set changes
after re-pin. Follow [Deployment topologies](topologies.md) and reuse the
shipped Rust client's discovery primitives rather than treating a cluster as
one stream endpoint.

Use HTTPS, authenticate the endpoint, and keep webhook credentials outside
command history in a production adaptation. This reference accepts a URL
directly so the delivery mechanics remain visible; it does not provide secret
storage, request signing, a dead-letter queue, or payload transformation.

Kafka uses the same rule: wait for the broker's durable produce
acknowledgement, then `XACK`. Kafka code is omitted here to avoid making
`librdkafka` part of the repository's example toolchain.
