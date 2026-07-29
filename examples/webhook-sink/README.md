# Webhook sink example

This standalone-first reference connector forwards Redis Event Stream entries
to an HTTP endpoint with consumer-group recovery and acknowledgement only after
a successful `2xx` response.

Run from this directory:

```bash
cargo run --release -- \
  --url redis://127.0.0.1:6379 \
  --webhook https://example.test/redis-events
```

The sink:

- discovers per-event streams through `EVENTSTREAM.STREAMS`;
- skips the module-owned `#` namespace and reads the control stream separately;
- creates its consumer group at `0`, preserving retained history;
- drains its own pending list and uses `XAUTOCLAIM` for abandoned work;
- retries failed POSTs with capped exponential backoff;
- sends `XACK` only after a `2xx` response;
- forwards gap markers as a distinct JSON record; and
- base64-encodes raw Redis key bytes.

Every POST includes `Idempotency-Key: <stream>/<entry-id>`. A crash after the
endpoint accepts a request but before `XACK` causes a duplicate POST, so the
endpoint must deduplicate on that value.

Use `--max-attempts N` to exit after a finite number of failed attempts. The
entry remains pending and is eligible for recovery by a later process. Use
`--max-records N` for a bounded demonstration run.

The example intentionally supports standalone Redis first. Cluster mode needs
one group and reader per node-local stream plus topology refresh; use the
shipped `eventstream-client` and the deployment-topology guide as the starting
point for that extension.

See the [sink connector guide](../../docs/src/sink-connector.md) for record
shapes, retention sizing, failure semantics, and security guidance.
