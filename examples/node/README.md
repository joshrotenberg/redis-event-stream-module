# Node.js consumer example

An [ioredis](https://github.com/redis/ioredis) consumer for
`redis-event-stream-module`, covering the core patterns from
[docs/consumer-patterns.md](../../docs/consumer-patterns.md) and its companion
pages.

```sh
npm install
# against a server with the module loaded (default: expirations only)
node consumer.js tail        # live tail (pub/sub replacement)
node consumer.js work        # durable work queue + stuck-work recovery
node consumer.js reconcile   # delimit capture gaps from the control stream
node consumer.js discover    # list destination streams
```

Connection defaults to `127.0.0.1:6379`; override with `REDIS_HOST` /
`REDIS_PORT` (and `CONSUMER` for the work-queue consumer name).

| Subcommand | Documentation section |
|---|---|
| `tail` | Live tail ([consumer-patterns.md](../../docs/consumer-patterns.md)) |
| `work` | Durable work queue + Recovering stuck work ([work-queues.md](../../docs/work-queues.md)) |
| `reconcile` | Handling gaps (→ [loss-windows.md](../../docs/loss-windows.md)) |
| `discover` | Discovery ([cluster-consumers.md](../../docs/cluster-consumers.md)) |

## Why ioredis, and binary-safe keys

The `key` field is raw bytes. Per SPEC.md section 6: *"Consumers must read `key`
with a bytes-typed client API; clients that eagerly decode replies as UTF-8 will
mangle non-UTF-8 keys, which is a client configuration issue, not stream data
loss."* This example uses **ioredis** specifically because its `*Buffer` command
variants (`xreadBuffer`, `xreadgroupBuffer`, `xrangeBuffer`, `xautoclaimBuffer`)
return replies as `Buffer`s, so the `key` field is kept as bytes and round-trips
exactly; it is decoded only for display. (node-redis works too, via its
buffer/type-mapping options — ioredis just makes it a one-word change per call.)
