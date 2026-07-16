# Node.js consumer example

An [ioredis](https://github.com/redis/ioredis) consumer for
`redis-event-stream-module`, covering the three patterns from
[docs/consumer-patterns.md](../../docs/consumer-patterns.md).

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

| Subcommand | consumer-patterns.md section |
|---|---|
| `tail` | Live tail (pub/sub replacement) |
| `work` | Durable work queue (consumer groups) + Recovering stuck work |
| `reconcile` | Handling gaps (→ [loss-windows.md](../../docs/loss-windows.md)) |
| `discover` | Discovery |

## Why ioredis, and binary-safe keys

The `key` field is raw bytes. Per SPEC.md section 6: *"Consumers must read `key`
with a bytes-typed client API; clients that eagerly decode replies as UTF-8 will
mangle non-UTF-8 keys, which is a client configuration issue, not stream data
loss."* This example uses **ioredis** specifically because its `*Buffer` command
variants (`xreadBuffer`, `xreadgroupBuffer`, `xrangeBuffer`, `xautoclaimBuffer`)
return replies as `Buffer`s, so the `key` field is kept as bytes and round-trips
exactly; it is decoded only for display. (node-redis works too, via its
buffer/type-mapping options — ioredis just makes it a one-word change per call.)
