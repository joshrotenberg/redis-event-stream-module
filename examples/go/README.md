# Go consumer example

A [go-redis/v9](https://github.com/redis/go-redis) consumer for
`redis-event-stream-module`, covering the three patterns from
[docs/consumer-patterns.md](../../docs/consumer-patterns.md).

```sh
# against a server with the module loaded (default: expirations only)
go run . tail        # live tail (pub/sub replacement)
go run . work        # durable work queue + stuck-work recovery
go run . reconcile   # delimit capture gaps from the control stream
go run . discover    # list destination streams
```

Connection defaults to `127.0.0.1:6379`; override with `REDIS_ADDR` (and
`CONSUMER` for the work-queue consumer name).

| Subcommand | consumer-patterns.md section |
|---|---|
| `tail` | Live tail (pub/sub replacement) |
| `work` | Durable work queue (consumer groups) + Recovering stuck work |
| `reconcile` | Handling gaps (→ [loss-windows.md](../../docs/loss-windows.md)) |
| `discover` | Discovery |

## Binary-safe keys

The `key` field is raw bytes. Per SPEC.md section 6: *"Consumers must read `key`
with a bytes-typed client API; clients that eagerly decode replies as UTF-8 will
mangle non-UTF-8 keys, which is a client configuration issue, not stream data
loss."* go-redis returns field values as Go `string`s, and a Go string is
byte-safe — it can hold arbitrary bytes rather than validated Unicode — so the
key round-trips exactly. Recover the raw bytes with `[]byte(values["key"].(string))`.
