# Python consumer example

A [redis-py](https://github.com/redis/redis-py) consumer for
`redis-event-stream-module`, covering the core patterns from
[Consume events](../../docs/src/consume.md) and
[Reliability and delivery](../../docs/src/reliability.md).

```sh
pip install -r requirements.txt
# against a server with the module loaded (default: expirations only)
python3 consumer.py tail        # live tail (pub/sub replacement)
python3 consumer.py work        # durable work queue + stuck-work recovery
python3 consumer.py reconcile   # delimit capture gaps from the control stream
python3 consumer.py discover    # list destination streams
```

Connection defaults to `127.0.0.1:6379`; override with `REDIS_HOST` /
`REDIS_PORT` (and `CONSUMER` for the work-queue consumer name).

| Subcommand | Documentation section |
|---|---|
| `tail` | Live tail ([Consume events](../../docs/src/consume.md)) |
| `work` | Durable work queue and stuck-work recovery ([Consume events](../../docs/src/consume.md)) |
| `reconcile` | Gap handling ([Reliability and delivery](../../docs/src/reliability.md)) |
| `discover` | Stream discovery ([Deployment topologies](../../docs/src/topologies.md)) |

## Binary-safe keys

The `key` field is raw bytes. Per SPEC.md section 6: *"Consumers must read `key`
with a bytes-typed client API; clients that eagerly decode replies as UTF-8 will
mangle non-UTF-8 keys, which is a client configuration issue, not stream data
loss."* This example creates the client **without** `decode_responses`, so
replies stay `bytes` and a non-UTF-8 key round-trips exactly; it decodes only
for display, and only with `errors="replace"`.
