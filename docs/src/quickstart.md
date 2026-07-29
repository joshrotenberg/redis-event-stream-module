# Quickstart

Start Redis with the module preloaded, expire one key, and read the resulting
stream entry. The Docker path needs no Rust toolchain or local Redis
installation.

## Start Redis

Run the published image and give the container a stable name:

```bash
docker run --rm --name eventstream-quickstart \
  -d -p 6379:6379 \
  ghcr.io/joshrotenberg/redis-event-stream-module:latest
```

Wait for `PONG`:

```bash
docker exec eventstream-quickstart redis-cli PING
```

The default configuration captures expirations into `events:expired`.

## Capture an expiration

Create a short-lived key, wait for its TTL, and force Redis to check it:

```bash
docker exec eventstream-quickstart \
  redis-cli SET quickstart:session active PX 500
sleep 1
docker exec eventstream-quickstart \
  redis-cli GET quickstart:session
```

The final command returns nil because Redis removed the key.

Read the mirrored entry:

```bash
docker exec eventstream-quickstart \
  redis-cli XRANGE events:expired - +
```

The reply contains an entry shaped like this:

```text
1785349757949-0
event
expired
key
quickstart:session
db
0
```

The stream ID contains the event time in milliseconds. The `key` field is
binary-safe, and `db` records the database in which the event occurred.

## Check capture health

The module exposes its configuration, counters, and discovered streams:

```bash
docker exec eventstream-quickstart \
  redis-cli CONFIG GET 'eventstream.*'
docker exec eventstream-quickstart \
  redis-cli EVENTSTREAM.STATS
docker exec eventstream-quickstart \
  redis-cli EVENTSTREAM.STREAMS
```

`notify-keyspace-events` does not need to be enabled. That setting controls
Redis pub/sub notifications; module subscribers receive keyspace events
independently.

Stop the container when finished:

```bash
docker stop eventstream-quickstart
```

## Build from source

To load a local build instead, use Rust 1.88 or newer:

```bash
cargo build --release
redis-server \
  --loadmodule ./target/release/libredis_event_stream_module.so
```

On macOS the artifact ends in `.dylib`. Prebuilt module artifacts and checksums
for Linux and macOS are attached to every
[GitHub release](https://github.com/joshrotenberg/redis-event-stream-module/releases).

## Next steps

- [Try the observatory](observatory.md) for an interactive workflow.
- [Configure capture](configure.md) to add events and filters.
- [Consume events](consume.md) with live readers or consumer groups.
- Review the [production checklist](production.md) before deployment.
