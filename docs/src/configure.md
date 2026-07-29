# Configure capture

The default configuration captures key expirations into `events:expired` and
keeps approximately the newest 10,000 entries. Most evaluations need only
three decisions: which events to capture, which keys to include, and how much
history to retain.

## Choose events

Change the live event filter with `CONFIG SET`:

```text
CONFIG SET eventstream.events "expired,set,hset,del"
```

Each event name receives its own stream:

| Event | Destination |
|---|---|
| `expired` | `events:expired` |
| `set` | `events:set` |
| `hset` | `events:hset` |
| `del` | `events:del` |

Use a comma-separated list for exact event names, an `@class` token for a Redis
notification class, or `*` for all events. A narrow exact list is usually the
best starting point: wider filters increase write amplification and stream
cardinality.

The high-volume `@missed` and `@new` classes must be selected at module load so
Redis subscribes the module to them. Adding either class only with a later
`CONFIG SET` cannot make events appear.

Hash-field expirations on Redis 7.4 or newer use the separate `hexpired` event.
The entry identifies the hash key, not the expired field name.

## Filter keys and databases

Key filters use comma-separated glob patterns:

```text
CONFIG SET eventstream.key-filter "session:*,lease:*"
```

The key filter and event filter are combined: an event must match both. Keys
are raw bytes, so patterns are matched byte-for-byte.

Standalone deployments can also select source databases:

```text
CONFIG SET eventstream.source-dbs "0,2"
```

All destination streams live in database 0. The entry's `db` field records the
source database.

## Choose retention

`eventstream.maxlen` applies an approximate count cap to every stream:

```text
CONFIG SET eventstream.maxlen 100000
```

Set it to `0` only when another process owns trimming and unbounded growth is
intentional. A useful first estimate is:

```text
maxlen ≥ peak events per second × maximum consumer downtime
```

Busy and quiet event types can use different caps:

```text
CONFIG SET eventstream.maxlen-overrides \
  "expired=600000,set=50000"
```

Time-based retention is also available:

```text
CONFIG SET eventstream.retention-ms 86400000
```

When `retention-ms` is greater than zero, it takes precedence over count-based
trimming and retains approximately one day in this example. Redis trims during
new writes, not on a background schedule.

## Optional stream shapes

The default `fixed` entry format contains `event`, `key`, and `db`. Other
formats are Preview:

```text
CONFIG SET eventstream.entry-format minimal
CONFIG SET eventstream.entry-format verbose
CONFIG SET eventstream.entry-format json
```

Because the setting changes live, one stream can contain old and new formats.
Non-default entries carry a `format` discriminator.

Enable the firehose when a consumer needs one combined stream:

```text
CONFIG SET eventstream.firehose yes
```

Every event is then written both to its per-event stream and to
`events:#firehose`. This provides one node-local total order but doubles stream
writes and uses the shared `maxlen` cap.

## Load-time settings

Most settings can be changed with `CONFIG SET`. These settings are immutable
and must be supplied when Redis loads the module:

- `eventstream.stream-prefix`
- `eventstream.cluster-streams`
- `eventstream.entry-seq`

Module arguments use their short names:

```bash
redis-server \
  --loadmodule /path/to/libredis_event_stream_module.so \
  events 'expired,set' \
  maxlen 100000 \
  stream-prefix 'events:'
```

The server command line also accepts the full `--eventstream.<name>` form.

## Verify the effective configuration

Inspect the live values after every deployment or reconfiguration:

```text
CONFIG GET eventstream.*
EVENTSTREAM.STATS
EVENTSTREAM.STREAMS VERBOSE
```

For exact types, defaults, validation rules, and live-change semantics, use the
[configuration reference](reference/configuration.md).
