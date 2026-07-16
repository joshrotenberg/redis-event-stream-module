# Commands

The module adds three keyless commands. Everything else observable goes through
standard stream commands (`XLEN`, `XRANGE`, `XINFO STREAM`, `XINFO GROUPS`) and
the [counters](./counters.md) INFO section; behavior changes go through
[`CONFIG SET`](./configuration.md).

- `EVENTSTREAM.STATS` — the counter surface as a flat field/value array
  (`readonly fast`).
- `EVENTSTREAM.STREAMS [WITHSTATS | VERBOSE]` — the registered destination
  streams, optionally annotated with per-stream counters or liveness
  (`readonly`).
- `EVENTSTREAM.PRUNE` — drop registry entries whose destination stream no
  longer exists (`write`).

The full reply formats, flags, database-0 read behavior, and ACL notes are
included from [the specification](./specification.md) (section 8) below:

{{#include ../SPEC.md:commands}}
