# Configuration

The module name is `eventstream`, so every key is `eventstream.<name>`. Set
them at load (module arguments, a `loadmodule` line, a redis.conf directive, or
`MODULE LOADEX ... CONFIG`) and, except where marked IMMUTABLE, live via
`CONFIG SET`. Read them back with `CONFIG GET eventstream.*`.

The table below is included verbatim from the authoritative source in
[the specification](./specification.md) (section 7) — this page does not keep a
separate copy. Each key's full rationale follows the table in that section.

## Keys

{{#include ../SPEC.md:config-table}}

## Filter grammars

`eventstream.events`, `eventstream.key-filter`, `eventstream.source-dbs`, and
`eventstream.maxlen-overrides` each take a small grammar:

{{#include ../SPEC.md:config-grammars}}

## Load-time precedence

{{#include ../SPEC.md:config-precedence}}

## Live-change semantics

`CONFIG SET` on a live-settable key takes effect on the next captured event.
The table below records exactly when each change applies and what happens to
post-notification jobs already enqueued:

{{#include ../SPEC.md:config-live-change}}
