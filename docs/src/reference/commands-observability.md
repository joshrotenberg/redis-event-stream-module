# Commands and observability

The module exposes three commands, one INFO section, and a control stream.
These sections are included from the canonical
[SPEC.md](https://github.com/joshrotenberg/redis-event-stream-module/blob/main/SPEC.md)
definitions.

## Commands

{{#include ../../../SPEC.md:commands}}

## Gap markers

{{#include ../../../SPEC.md:gap-markers}}

## INFO fields

Module fields do not appear in plain `INFO` or `INFO all`. Use
`INFO eventstream`, `INFO eventstream_stats`, `INFO modules`, or
`INFO everything`.

{{#include ../../../SPEC.md:counters-info}}

## Counter semantics

{{#include ../../../SPEC.md:counters-explanation}}

## Alerting baseline

{{#include ../../../SPEC.md:alerting-table}}
