# Async and batch write spike

Date: 2026-07-30  
Issue: #265  
Branch commit: `583ce7f6e8d9b5c73bb3c5c43d913aeba289751f`  
Module artifact SHA-256:
`aea7b10ed6927b83ff81b427fbe469476aa09c44980a8a936ad80b75caab936a`

## Question

Can capture move off the notification path without losing events, and does
combining logical events into fewer `XADD` calls improve throughput?

The spike keeps the historical path as the default and compares:

- `sync`: the existing post-notification job, one `XADD` per event;
- `individual`: a bounded worker, still one `XADD` per event; and
- `envelope`: a bounded worker writing versioned `batch-v1` entries.

The queue never drops on saturation. After a fixed lock-attempt budget, a
post-notification fallback drains every earlier queued event before the current
event, preserving order. Unload stops and joins the worker, then synchronously
finishes accepted events.

## Environment

- AWS `us-west-2`, one `c7i.large` Redis host and one `c7i.large` load host
- Redis 8.8.0, same pinned runtime image in every scenario
- Branch module built on the x86 server from the GitHub commit above
- 5,000,000 `SET` requests, 100 clients, 4 load-generator threads
- 64-byte values, 100,000-key random keyspace
- queue capacity 1,048,576; batch size 256; maximum wait 1 ms
- randomized trial order, three sync and three envelope repetitions

Local raw result (gitignored):
`infra/aws-smoke/results/20260730T1900Z-async-batch-repeat/result.json`.

## Repeated result

| Mode | Client ops/s median (range) | End-to-end ops/s median (range) | p99 median | Total core median | Settle median |
|---|---:|---:|---:|---:|---:|
| sync | 121,142 (120,406–121,880) | 121,142 (120,406–121,880) | 1.079 ms | 99.4% | 0 ms |
| envelope | 135,051 (134,135–135,958) | 134,203 (133,395–135,127) | 1.527 ms | 160.4% | 226 ms |

The envelope median improved client-visible throughput by 11.5% and
end-to-end throughput by 10.8%. The latter includes the time from benchmark
completion until all five million logical events were forwarded, so an async
backlog cannot inflate the comparison.

The cost moved rather than disappeared:

- total Redis-process CPU rose by about 61%;
- p99 latency rose by about 42%;
- maximum latency rose from an 11.6 ms median to 13.7 ms; and
- the worker needed about 226 ms after clients finished to settle.

All six capture trials reported:

- exactly 5,000,000 forwarded logical events;
- `events_lost=0`, `dropped=0`, and `handler_panics=0`; and
- `async_worker_errors=0`.

## What made batching work

The first implementation encoded while holding the FIFO mutex. Under load,
92,820 events took the ordered fallback and only 2,979 envelopes were written;
envelope throughput fell to 92,517 ops/s.

Moving encoding outside both the queue mutex and Redis lock raised throughput
to 120,398 ops/s. A small bounded spin around the now-tiny FIFO critical
section, plus 256-event batches, produced the repeated result above:

- fallback rate: 0.142–0.153%;
- logical events actually written in envelopes: 82.65–83.83%;
- average logical events per envelope: 153.6–155.3; and
- queue high-water mark: 1,834–2,450, far below capacity.

The worker still writes some fixed-format entries through the ordered fallback,
so an envelope-mode consumer must accept a mixed stream.

## Async individual is not an optimization

The one-XADD-per-event worker remained slower than sync. In the final
single-repetition probe it delivered 108,632 end-to-end ops/s while consuming
about 140% of a core, versus sync at 119,683 ops/s and about 99% of a core.
Thread handoff and thread-safe-context lock competition add cost without
removing Redis writes.

## Conclusion

Keep `sync` as the default. Async individual is a useful control and fallback
design exercise, not a recommended deployment mode.

`envelope` is a credible, explicitly experimental throughput option when a
second core is available and consumers can decode `batch-v1`. It is not a free
optimization: it trades CPU, tail latency, physical-entry retention semantics,
and consumer complexity for roughly 11% more sustained throughput in this
workload. Larger and longer workload matrices should precede any stability
promotion.
