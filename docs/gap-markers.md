# Gap markers

Every capture gap — a disable, a flush, a swap, an unload, a cluster re-pin — is
made machine-readable by a marker entry written to a control stream at
`<stream-prefix>#control` (default `events:#control`). Each marker carries an
`action` field and a `module-version` field (the `flushed` marker adds a `db`
field). A consumer delimits a gap window by reading marker *pairs*: the span
between a `disabled`/`unloading` marker and the next `enabled`/`loaded` marker is
a window where events were not captured, so reconciliation can be bounded to it
instead of sweeping the whole keyspace.

Two gaps carry no closing marker, by design: a **crash** writes nothing, and a
**clean shutdown** cannot (structurally impossible — investigated in #67). Both
appear afterward as a `loaded` marker with no preceding `unloading`, bounded
below by the last entry ID across the mirrored streams.

The trigger vocabulary, delivery mechanics (why markers are deferred, not
written directly), and the per-node `repinned` marker are included from the
authoritative [specification](./specification.md) (section 9) below. See the
[Loss windows and reconciliation](./loss-windows.md) guide for how to turn a
marker pair into a reconcile plan.

{{#include ../SPEC.md:gap-markers}}
