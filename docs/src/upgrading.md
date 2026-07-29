# Upgrading

An in-process module upgrade creates a short capture gap while the old module
is unloaded and the new module is loaded. The control stream makes that window
visible, but it does not recover events that occurred inside it.

## Before the upgrade

Record:

```text
MODULE LIST
CONFIG GET eventstream.*
EVENTSTREAM.STATS
EVENTSTREAM.STREAMS
```

Preserve the immutable settings:

- `eventstream.stream-prefix`
- `eventstream.cluster-streams`
- `eventstream.entry-seq`

They must be supplied again when loading the new artifact.

Quiesce writers if the application cannot tolerate the capture gap.

## Replace the module

Run the unload and load against a controlled instance:

```text
MODULE UNLOAD eventstream
MODULE LOAD /path/to/libredis_event_stream_module.so \
  stream-prefix events: \
  cluster-streams refuse \
  entry-seq no
```

Supply every nondefault module argument used by the deployment, not only the
immutable examples above.

The unload writes an `unloading` marker. The new module records `loaded` on the
next keyspace event, so consumers can bound the gap between those IDs.

## Verify

Repeat the pre-upgrade commands and run an end-to-end probe:

```bash
./demo-preflight.sh -h <host> -p <port>
```

Confirm that:

- `MODULE LIST` reports the expected version;
- configuration matches the recorded values;
- stream discovery still includes historical names;
- the control stream contains the expected boundary;
- loss and panic counters remain zero after the probe.

Counters reset on load. Stream data and the persistent stream registry remain
in Redis.

## Cluster upgrades

Upgrade one master at a time and verify its local module, streams, counters, and
gap markers before continuing. Consumers must keep discovering every master
during a mixed-version rollout. Test the exact rolling procedure before using
Preview cluster mode in production.
