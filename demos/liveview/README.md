# Redis Event Stream Observatory

The canonical interactive demo for `redis-event-stream-module`.

The observatory makes the full path visible:

```text
Configure capture  →  Send commands or generate traffic  →  Watch event lanes
```

- `redis_server_wrapper` owns a disposable standalone Redis server or a real
  three-master cluster and loads the module on every node.
- `redis_client_ex` sends commands and routes cluster traffic by hash slot.
- One observer discovers node-local streams, batches `XREAD` results, and
  publishes them through Phoenix.PubSub.
- Phoenix LiveView renders the shared state in every connected browser.

The UI can start, stop, restart, and reconfigure the Redis topology without
restarting Phoenix. It includes a command console, mixed workloads, continuous
flow, capture-gap controls, counters, gap markers, and node-aware cluster
lanes.

## Run with Docker

Build from the repository root:

```bash
docker build \
  -f demos/liveview/Dockerfile \
  -t redis-event-stream-observatory .
```

Start the self-contained image:

```bash
docker run --rm -p 4000:4000 redis-event-stream-observatory
```

Open [http://127.0.0.1:4000](http://127.0.0.1:4000).

Only the web port is published. The wrapper-owned Redis processes and
cluster-bus ports remain private to the container.

## Run from source

Build the native module from the repository root:

```bash
cargo build --release
```

Then start the demo:

```bash
cd demos/liveview
mix setup
mix phx.server
```

Override the module artifact when needed:

```bash
EVENTSTREAM_MODULE_PATH=/path/to/libredis_event_stream_module.so \
  mix phx.server
```

The demo starts in standalone mode on `127.0.0.1:6380`; it does not use Redis
on the default port. Selecting **3-node cluster** and **Apply + rerun** replaces
it with three wrapper-owned masters on ports 7100–7102.

## Observation model

The observatory uses `XREAD`, not a consumer group. It is a fan-out viewer: one
backend observer reads each entry once and broadcasts it to every browser.
Consumer groups split entries among workers and add acknowledgement state,
which is appropriate for durable work distribution but not a shared display.

Entries are read in batches of up to 1,000 every 80ms. In cluster mode the
observer discovers streams directly on each master, reads each co-located
stream set, merges entries by stream ID, and preserves the source node. The UI
retains only the newest 36 rows in each visible lane while counters show the
full session totals.

## Security boundary

This is a local evaluation environment with lifecycle and arbitrary command
access to disposable Redis instances.

The container sets `LOCAL_DEMO=true`, which enables a fixed development signing
key and relaxes origin checks so `docker run` works without setup. Do not expose
that configuration as a production Redis control plane.

For a nonlocal deployment:

- disable `LOCAL_DEMO`;
- provide `SECRET_KEY_BASE` and `PHX_HOST`;
- configure TLS, authentication, and network controls;
- restrict or remove arbitrary command and lifecycle controls.

See the main [observatory guide](../../docs/src/observatory.md) for the
evaluator workflow.
