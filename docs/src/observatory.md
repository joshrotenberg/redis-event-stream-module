# Live observatory

The Redis Event Stream Observatory is the fastest way to explore the complete
module lifecycle. It owns disposable Redis processes, loads the local module,
sends commands through a real Redis client, and pushes captured events into a
Phoenix LiveView interface.

The workflow is deliberately visible:

```text
Configure capture  →  Send commands or generate traffic  →  Watch event lanes
```

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

Only the web port is published. The Redis processes and cluster-bus ports stay
inside the container.

## Follow the workflow

### 1. Configure capture

Choose the event names and retention cap, then apply the configuration. The
default set shows expirations, string writes, hash writes, and deletes. The
page suggests additional event names that can be added directly.

Standalone mode owns one Redis server on port 6380. The advanced three-node
mode replaces it with three masters and enables the module's per-node cluster
streams.

### 2. Create Redis activity

Use the command console for a specific Redis command, or choose a generated
workload:

- **Pulse** creates a small mix of strings, hashes, expirations, and deletes.
- **Burst** makes stream growth and batching visible.
- **Flow** produces continuous activity until stopped.
- **Pause capture** creates an explicit disabled/enabled gap.
- **Flush** shows the control-stream marker produced by a destructive Redis
  lifecycle event.

### 3. Watch the streams

Each lane represents one captured event type. New entries rise to the top and
show their key, origin database, stream ID, and source node in cluster mode.
Counter tiles continue to report the full session even though the UI retains
only a small visible window.

Cluster mode also displays each master, its port, its pinned hash tag, and its
local stream count. Writes are routed by slot; observation fans out to every
master and merges the node-local streams for display.

## What each component does

| Component | Role |
|---|---|
| `redis_server_wrapper` | Owns the disposable standalone server or three-master cluster and loads the module |
| `redis_client_ex` | Sends commands and routes cluster traffic by hash slot |
| Stream observer | Discovers node-local streams, batches `XREAD` results, and publishes them once |
| Phoenix LiveView | Renders shared server-pushed state without polling from each browser |

The observer uses `XREAD`, not a consumer group, because the observatory is a
fan-out viewer. Consumer groups split work among consumers and carry
acknowledgement state, which is the opposite of a shared live display.

## Run from source

Build the native module, then start Phoenix:

```bash
cargo build --release
cd demos/liveview
mix setup
mix phx.server
```

Override the artifact location with `EVENTSTREAM_MODULE_PATH` when needed.

## Security boundary

The Docker image sets `LOCAL_DEMO=true`, which enables a fixed signing key and
relaxes origin checks for local evaluation. It also gives the UI lifecycle and
command access to disposable Redis instances. Do not expose that configuration
as a production control plane.

For any nonlocal deployment, disable local-demo mode, provide
`SECRET_KEY_BASE` and `PHX_HOST`, configure TLS and authentication, and review
the command surface for the intended users.
