defmodule EventstreamDemoWeb.EventstreamLive do
  use EventstreamDemoWeb, :live_view

  @topic "eventstream"
  @max_rows 36

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(EventstreamDemo.PubSub, @topic)
    end

    runtime_settings = EventstreamDemo.RuntimeConfig.current()

    {:ok,
     assign(socket,
       page_title: "Redis Event Stream Observatory",
       status: :connecting,
       status_detail: nil,
       stats: %{
         forwarded: 0,
         events_lost: 0,
         dropped: 0,
         streams: 0,
         retained: 0,
         rate: 0.0
       },
       lanes: %{},
       markers: [],
       topology: [],
       runtime_mode: runtime_settings.mode,
       auto?: false,
       capture_paused?: false,
       last_action: "Observer online · waiting for traffic",
       runtime_running?: EventstreamDemo.RuntimeSupervisor.running?(),
       runtime_form:
         to_form(
           %{
             "events" => Enum.join(runtime_settings.events, ","),
             "maxlen" => Integer.to_string(runtime_settings.maxlen),
             "mode" => Atom.to_string(runtime_settings.mode)
           },
           as: :runtime
         ),
       command_form: to_form(%{"command" => "SET demo:console hello"}, as: :console),
       command_history: []
     )}
  end

  @impl true
  def handle_event("pulse", _params, socket) do
    EventstreamDemo.Traffic.pulse(240)
    {:noreply, assign(socket, :last_action, "Sent a 240-key mixed pulse")}
  end

  def handle_event("burst", _params, socket) do
    EventstreamDemo.Traffic.burst(5_000)
    {:noreply, assign(socket, :last_action, "Launching a 5,000-key burst")}
  end

  def handle_event("toggle-flow", _params, socket) do
    auto? = !socket.assigns.auto?

    if auto? do
      send(self(), :traffic_tick)
    end

    message = if auto?, do: "Continuous flow started", else: "Continuous flow stopped"
    {:noreply, assign(socket, auto?: auto?, last_action: message)}
  end

  def handle_event("toggle-capture", _params, socket) do
    {result, paused?, message} =
      if socket.assigns.capture_paused? do
        {EventstreamDemo.Traffic.resume_capture(), false, "Capture resumed"}
      else
        {EventstreamDemo.Traffic.pause_capture(), true,
         "Capture paused · traffic will create a visible gap"}
      end

    case result do
      {:ok, _reply} ->
        {:noreply, assign(socket, capture_paused?: paused?, last_action: message)}

      {:error, reason} ->
        {:noreply, assign(socket, :last_action, "Capture control failed: #{inspect(reason)}")}
    end
  end

  def handle_event("flush", _params, socket) do
    EventstreamDemo.Traffic.flush()
    {:noreply, assign(socket, :last_action, "Redis flushed · watching stream discovery recover")}
  end

  def handle_event("runtime-stop", _params, socket) do
    :ok = EventstreamDemo.RuntimeSupervisor.stop()

    {:noreply,
     assign(socket,
       runtime_running?: false,
       status: :stopped,
       status_detail: nil,
       topology: [],
       auto?: false,
       capture_paused?: false,
       last_action: "Redis runtime stopped · LiveView stayed online"
     )}
  end

  def handle_event("runtime-start", _params, socket) do
    case EventstreamDemo.RuntimeSupervisor.start() do
      {:ok, _pid} ->
        {:noreply,
         assign(socket,
           runtime_running?: true,
           status: :connecting,
           lanes: %{},
           markers: [],
           topology: [],
           capture_paused?: false,
           last_action: "Redis runtime started · reconnecting observer"
         )}

      {:error, reason} ->
        {:noreply, assign(socket, :last_action, "Start failed: #{inspect(reason)}")}
    end
  end

  def handle_event("runtime-restart", _params, socket) do
    case EventstreamDemo.RuntimeSupervisor.restart() do
      :ok ->
        {:noreply,
         assign(socket,
           runtime_running?: true,
           status: :connecting,
           lanes: %{},
           markers: [],
           topology: [],
           auto?: false,
           capture_paused?: false,
           last_action: "Redis runtime restarted · fresh streams incoming"
         )}

      {:error, reason} ->
        {:noreply,
         assign(socket,
           runtime_running?: EventstreamDemo.RuntimeSupervisor.running?(),
           status: :stopped,
           last_action: "Restart failed: #{inspect(reason)}"
         )}
    end
  end

  def handle_event(
        "reconfigure",
        %{"runtime" => %{"events" => events, "maxlen" => maxlen, "mode" => mode}},
        socket
      ) do
    case EventstreamDemo.RuntimeSupervisor.reconfigure(events, maxlen, mode) do
      {:ok, settings} ->
        runtime_form =
          to_form(
            %{
              "events" => Enum.join(settings.events, ","),
              "maxlen" => Integer.to_string(settings.maxlen),
              "mode" => Atom.to_string(settings.mode)
            },
            as: :runtime
          )

        {:noreply,
         assign(socket,
           runtime_form: runtime_form,
           runtime_mode: settings.mode,
           runtime_running?: true,
           status: :connecting,
           lanes: %{},
           markers: [],
           topology: [],
           auto?: false,
           capture_paused?: false,
           last_action:
             "Applied #{Enum.join(settings.events, ",")} · maxlen #{settings.maxlen} · #{mode_label(settings.mode)} restarted"
         )}

      {:error, reason} ->
        {:noreply,
         assign(socket,
           runtime_running?: EventstreamDemo.RuntimeSupervisor.running?(),
           status:
             if(EventstreamDemo.RuntimeSupervisor.running?(),
               do: socket.assigns.status,
               else: :stopped
             ),
           last_action: "Configuration rejected: #{format_reason(reason)}"
         )}
    end
  end

  def handle_event("run-command", %{"console" => %{"command" => command}}, socket) do
    {:noreply, record_command(socket, command)}
  end

  def handle_event("run-preset", %{"command" => command}, socket) do
    {:noreply,
     socket
     |> assign(:command_form, to_form(%{"command" => command}, as: :console))
     |> record_command(command)}
  end

  @impl true
  def handle_info(:traffic_tick, %{assigns: %{auto?: true}} = socket) do
    EventstreamDemo.Traffic.pulse(160)
    Process.send_after(self(), :traffic_tick, 650)
    {:noreply, socket}
  end

  def handle_info(:traffic_tick, socket), do: {:noreply, socket}

  def handle_info({:eventstream_batch, entries}, socket) do
    {lanes, markers} =
      Enum.reduce(entries, {socket.assigns.lanes, socket.assigns.markers}, fn
        %{kind: :entry} = entry, {lanes, markers} ->
          lane =
            lanes
            |> Map.get(entry.event, %{count: 0, entries: []})
            |> Map.update!(:count, &(&1 + 1))
            |> Map.update!(:entries, fn current ->
              [entry | current] |> Enum.take(@max_rows)
            end)

          {Map.put(lanes, entry.event, lane), markers}

        %{kind: :marker} = marker, {lanes, markers} ->
          {lanes, [marker | markers] |> Enum.take(8)}
      end)

    {:noreply, assign(socket, lanes: lanes, markers: markers)}
  end

  def handle_info({:eventstream_stats, stats}, socket) do
    {:noreply,
     assign(socket,
       stats: stats,
       topology: Map.get(stats, :nodes, []),
       status: :live,
       status_detail: nil,
       runtime_running?: true
     )}
  end

  def handle_info({:eventstream_status, :live}, socket) do
    {:noreply, assign(socket, status: :live, status_detail: nil, runtime_running?: true)}
  end

  def handle_info({:eventstream_status, {:down, detail}}, socket) do
    {:noreply, assign(socket, status: :down, status_detail: detail)}
  end

  defp sorted_lanes(lanes) do
    Enum.sort_by(lanes, fn {event, _lane} -> event end)
  end

  defp format_int(value) when is_integer(value) do
    value
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end

  defp format_int(value), do: to_string(value)

  defp format_rate(rate) when is_float(rate) do
    cond do
      rate >= 1_000 -> "#{Float.round(rate / 1_000, 1)}k"
      true -> "#{round(rate)}"
    end
  end

  defp rate_width(rate) do
    min(100, max(4, round(rate / 100)))
  end

  defp event_class("expired"), do: "event-expired"
  defp event_class("set"), do: "event-set"
  defp event_class("hset"), do: "event-hset"
  defp event_class("del"), do: "event-del"
  defp event_class("incrby"), do: "event-incrby"
  defp event_class("hdel"), do: "event-hdel"
  defp event_class(_event), do: "event-other"

  defp event_glyph("expired"), do: "⌁"
  defp event_glyph("set"), do: "+"
  defp event_glyph("hset"), do: "≋"
  defp event_glyph("del"), do: "×"
  defp event_glyph("incrby"), do: "↑"
  defp event_glyph("hdel"), do: "−"
  defp event_glyph(_event), do: "·"

  defp marker_class(action) when action in ["enabled", "loaded"], do: "marker-good"
  defp marker_class(action) when action in ["disabled", "unloading"], do: "marker-bad"
  defp marker_class(_action), do: "marker-warn"

  defp mode_label(:cluster), do: "3-node cluster"
  defp mode_label(:standalone), do: "standalone"

  defp runtime_address(:cluster), do: "3 masters :7100–7102"
  defp runtime_address(:standalone), do: "standalone :6380"

  defp topology_tag(%{tag: nil}), do: "pins on first event"
  defp topology_tag(%{tag: tag}), do: "{#{tag}}"

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(reason), do: inspect(reason)

  defp record_command(socket, command) do
    {state, result} =
      case EventstreamDemo.CommandConsole.run(command) do
        {:ok, value} -> {:ok, inspect(value, limit: 40, printable_limit: 600)}
        {:error, reason} -> {:error, inspect(reason)}
      end

    entry = %{
      id: System.unique_integer([:positive, :monotonic]),
      command: command,
      result: result,
      state: state
    }

    assign(socket,
      command_history: [entry | socket.assigns.command_history] |> Enum.take(7),
      last_action: "Ran #{command}"
    )
  end

  @impl true
  def render(assigns) do
    ~H"""
    <main class="observatory">
      <div class="aurora aurora-one"></div>
      <div class="aurora aurora-two"></div>

      <header class="topbar">
        <div class="brand-lockup">
          <div class="brand-mark" aria-hidden="true">
            <span></span><span></span><span></span>
          </div>
          <div>
            <p class="eyebrow">Redis Event Stream</p>
            <h1>Observatory</h1>
          </div>
        </div>

        <div class={"live-status status-#{@status}"} title={@status_detail}>
          <span class="status-orbit"><i></i></span>
          <span>{if @status == :live, do: "live", else: @status}</span>
        </div>
      </header>

      <section class="hero">
        <div>
          <p class="hero-kicker">
            KEYSPACE TELEMETRY / DB 0 / {String.upcase(mode_label(@runtime_mode))}
          </p>
          <h2>See Redis change<br /><em>as it happens.</em></h2>
          <p class="hero-copy">
            <strong>redis-event-stream-module</strong> turns keyspace changes into bounded,
            replayable streams. Generate real traffic and watch those streams fill in real time.
          </p>
        </div>

        <div class="rate-orb" style={"--rate: #{rate_width(@stats.rate)}%"}>
          <div class="rate-ring">
            <strong>{format_rate(@stats.rate)}</strong>
            <span>events / sec</span>
          </div>
          <div class="rate-sweep"></div>
        </div>
      </section>

      <section class="metric-grid" aria-label="Capture metrics">
        <.metric label="Forwarded" value={format_int(@stats.forwarded)} tone="blue" />
        <.metric label="Retained" value={format_int(@stats.retained)} tone="violet" />
        <.metric label="Streams" value={format_int(@stats.streams)} tone="mint" />
        <.metric label="Events lost" value={format_int(@stats.events_lost)} tone="coral" />
        <.metric label="Dropped writes" value={format_int(@stats.dropped)} tone="amber" />
      </section>

      <nav class="workflow-path" aria-label="Demo workflow">
        <a href="#configure-capture" class="workflow-node">
          <span>01</span>
          <div>
            <strong>Configure capture</strong>
            <small>Choose event names and stream retention.</small>
          </div>
        </a>
        <span class="workflow-arrow" aria-hidden="true">→</span>
        <a href="#send-commands" class="workflow-node">
          <span>02</span>
          <div>
            <strong>Create Redis activity</strong>
            <small>Run a command or generate a workload.</small>
          </div>
        </a>
        <span class="workflow-arrow" aria-hidden="true">→</span>
        <a href="#observe-streams" class="workflow-node">
          <span>03</span>
          <div>
            <strong>Watch the streams</strong>
            <small>See matching events arrive in their lanes.</small>
          </div>
        </a>
      </nav>

      <section class="runtime-lab">
        <article id="configure-capture" class="runtime-card">
          <header class="lab-heading">
            <div>
              <span class="deck-label"><b>01</b> CONFIGURE CAPTURE</span>
              <h3>Redis lifecycle</h3>
            </div>
            <span class={"runtime-state #{if @runtime_running?, do: "is-running", else: "is-stopped"}"}>
              <i></i>
              {if @runtime_running?, do: runtime_address(@runtime_mode), else: "stopped"}
            </span>
          </header>

          <div class="lifecycle-actions">
            <button
              phx-click="runtime-start"
              class="lifecycle-button start"
              disabled={@runtime_running?}
            >
              <span>▶</span> Start
            </button>
            <button
              phx-click="runtime-stop"
              class="lifecycle-button stop"
              disabled={!@runtime_running?}
            >
              <span>■</span> Stop
            </button>
            <button
              phx-click="runtime-restart"
              class="lifecycle-button restart"
              disabled={!@runtime_running?}
            >
              <span>↻</span> Restart
            </button>
          </div>

          <form phx-submit="reconfigure" class="runtime-config">
            <fieldset class="mode-picker">
              <legend>RUNTIME TOPOLOGY</legend>
              <label class={if @runtime_form[:mode].value == "standalone", do: "is-selected"}>
                <input
                  type="radio"
                  name={@runtime_form[:mode].name}
                  value="standalone"
                  checked={@runtime_form[:mode].value == "standalone"}
                />
                <span>
                  <strong>Standalone</strong>
                  <small>One disposable Redis node</small>
                </span>
              </label>
              <label class={if @runtime_form[:mode].value == "cluster", do: "is-selected"}>
                <input
                  type="radio"
                  name={@runtime_form[:mode].name}
                  value="cluster"
                  checked={@runtime_form[:mode].value == "cluster"}
                />
                <span>
                  <strong>3-node cluster</strong>
                  <small>Three masters · per-node streams</small>
                </span>
              </label>
              <em>Advanced</em>
            </fieldset>

            <label>
              <span>CAPTURE EVENTS</span>
              <input
                type="text"
                name={@runtime_form[:events].name}
                value={@runtime_form[:events].value}
                autocomplete="off"
                spellcheck="false"
              />
            </label>
            <label class="maxlen-field">
              <span>MAXLEN</span>
              <input
                type="number"
                name={@runtime_form[:maxlen].name}
                value={@runtime_form[:maxlen].value}
                min="100"
                max="1000000"
              />
            </label>
            <button type="submit" class="apply-config">Apply + rerun</button>
            <p class="capture-hint">
              More to try: <code>incrby</code> · <code>hdel</code>
            </p>
          </form>
        </article>

        <article id="send-commands" class="console-card">
          <header class="lab-heading">
            <div>
              <span class="deck-label"><b>02</b> SEND COMMANDS</span>
              <h3>Fire and observe</h3>
            </div>
            <code>redis_client_ex</code>
          </header>

          <form phx-submit="run-command" class="command-line">
            <span class="prompt">›</span>
            <input
              type="text"
              name={@command_form[:command].name}
              value={@command_form[:command].value}
              autocomplete="off"
              spellcheck="false"
              aria-label="Redis command"
            />
            <button type="submit">Run</button>
          </form>

          <div class="command-presets">
            <button phx-click="run-preset" phx-value-command="SET demo:console hello">SET</button>
            <button
              phx-click="run-preset"
              phx-value-command="HSET demo:console:hash status active"
            >
              HSET
            </button>
            <button phx-click="run-preset" phx-value-command="DEL demo:console">DEL</button>
            <button
              phx-click="run-preset"
              phx-value-command="SET demo:console:ttl waiting PX 800"
            >
              EXPIRE
            </button>
          </div>

          <div class="command-history">
            <p :if={@command_history == []}>Command results will appear here.</p>
            <div
              :for={item <- @command_history}
              id={"command-#{item.id}"}
              class={"command-result result-#{item.state}"}
            >
              <code><span>›</span> {item.command}</code>
              <pre>{item.result}</pre>
            </div>
          </div>
        </article>
      </section>

      <section class="control-deck">
        <div class="control-copy">
          <span class="deck-label">OR GENERATE A WORKLOAD</span>
          <strong>{@last_action}</strong>
        </div>
        <div class="controls">
          <button phx-click="pulse" class="control primary">
            <span class="button-icon">↗</span> Pulse 240
          </button>
          <button phx-click="burst" class="control">
            <span class="button-icon">ϟ</span> Burst 5k
          </button>
          <button phx-click="toggle-flow" class={"control #{if @auto?, do: "active"}"}>
            <span class="button-icon">{if @auto?, do: "■", else: "▶"}</span>
            {if @auto?, do: "Stop flow", else: "Start flow"}
          </button>
          <span class="control-divider"></span>
          <button
            phx-click="toggle-capture"
            class={"control subtle #{if @capture_paused?, do: "capture-paused"}"}
            disabled={!@runtime_running?}
          >
            <span class="button-icon">{if @capture_paused?, do: "▶", else: "Ⅱ"}</span>
            {if @capture_paused?, do: "Resume capture", else: "Pause capture"}
          </button>
          <button phx-click="flush" class="control danger">Flush</button>
        </div>
      </section>

      <section :if={@markers != []} class="marker-stack" aria-label="Gap markers">
        <article
          :for={marker <- @markers}
          id={"marker-#{marker.node || "standalone"}-#{marker.id}"}
          class={"marker #{marker_class(marker.action)}"}
        >
          <span class="marker-pip"></span>
          <strong>{marker.action}</strong>
          <span>capture boundary</span>
          <span :if={@runtime_mode == :cluster && marker.node} class="node-badge">
            {marker.node}
          </span>
          <code>{marker.id}</code>
        </article>
      </section>

      <section id="observe-streams" class="stream-section">
        <div class="section-heading">
          <div>
            <span class="deck-label"><b>03</b> WATCH THE STREAMS</span>
            <h3>Event lanes</h3>
          </div>
          <p>
            Matching events appear in one lane per event type · cluster streams merge in place ·
            newest entries rise to the top · 36 visible per lane
          </p>
        </div>

        <section
          :if={@runtime_mode == :cluster}
          class="cluster-topology"
          aria-label="Redis Cluster topology"
        >
          <header>
            <div>
              <span class="deck-label">LIVE TOPOLOGY</span>
              <strong>One capture stream set per master</strong>
            </div>
            <p>
              <code>redis_client_ex</code> routes writes by slot; the observer fans out
              node-local discovery and merges the results below.
            </p>
          </header>
          <div class="cluster-nodes">
            <article
              :for={node <- @topology}
              class={"cluster-node node-#{node.status}"}
              id={"topology-#{node.id}"}
            >
              <span class="cluster-node-index">{String.replace(node.id, "node-", "0")}</span>
              <div>
                <strong>{node.id}</strong>
                <code>127.0.0.1:{node.port}</code>
              </div>
              <dl>
                <div>
                  <dt>role</dt><dd>{node.role}</dd>
                </div>
                <div>
                  <dt>streams</dt><dd>{node.streams}</dd>
                </div>
                <div>
                  <dt>slot tag</dt><dd>{topology_tag(node)}</dd>
                </div>
              </dl>
            </article>
            <div :if={@topology == []} class="topology-loading">
              Forming cluster and waiting for topology…
            </div>
          </div>
        </section>

        <div :if={map_size(@lanes) == 0} class="empty-signal">
          <div class="empty-radar"><span></span></div>
          <strong>Scanning for signal</strong>
          <p>Send a pulse or start continuous flow.</p>
        </div>

        <div :if={map_size(@lanes) > 0} class="lane-grid">
          <article
            :for={{event, lane} <- sorted_lanes(@lanes)}
            id={"lane-#{event}"}
            class={"lane #{event_class(event)}"}
          >
            <header class="lane-header">
              <div class="lane-name">
                <span class="lane-glyph">{event_glyph(event)}</span>
                <div>
                  <span>EVENT TYPE</span>
                  <strong>{event}</strong>
                </div>
              </div>
              <span class="lane-count">{format_int(lane.count)}</span>
            </header>

            <div class="lane-entries">
              <div
                :for={entry <- lane.entries}
                id={"entry-#{event}-#{entry.node || "standalone"}-#{entry.id}"}
                class="event-row"
              >
                <span class="event-pulse"></span>
                <div class="event-content">
                  <strong>{entry.key}</strong>
                  <span>
                    <b :if={@runtime_mode == :cluster && entry.node} class="node-badge">
                      {entry.node}
                    </b>
                    db {entry.db} · {entry.id}
                  </span>
                </div>
              </div>
            </div>
          </article>
        </div>
      </section>

      <details class="system-map">
        <summary class="system-map-heading">
          <div>
            <span class="deck-label">UNDER THE HOOD</span>
            <h3>Curious how the demo is wired?</h3>
          </div>
          <span class="system-map-toggle">
            <span class="toggle-closed">Explore architecture</span>
            <span class="toggle-open">Hide architecture</span>
            <i aria-hidden="true">+</i>
          </span>
        </summary>

        <div class="system-map-body">
          <p class="system-map-intro">
            A command enters Redis, becomes a durable stream entry, then crosses the BEAM
            to every connected browser. Four pieces make that loop work.
          </p>

          <div class="system-flow">
            <article class="flow-step flow-wrapper">
              <span class="flow-index">01</span>
              <code>redis_server_wrapper</code>
              <strong>Owns the topology</strong>
              <p>
                Starts the disposable server or cluster, supplies its config, and loads the
                native module on every node.
              </p>
            </article>

            <article class="flow-step flow-module">
              <span class="flow-index">02</span>
              <code>redis-event-stream-module</code>
              <strong>Captures changes</strong>
              <p>Hooks selected keyspace events and appends them to bounded, per-event streams.</p>
            </article>

            <article class="flow-step flow-client">
              <span class="flow-index">03</span>
              <code>redis_client_ex</code>
              <strong>Reads the signal</strong>
              <p>
                Routes commands by hash slot while one observer discovers and XREADs every
                node-local stream.
              </p>
            </article>

            <article class="flow-step flow-liveview">
              <span class="flow-index">04</span>
              <code>Phoenix LiveView</code>
              <strong>Fans it out</strong>
              <p>
                PubSub broadcasts each batch; WebSocket patches update metrics, markers, and lanes.
              </p>
            </article>
          </div>

          <div class="system-notes">
            <article>
              <strong>Lifecycle + configuration</strong>
              <p>
                Start, stop, and restart control the wrapper-owned Redis child. Apply + rerun
                reloads the module with a new event filter, maxlen, and runtime topology.
              </p>
            </article>
            <article>
              <strong>Traffic + commands</strong>
              <p>
                The console and generators issue real Redis commands. Matching keyspace changes
                are captured by the module, not synthesized by the UI.
              </p>
            </article>
            <article>
              <strong>Markers + lanes</strong>
              <p>
                Control-stream markers expose capture boundaries. Every event lane represents
                one event type, merged from its module-owned per-node streams in cluster mode.
              </p>
            </article>
          </div>
        </div>
      </details>

      <footer class="demo-footer">
        <div>
          <span>POWERED BY</span>
          <strong>redis-event-stream-module</strong>
        </div>
        <div class="stack-chips">
          <span>Elixir</span>
          <span>Phoenix LiveView</span>
          <span>redis_client_ex</span>
          <span>redis_server_wrapper</span>
        </div>
      </footer>
    </main>
    """
  end

  attr :label, :string, required: true
  attr :value, :string, required: true
  attr :tone, :string, required: true

  defp metric(assigns) do
    ~H"""
    <article class={"metric metric-#{@tone}"}>
      <span class="metric-light"></span>
      <strong>{@value}</strong>
      <span>{@label}</span>
    </article>
    """
  end
end
