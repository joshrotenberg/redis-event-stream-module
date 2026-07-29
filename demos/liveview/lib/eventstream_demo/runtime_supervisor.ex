defmodule EventstreamDemo.RuntimeSupervisor do
  @moduledoc """
  Owns the throwaway Redis process, its client connection, and the observer.

  This is intentionally a development/demo topology. The server wrapper keeps
  Redis lifecycle inside the BEAM, while `:rest_for_one` makes every downstream
  process reconnect cleanly if that Redis process is restarted.
  """

  use Supervisor

  @redis EventstreamDemo.Redis
  @cluster_node_names [
    EventstreamDemo.RedisClusterNode1,
    EventstreamDemo.RedisClusterNode2,
    EventstreamDemo.RedisClusterNode3
  ]

  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def running?, do: is_pid(Process.whereis(__MODULE__))

  def stop do
    if running?() do
      Supervisor.terminate_child(EventstreamDemo.Supervisor, __MODULE__)
    else
      :ok
    end
  end

  def start do
    if running?() do
      {:ok, Process.whereis(__MODULE__)}
    else
      Supervisor.restart_child(EventstreamDemo.Supervisor, __MODULE__)
    end
  end

  def restart do
    with :ok <- stop(),
         {:ok, _pid} <- start() do
      :ok
    end
  end

  def reconfigure(events, maxlen, mode \\ nil) do
    mode = mode || Atom.to_string(EventstreamDemo.RuntimeConfig.current().mode)

    with {:ok, settings} <- EventstreamDemo.RuntimeConfig.update(events, maxlen, mode),
         :ok <- restart() do
      {:ok, settings}
    end
  end

  def mode, do: EventstreamDemo.RuntimeConfig.current().mode

  def observer_nodes do
    case mode() do
      :cluster ->
        base_port = Application.fetch_env!(:eventstream_demo, :cluster_base_port)

        @cluster_node_names
        |> Enum.with_index()
        |> Enum.map(fn {conn, index} ->
          port = base_port + index
          %{id: "node-#{index + 1}", conn: conn, host: "127.0.0.1", port: port}
        end)

      :standalone ->
        port = Application.fetch_env!(:eventstream_demo, :redis_port)
        [%{id: "standalone", conn: @redis, host: "127.0.0.1", port: port}]
    end
  end

  @impl true
  def init(:ok) do
    module_path = Application.fetch_env!(:eventstream_demo, :module_path)
    prefix = Application.fetch_env!(:eventstream_demo, :stream_prefix)

    unless File.regular?(module_path) do
      raise """
      Redis event-stream module not found at:
        #{module_path}

      Build it from the repository root with:
        cargo build --release

      Or set EVENTSTREAM_MODULE_PATH to a prebuilt module.
      """
    end

    nodes = observer_nodes()

    children =
      topology_children(nodes) ++
        [
          {Task.Supervisor, name: EventstreamDemo.TrafficSupervisor},
          {EventstreamDemo.StreamObserver,
           conn: @redis, prefix: prefix, mode: mode(), nodes: nodes}
        ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp topology_children(nodes) do
    case mode() do
      :cluster ->
        cluster_client =
          {Redis.Cluster,
           name: @redis, nodes: Enum.map(nodes, &{&1.host, &1.port}), timeout: 10_000}

        direct_connections =
          Enum.map(nodes, fn node ->
            Supervisor.child_spec(
              {Redis.Connection,
               name: node.conn,
               host: node.host,
               port: node.port,
               client_name: "eventstream-observer-#{node.id}",
               timeout: 10_000},
              id: node.conn
            )
          end)

        [EventstreamDemo.RedisClusterNode, cluster_client | direct_connections]

      :standalone ->
        port = Application.fetch_env!(:eventstream_demo, :redis_port)

        [
          EventstreamDemo.RedisNode,
          {Redis.Connection,
           name: @redis,
           host: "127.0.0.1",
           port: port,
           client_name: "eventstream-liveview-demo",
           timeout: 10_000}
        ]
    end
  end
end
