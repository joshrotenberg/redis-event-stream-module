defmodule EventstreamDemo.RedisClusterNode do
  @moduledoc false

  @cluster EventstreamDemo.RedisClusterServer

  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [[]]},
      restart: :permanent,
      type: :worker
    }
  end

  def start_link(_opts) do
    module_path = Application.fetch_env!(:eventstream_demo, :module_path)
    base_port = Application.fetch_env!(:eventstream_demo, :cluster_base_port)
    %{events: events, maxlen: maxlen} = EventstreamDemo.RuntimeConfig.current()

    RedisServerWrapper.Cluster.start_link(
      name: @cluster,
      masters: 3,
      replicas_per_master: 0,
      base_port: base_port,
      bind: "127.0.0.1",
      loadmodule: [
        {module_path,
         [
           "events",
           Enum.join(events, ","),
           "maxlen",
           Integer.to_string(maxlen),
           "cluster-streams",
           "per-node"
         ]}
      ],
      managed: true
    )
  end
end
