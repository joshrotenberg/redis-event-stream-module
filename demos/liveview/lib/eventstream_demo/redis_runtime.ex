defmodule EventstreamDemo.RedisRuntime do
  @moduledoc """
  Routes demo traffic through the active standalone or cluster client.

  Cluster-wide lifecycle controls intentionally fan out to every master because
  module configuration and keyspace commands are node-local.
  """

  @client EventstreamDemo.Redis

  def command(args, opts \\ []) do
    case EventstreamDemo.RuntimeConfig.current().mode do
      :cluster -> Redis.Cluster.command(@client, args, opts)
      :standalone -> Redis.command(@client, args, opts)
    end
  end

  def pipeline(commands, opts \\ []) do
    case EventstreamDemo.RuntimeConfig.current().mode do
      :cluster -> Redis.Cluster.pipeline(@client, commands, opts)
      :standalone -> Redis.pipeline(@client, commands, opts)
    end
  end

  def command_all(args, opts \\ []) do
    results =
      EventstreamDemo.RuntimeSupervisor.observer_nodes()
      |> Enum.map(fn node -> Redis.command(node.conn, args, opts) end)

    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> {:ok, Enum.map(results, fn {:ok, reply} -> reply end)}
      {:error, reason} -> {:error, reason}
    end
  end
end
