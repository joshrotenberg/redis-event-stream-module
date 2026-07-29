defmodule EventstreamDemo.Traffic do
  @moduledoc """
  Demo-only traffic controls for making capture behavior visible.
  """

  def pulse(size \\ 120), do: run_async(fn -> mixed_batch(size, 400) end)
  def burst(size \\ 5_000), do: run_async(fn -> mixed_batch(size, 80) end)

  def pause_capture do
    safe_command_all(["CONFIG", "SET", "eventstream.enabled", "no"])
  end

  def resume_capture do
    safe_command_all(["CONFIG", "SET", "eventstream.enabled", "yes"])
  end

  def flush do
    safe_command_all(["FLUSHDB"])
  end

  defp run_async(fun) do
    Task.Supervisor.start_child(EventstreamDemo.TrafficSupervisor, fun)
  end

  defp mixed_batch(size, ttl_ms) do
    token = System.unique_integer([:positive, :monotonic])
    each = max(div(size, 4), 1)

    commands =
      set_commands(token, each) ++
        hash_commands(token, each) ++
        expiry_commands(token, each, ttl_ms) ++
        delete_commands(token, each)

    EventstreamDemo.RedisRuntime.pipeline(commands, timeout: 30_000)
  end

  defp safe_command_all(command) do
    EventstreamDemo.RedisRuntime.command_all(command)
  catch
    :exit, _reason -> {:error, :runtime_stopped}
  end

  defp set_commands(token, count) do
    for i <- 1..count do
      ["SET", "demo:live:set:#{token}:#{i}", Integer.to_string(i)]
    end
  end

  defp hash_commands(token, count) do
    for i <- 1..count do
      [
        "HSET",
        "demo:live:hash:#{token}:#{i}",
        "status",
        "active",
        "sequence",
        Integer.to_string(i)
      ]
    end
  end

  defp expiry_commands(token, count, ttl_ms) do
    for i <- 1..count do
      ["SET", "demo:live:expiry:#{token}:#{i}", "waiting", "PX", Integer.to_string(ttl_ms)]
    end
  end

  defp delete_commands(token, count) do
    Enum.flat_map(1..count, fn i ->
      key = "demo:live:delete:#{token}:#{i}"
      [["SET", key, "temporary"], ["DEL", key]]
    end)
  end
end
