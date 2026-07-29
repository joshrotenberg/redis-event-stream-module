defmodule EventstreamDemo.CommandConsole do
  @moduledoc """
  Runs commands against the isolated demo Redis instance.

  This is deliberately unrestricted: the entire Redis process and its data are
  disposable and owned by the demo. It must not be pointed at a shared server.
  """

  def run(line) when is_binary(line) do
    args = OptionParser.split(line)

    case args do
      [] ->
        {:error, "enter a Redis command"}

      args ->
        safe_command(args)
    end
  end

  defp safe_command(args) do
    EventstreamDemo.RedisRuntime.command(args, timeout: 15_000)
  catch
    :exit, _reason -> {:error, "Redis runtime is stopped"}
  end
end
