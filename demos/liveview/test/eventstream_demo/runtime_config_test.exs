defmodule EventstreamDemo.RuntimeConfigTest do
  use ExUnit.Case, async: false

  alias EventstreamDemo.RuntimeConfig

  @key :runtime_settings

  setup do
    previous = Application.get_env(:eventstream_demo, @key)

    on_exit(fn ->
      if previous do
        Application.put_env(:eventstream_demo, @key, previous, persistent: true)
      else
        Application.delete_env(:eventstream_demo, @key, persistent: true)
      end
    end)

    Application.delete_env(:eventstream_demo, @key, persistent: true)
    :ok
  end

  test "normalizes events and stores standalone settings" do
    assert {:ok, %{events: ["expired", "set"], maxlen: 25_000, mode: :standalone} = settings} =
             RuntimeConfig.update(" expired, set,expired ", "25000", "standalone")

    assert RuntimeConfig.current() == settings
  end

  test "accepts the advanced cluster topology" do
    assert {:ok, %{mode: :cluster, events: ["hset"], maxlen: 100}} =
             RuntimeConfig.update("hset", "100", "cluster")
  end

  test "rejects invalid values without replacing the previous settings" do
    assert {:ok, previous} = RuntimeConfig.update("expired", "10000", "standalone")

    assert {:error, "capture at least one event type"} =
             RuntimeConfig.update(" , ", "10000", "standalone")

    assert {:error, "event names may contain lowercase letters, digits, _ and -"} =
             RuntimeConfig.update("Expired", "10000", "standalone")

    assert {:error, "maxlen must be an integer from 100 to 1,000,000"} =
             RuntimeConfig.update("expired", "99", "standalone")

    assert {:error, "runtime mode must be standalone or cluster"} =
             RuntimeConfig.update("expired", "10000", "sentinel")

    assert RuntimeConfig.current() == previous
  end
end
