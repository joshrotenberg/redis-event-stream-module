defmodule EventstreamDemo.RedisNode do
  @moduledoc false

  @redis_server EventstreamDemo.RedisServer

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
    port = Application.fetch_env!(:eventstream_demo, :redis_port)
    %{events: events, maxlen: maxlen} = EventstreamDemo.RuntimeConfig.current()

    RedisServerWrapper.Server.start_link(
      name: @redis_server,
      bind: "127.0.0.1",
      port: port,
      save: :disabled,
      appendonly: false,
      loadmodule: [
        {module_path, ["events", Enum.join(events, ","), "maxlen", Integer.to_string(maxlen)]}
      ],
      managed: true
    )
  end
end
