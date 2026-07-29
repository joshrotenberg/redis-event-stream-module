defmodule EventstreamDemo.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      [
        EventstreamDemoWeb.Telemetry,
        {DNSCluster,
         query: Application.get_env(:eventstream_demo, :dns_cluster_query) || :ignore},
        {Phoenix.PubSub, name: EventstreamDemo.PubSub},
        demo_runtime_child(),
        # Start to serve requests, typically the last entry
        EventstreamDemoWeb.Endpoint
      ]
      |> Enum.reject(&is_nil/1)

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: EventstreamDemo.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp demo_runtime_child do
    if Application.get_env(:eventstream_demo, :start_demo_runtime, true) do
      EventstreamDemo.RuntimeSupervisor
    end
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    EventstreamDemoWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
