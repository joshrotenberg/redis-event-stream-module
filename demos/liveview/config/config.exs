# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

module_extension = if match?({:unix, :darwin}, :os.type()), do: "dylib", else: "so"

config :eventstream_demo,
  generators: [timestamp_type: :utc_datetime],
  start_demo_runtime: true,
  redis_port: 6380,
  cluster_base_port: 7100,
  stream_prefix: "events:",
  capture_events: ~w(expired set hset del),
  module_path:
    System.get_env("EVENTSTREAM_MODULE_PATH") ||
      Path.expand(
        "../../../target/release/libredis_event_stream_module.#{module_extension}",
        __DIR__
      )

# Configure the endpoint
config :eventstream_demo, EventstreamDemoWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: EventstreamDemoWeb.ErrorHTML, json: EventstreamDemoWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: EventstreamDemo.PubSub,
  live_view: [signing_salt: "uKKenSNQ"]

# Configure esbuild (the version is required)
config :esbuild,
  version: "0.25.4",
  eventstream_demo: [
    args:
      ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/* --alias:@=.),
    cd: Path.expand("../assets", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

# Configure Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
