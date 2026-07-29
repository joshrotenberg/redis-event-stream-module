defmodule EventstreamDemoWeb.Router do
  use EventstreamDemoWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {EventstreamDemoWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", EventstreamDemoWeb do
    pipe_through :browser

    live "/", EventstreamLive
  end

  # Other scopes may use custom stacks.
  # scope "/api", EventstreamDemoWeb do
  #   pipe_through :api
  # end
end
