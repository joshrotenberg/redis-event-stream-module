defmodule EventstreamDemoWeb.PageController do
  use EventstreamDemoWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
