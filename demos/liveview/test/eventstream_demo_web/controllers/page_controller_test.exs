defmodule EventstreamDemoWeb.EventstreamLiveTest do
  use EventstreamDemoWeb.ConnCase

  test "GET /", %{conn: conn} do
    conn = get(conn, ~p"/")
    body = html_response(conn, 200)

    assert body =~ "Redis Event Stream"
    assert body =~ "Observatory"
    assert body =~ "redis-event-stream-module"
    assert body =~ "Configure capture"
    assert body =~ "Create Redis activity"
    assert body =~ "Watch the streams"
    assert body =~ "Curious how the demo is wired?"
    assert body =~ "redis_server_wrapper"
    assert body =~ "redis_client_ex"
    assert body =~ "Phoenix LiveView"
    assert body =~ "Pulse 240"
    assert body =~ "Pause capture"
    assert body =~ "3-node cluster"
    assert body =~ "Three masters · per-node streams"
    assert body =~ ~s(phx-click="toggle-capture")
    refute body =~ ~s(phx-click="pause-capture")
    refute body =~ ~s(phx-click="resume-capture")
    assert body =~ "incrby"
    assert body =~ "hdel"
  end

  test "renders observer stats, entries, and gap markers", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/")

    send(
      view.pid,
      {:eventstream_stats,
       %{
         forwarded: 12,
         events_lost: 0,
         dropped: 0,
         streams: 2,
         retained: 12,
         rate: 4.0,
         nodes: []
       }}
    )

    send(
      view.pid,
      {:eventstream_batch,
       [
         %{
           kind: :entry,
           event: "set",
           key: "demo:test",
           db: "0",
           id: "100-0",
           node: nil
         },
         %{kind: :marker, action: "disabled", id: "101-0", node: nil}
       ]}
    )

    html = render(view)
    assert html =~ "demo:test"
    assert html =~ "100-0"
    assert html =~ "disabled"
    assert html =~ "101-0"
    assert html =~ "12"
  end
end
