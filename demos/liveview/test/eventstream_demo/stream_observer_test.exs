defmodule EventstreamDemo.StreamObserverTest do
  use ExUnit.Case, async: true

  alias EventstreamDemo.StreamObserver

  test "parses data entries and control markers while advancing each cursor" do
    reply = [
      [
        "events:set",
        [
          ["100-0", ["event", "set", "key", "demo:key", "db", "0"]],
          ["101-0", ["event", "set", "key", "demo:next", "db", "0"]]
        ]
      ],
      [
        "events:#control",
        [["102-0", ["action", "disabled", "module-version", "0.3.0"]]]
      ]
    ]

    {entries, cursors} =
      StreamObserver.parse_xread(
        reply,
        %{"events:set" => "99-0", "events:#control" => "99-0"},
        "events:#control"
      )

    assert [
             %{kind: :entry, event: "set", key: "demo:key", id: "100-0"},
             %{kind: :entry, event: "set", key: "demo:next", id: "101-0"},
             %{kind: :marker, action: "disabled", id: "102-0"}
           ] = entries

    assert cursors == %{
             "events:set" => "101-0",
             "events:#control" => "102-0"
           }
  end

  test "accepts the RESP3 map shape returned by XREAD" do
    reply = %{
      "events:hset" => [
        ["200-0", ["event", "hset", "key", "demo:hash", "db", "0"]]
      ]
    }

    assert {[%{kind: :entry, event: "hset", key: "demo:hash", id: "200-0"}],
            %{"events:hset" => "200-0"}} =
             StreamObserver.parse_xread(
               reply,
               %{"events:hset" => "199-0"},
               "events:#control"
             )
  end
end
