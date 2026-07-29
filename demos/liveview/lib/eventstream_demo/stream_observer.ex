defmodule EventstreamDemo.StreamObserver do
  @moduledoc """
  Discovers module-owned streams and broadcasts a batched, read-only live feed.

  This deliberately uses `XREAD`, not a consumer group: one BEAM observer reads
  once and Phoenix.PubSub fans the same observations out to every LiveView.
  """

  use GenServer

  alias Redis.Commands.Stream

  @topic "eventstream"
  @poll_ms 80
  @discovery_ms 1_000
  @stats_ms 1_000
  @read_count 1_000

  defstruct [
    :conn,
    :prefix,
    :mode,
    :nodes,
    :last_stats_at,
    cursors: %{},
    stream_nodes: %{},
    control_streams: MapSet.new(),
    initialized?: false,
    interval_events: 0
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    prefix = Keyword.fetch!(opts, :prefix)

    state = %__MODULE__{
      conn: Keyword.fetch!(opts, :conn),
      prefix: prefix,
      mode: Keyword.get(opts, :mode, :standalone),
      nodes: Keyword.fetch!(opts, :nodes),
      last_stats_at: System.monotonic_time(:millisecond)
    }

    send(self(), :discover)
    Process.send_after(self(), :poll, @poll_ms)
    Process.send_after(self(), :stats, @stats_ms)

    {:ok, state}
  end

  @impl true
  def handle_info(:discover, state) do
    state = discover(state)
    Process.send_after(self(), :discover, @discovery_ms)
    {:noreply, state}
  end

  def handle_info(:poll, state) do
    state = poll(state)
    Process.send_after(self(), :poll, @poll_ms)
    {:noreply, state}
  end

  def handle_info(:stats, state) do
    now = System.monotonic_time(:millisecond)
    elapsed = max(now - state.last_stats_at, 1)

    stats =
      state
      |> read_stats()
      |> Map.put(:rate, Float.round(state.interval_events * 1_000 / elapsed, 1))

    Phoenix.PubSub.broadcast(
      EventstreamDemo.PubSub,
      @topic,
      {:eventstream_stats, stats}
    )

    Process.send_after(self(), :stats, @stats_ms)
    {:noreply, %{state | interval_events: 0, last_stats_at: now}}
  end

  defp discover(state) do
    {stream_nodes, successful_nodes, errors} =
      Enum.reduce(state.nodes, {%{}, 0, []}, fn node, {streams_acc, ok_acc, errors_acc} ->
        case Redis.command(node.conn, ["EVENTSTREAM.STREAMS"]) do
          {:ok, streams} when is_list(streams) ->
            data_streams =
              streams
              |> Enum.filter(&is_binary/1)
              |> Enum.reject(&String.contains?(&1, "#"))
              |> Enum.uniq()

            node_streams =
              data_streams
              |> Enum.reduce(%{}, &Map.put(&2, &1, node.id))
              |> add_control_streams(data_streams, node.id, state)

            {Map.merge(streams_acc, node_streams), ok_acc + 1, errors_acc}

          {:error, reason} ->
            {streams_acc, ok_acc, [{node.id, reason} | errors_acc]}

          _other ->
            {streams_acc, ok_acc, errors_acc}
        end
      end)

    if successful_nodes == 0 and errors != [] do
      broadcast_status({:down, inspect(errors)})
    end

    wanted = Map.keys(stream_nodes)
    control_streams = control_streams(stream_nodes)
    state = %{state | stream_nodes: stream_nodes, control_streams: control_streams}

    cursors =
      state.cursors
      |> Map.take(wanted)
      |> add_new_streams(state, wanted)

    %{state | cursors: cursors, initialized?: successful_nodes > 0}
  end

  defp add_new_streams(cursors, state, wanted) do
    Enum.reduce(wanted, cursors, fn stream, acc ->
      Map.put_new_lazy(acc, stream, fn ->
        if state.initialized?, do: "0-0", else: newest_id(state, stream)
      end)
    end)
  end

  defp newest_id(state, stream) do
    node = node_for_stream(state, stream)

    case Redis.command(node.conn, Stream.xrevrange(stream, "+", "-", count: 1)) do
      {:ok, [[id, _fields]]} when is_binary(id) -> id
      _ -> "0-0"
    end
  end

  defp poll(%{cursors: cursors} = state) when map_size(cursors) == 0, do: state

  defp poll(state) do
    {entries, cursors, successful_nodes, errors} =
      state.cursors
      |> Enum.group_by(fn {stream, _id} -> Map.fetch!(state.stream_nodes, stream) end)
      |> Enum.reduce({[], state.cursors, 0, []}, fn {node_id, streams},
                                                    {entries_acc, cursors_acc, ok_acc, errors_acc} ->
        node = Enum.find(state.nodes, &(&1.id == node_id))
        command = Stream.xread(streams: Enum.sort(streams), count: @read_count)

        case Redis.command(node.conn, command, timeout: 5_000) do
          {:ok, nil} ->
            {entries_acc, cursors_acc, ok_acc + 1, errors_acc}

          {:ok, reply} when is_list(reply) or is_map(reply) ->
            {parsed, next_cursors} =
              parse_xread(reply, cursors_acc, state.control_streams, state.stream_nodes)

            {entries_acc ++ parsed, next_cursors, ok_acc + 1, errors_acc}

          {:error, reason} ->
            {entries_acc, cursors_acc, ok_acc, [{node_id, reason} | errors_acc]}

          _other ->
            {entries_acc, cursors_acc, ok_acc, errors_acc}
        end
      end)

    if entries != [] do
      entries = Enum.sort_by(entries, &stream_id_key(&1.id))

      Phoenix.PubSub.broadcast(
        EventstreamDemo.PubSub,
        @topic,
        {:eventstream_batch, entries}
      )
    end

    cond do
      successful_nodes > 0 -> broadcast_status(:live)
      errors != [] -> broadcast_status({:down, inspect(errors)})
      true -> :ok
    end

    %{
      state
      | cursors: cursors,
        interval_events: state.interval_events + count_data_entries(entries)
    }
  end

  @doc false
  def parse_xread(reply, cursors, control_stream) when is_map(reply) do
    reply
    |> Enum.map(fn {stream, entries} -> [stream, entries] end)
    |> parse_xread(cursors, control_stream)
  end

  def parse_xread(reply, cursors, control_stream) do
    control_streams = MapSet.new(List.wrap(control_stream))
    parse_xread(reply, cursors, control_streams, %{})
  end

  defp parse_xread(reply, cursors, control_streams, stream_nodes) when is_map(reply) do
    reply
    |> Enum.map(fn {stream, entries} -> [stream, entries] end)
    |> parse_xread(cursors, control_streams, stream_nodes)
  end

  defp parse_xread(reply, cursors, control_streams, stream_nodes) do
    Enum.reduce(reply, {[], cursors}, fn
      [stream, raw_entries], {entries_acc, cursor_acc} when is_list(raw_entries) ->
        parsed =
          Enum.flat_map(raw_entries, fn
            [id, fields] when is_binary(id) and is_list(fields) ->
              [
                to_event(
                  stream,
                  id,
                  fields,
                  MapSet.member?(control_streams, stream),
                  Map.get(stream_nodes, stream)
                )
              ]

            _ ->
              []
          end)

        cursor =
          case List.last(raw_entries) do
            [id, _fields] when is_binary(id) -> id
            _ -> Map.get(cursor_acc, stream, "0-0")
          end

        {entries_acc ++ parsed, Map.put(cursor_acc, stream, cursor)}

      _other, acc ->
        acc
    end)
  end

  defp to_event(stream, id, fields, control?, node) do
    fields = fields |> Enum.chunk_every(2) |> Map.new(fn [key, value] -> {key, value} end)

    if control? do
      %{
        kind: :marker,
        id: id,
        stream: stream,
        node: node,
        action: Map.get(fields, "action", "unknown"),
        db: Map.get(fields, "db")
      }
    else
      %{
        kind: :entry,
        id: id,
        stream: stream,
        node: node,
        event: Map.get(fields, "event", "unknown"),
        key: Map.get(fields, "key", ""),
        db: Map.get(fields, "db", "0")
      }
    end
  end

  defp count_data_entries(entries) do
    Enum.count(entries, &(&1.kind == :entry))
  end

  defp stream_id_key(id) do
    case String.split(id, "-", parts: 2) do
      [milliseconds, sequence] ->
        {parse_integer(milliseconds), parse_integer(sequence)}

      _ ->
        {0, 0}
    end
  end

  defp parse_integer(value) do
    case Integer.parse(value) do
      {integer, _rest} -> integer
      :error -> 0
    end
  end

  defp read_stats(state) do
    nodes = Enum.map(state.nodes, &read_node_stats(&1, state))

    %{
      forwarded: sum_node_stat(nodes, :forwarded),
      events_lost: sum_node_stat(nodes, :events_lost),
      dropped: sum_node_stat(nodes, :dropped),
      streams: sum_node_stat(nodes, :streams),
      retained: sum_node_stat(nodes, :retained),
      nodes: nodes
    }
  end

  defp read_node_stats(node, state) do
    {info, status} =
      case Redis.command(node.conn, ["INFO", "eventstream"]) do
        {:ok, value} when is_binary(value) -> {parse_info(value), :live}
        _ -> {%{}, :down}
      end

    data_streams =
      state.stream_nodes
      |> Enum.filter(fn {stream, node_id} ->
        node_id == node.id and not String.contains?(stream, "#")
      end)
      |> Enum.map(&elem(&1, 0))

    retained =
      case data_streams do
        [] ->
          0

        streams ->
          commands = Enum.map(streams, &Stream.xlen/1)

          case Redis.pipeline(node.conn, commands) do
            {:ok, lengths} -> Enum.sum(Enum.filter(lengths, &is_integer/1))
            _ -> 0
          end
      end

    %{
      id: node.id,
      host: node.host,
      port: node.port,
      status: status,
      role: if(state.mode == :cluster, do: :master, else: :standalone),
      tag: blank_to_nil(Map.get(info, "eventstream_cluster_pinned_tag")),
      forwarded: counter(info, "eventstream_forwarded"),
      events_lost: counter(info, "eventstream_events_lost"),
      dropped: counter(info, "eventstream_dropped"),
      streams: length(data_streams),
      retained: retained
    }
  end

  defp sum_node_stat(nodes, key), do: Enum.sum(Enum.map(nodes, &Map.fetch!(&1, key)))

  defp blank_to_nil(nil), do: nil
  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  defp add_control_streams(stream_nodes, data_streams, node_id, state) do
    data_streams
    |> Enum.map(&control_stream_for(&1, state))
    |> Enum.reject(&is_nil/1)
    |> Enum.reduce(stream_nodes, &Map.put(&2, &1, node_id))
  end

  defp control_stream_for(_stream, %{mode: :standalone, prefix: prefix}) do
    prefix <> "#control"
  end

  defp control_stream_for(stream, %{mode: :cluster, prefix: prefix}) do
    escaped = Regex.escape(prefix)

    case Regex.run(~r/^#{escaped}(\{[^}]+\})/, stream, capture: :all_but_first) do
      [tag] -> prefix <> tag <> "#control"
      _ -> nil
    end
  end

  defp control_streams(stream_nodes) do
    stream_nodes
    |> Map.keys()
    |> Enum.filter(&String.contains?(&1, "#control"))
    |> MapSet.new()
  end

  defp node_for_stream(state, stream) do
    node_id = Map.fetch!(state.stream_nodes, stream)
    Enum.find(state.nodes, &(&1.id == node_id))
  end

  defp parse_info(info) do
    info
    |> String.split(["\r\n", "\n"], trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Map.new(fn line ->
      case String.split(line, ":", parts: 2) do
        [key, value] -> {key, value}
        [key] -> {key, ""}
      end
    end)
  end

  defp counter(info, name) do
    info
    |> Map.get(name, "0")
    |> Integer.parse()
    |> case do
      {value, _rest} -> value
      :error -> 0
    end
  end

  defp broadcast_status(status) do
    Phoenix.PubSub.broadcast(
      EventstreamDemo.PubSub,
      @topic,
      {:eventstream_status, status}
    )
  end
end
