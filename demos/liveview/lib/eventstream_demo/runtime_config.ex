defmodule EventstreamDemo.RuntimeConfig do
  @moduledoc """
  In-BEAM configuration for the disposable Redis runtime.

  Storing this in application environment is intentional for the demo: values
  survive stop/start cycles of the runtime supervisor without introducing a
  database or another long-lived process.
  """

  @key :runtime_settings

  def current do
    defaults = %{
      events: Application.fetch_env!(:eventstream_demo, :capture_events),
      maxlen: 10_000,
      mode: :standalone
    }

    Map.merge(defaults, Application.get_env(:eventstream_demo, @key, %{}))
  end

  def update(events, maxlen) when is_binary(events) and is_binary(maxlen) do
    update(events, maxlen, Atom.to_string(current().mode))
  end

  def update(events, maxlen, mode)
      when is_binary(events) and is_binary(maxlen) and is_binary(mode) do
    with {:ok, events} <- parse_events(events),
         {:ok, maxlen} <- parse_maxlen(maxlen),
         {:ok, mode} <- parse_mode(mode) do
      settings = %{events: events, maxlen: maxlen, mode: mode}
      Application.put_env(:eventstream_demo, @key, settings, persistent: true)
      {:ok, settings}
    end
  end

  defp parse_events(value) do
    events =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    cond do
      events == [] ->
        {:error, "capture at least one event type"}

      Enum.any?(events, &(not Regex.match?(~r/^[a-z0-9_-]+$/, &1))) ->
        {:error, "event names may contain lowercase letters, digits, _ and -"}

      true ->
        {:ok, events}
    end
  end

  defp parse_maxlen(value) do
    case Integer.parse(value) do
      {maxlen, ""} when maxlen >= 100 and maxlen <= 1_000_000 -> {:ok, maxlen}
      _ -> {:error, "maxlen must be an integer from 100 to 1,000,000"}
    end
  end

  defp parse_mode("standalone"), do: {:ok, :standalone}
  defp parse_mode("cluster"), do: {:ok, :cluster}
  defp parse_mode(_value), do: {:error, "runtime mode must be standalone or cluster"}
end
