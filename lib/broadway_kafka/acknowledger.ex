defmodule BroadwayKafka.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger

  @type t :: %{key => value}
  @type key :: {:brod.group_generation_id(), :brod.topic(), :brod.partition()}
  @type value :: {pending_offsets, last_offset :: :brod.offset(), seen_offsets}
  @type pending_offsets :: [:brod.offset()]
  @type seen_offsets :: [:brod.offset()]

  @spec new() :: t
  def new(), do: %{}

  @doc """
  Add keys to the acknowledger.
  """
  @spec add(t, [{:brod.group_generation_id(), :brod.topic(), :brod.partition(), :brod.offset()}]) ::
          t
  def add(acknowledgers, list) do
    for entry <- list,
        {generation_id, topic, partition, offset} = entry,
        do: {{generation_id, topic, partition}, {:ordsets.new(), offset, :ordsets.new()}},
        into: acknowledgers
  end

  @doc """
  Whenever demand is pending, we will call:

      for key <- Acknowledger.keys(acks) do
        Process.send_after(self(), {:poll, key}, @timeout)
      end

  Then on :poll, one should call `last_offset/2`, see if the key is
  still valid. If so, we should poll Kafka and generate messages.
  The `ack_ref` of each message should be `{producer_pid, ack_key}`.
  Once poll is done, `update_last_offset/4` must be called every
  time messages are sent to the client.
  """
  @spec keys(t) :: [key]
  def keys(acknowledgers), do: Map.keys(acknowledgers)

  @doc """
  Returns the last offset for key.

  Returns nil if the key does not exist.
  """
  @spec last_offset(t, key) :: :brod.offset() | nil
  def last_offset(acknowledgers, key) do
    case acknowledgers do
      %{^key => {_, offset, _}} -> offset
      %{} -> nil
    end
  end

  @doc """
  Updates the last offset after every polling.
  """
  @spec update_last_offset(t, key, :brod.offset(), seen_offsets) :: t
  def update_last_offset(acknowledgers, key, last_offset, new_offsets) do
    %{^key => {pending, _, seen}} = acknowledgers
    %{acknowledgers | key => {pending ++ new_offsets, last_offset, seen}}
  end

  @doc """
  Receives a list of offsets update the current key.
  Returns `{drained?, new_offset, t}`. The `drained?`
  value should be used a quick check during draining
  before checking if all partitions are drained.

      {drained?, new_offset, acknowledgers} =
        Acknowledger.update_current_offset(state.acknowledgers, key, offsets)

      if new_offset do
        :brod_group_coordinator.ack(..., new_offset)
      end

      if drained? && state.draining && Acknowledger.all_drained?(acknowledgers) do
        ...
      end
  """
  @spec update_current_offset(t, key, [:brod.offset()]) ::
          {drained? :: boolean, :brod.offset() | nil, t}
  def update_current_offset(acknowledgers, key, offsets) when is_list(offsets) do
    %{^key => {pending, last, seen}} = acknowledgers
    {new_pending, new_seen} = update_offsets(offsets, pending, seen)

    next = List.first(pending) || last
    new_next = List.first(new_pending) || last
    update = if new_next > next, do: new_next - 1, else: nil

    value = {new_pending, last, new_seen}
    {drained?(value), update, %{acknowledgers | key => value}}
  end

  # Discard older offsets
  defp update_offsets([offset | offsets], [current | _] = pending, seen)
       when offset < current,
       do: update_offsets(offsets, pending, seen)

  # Bump latest offset
  defp update_offsets([offset | offsets], [offset | pending], seen),
    do: update_offsets(offsets, pending, seen)

  # Bump from seen
  defp update_offsets(offsets, [current | pending], [current | seen]),
    do: update_offsets(offsets, pending, seen)

  # Merge any left over
  defp update_offsets(offsets, pending, seen),
    do: {pending, :ordsets.union(offsets, seen)}

  @doc """
  Returns if all keys drained.
  """
  @spec all_drained?(t) :: boolean()
  def all_drained?(map) do
    Enum.all?(map, fn {_, v} -> drained?(v) end)
  end

  defp drained?({[], _, []}), do: true
  defp drained?(_), do: false

  @doc """
  The ack callback. It simply sends messages to the annotated producer.
  """
  @spec ack({pid, key}, [Broadway.Message.t()], [Broadway.Message.t()]) ::
          {:ack, key, [non_neg_integer], [non_neg_integer]}
  def ack({producer_pid, key}, successful, failed) do
    successful_offsets = fetch_offsets(successful)
    failed_offsets = fetch_offsets(failed)

    offsets = successful_offsets ++ failed_offsets

    send(producer_pid, {:ack, key, Enum.sort(offsets), Enum.sort(failed_offsets)})
  end

  @spec fetch_offsets([Broadway.Message.t()]) :: [non_neg_integer]
  defp fetch_offsets([]), do: []

  defp fetch_offsets(messages),
    do: Enum.map(messages, fn %{acknowledger: {_, _, %{offset: offset}}} -> offset end)
end
