defmodule BroadwayKafka.Acknowledger do
  @moduledoc false
  @behaviour Broadway.Acknowledger

  @type t :: %{key => value}
  @type key :: {:brod.group_generation_id(), :brod.topic(), :brod.partition()}
  @type value ::
          {current_offset :: :brod.offset(), last_offset :: :brod.offset(), pending_offsets}
  @type pending_offsets :: [:brod.offset()]

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
        do: {{generation_id, topic, partition}, {offset - 1, offset, :ordsets.new()}},
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
  Once poll is done, `update_last_offset/3` must be called every
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
  @spec update_last_offset(t, key, :brod.offset()) :: t
  def update_last_offset(acknowledgers, key, last_offset) do
    %{^key => {current, _, pending}} = acknowledgers
    %{acknowledgers | key => {current, last_offset, pending}}
  end

  @doc """
  Receives a list of offsets update the current key.
  Returns `{drained?, t}`. The `drained?` value should
  be used a quick check during draining before checking
  if all partitions are drained.

      {drained?, new_offset, acknowledgers} = Acknowledger.update_current_offset(state.acknowledgers, key, offsets)

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
    %{^key => {current, last, pending}} = acknowledgers
    {new_current, new_pending} = update_offsets(offsets, current, pending)
    update = if new_current > current, do: new_current, else: nil
    value = {new_current, last, new_pending}
    {drained?(value), update, %{acknowledgers | key => value}}
  end

  # Discard older offsets
  defp update_offsets([offset | offsets], current, all_pending) when offset <= current do
    update_offsets(offsets, current, all_pending)
  end

  # Bump latest offset
  defp update_offsets([offset | offsets], current, all_pending) when current + 1 == offset do
    update_offsets(offsets, offset, all_pending)
  end

  # Bump from pending
  defp update_offsets(offsets, current, [pending | all_pending]) when current + 1 == pending do
    update_offsets(offsets, pending, all_pending)
  end

  # Merge any left over
  defp update_offsets(offsets, current, all_pending) do
    {current, :ordsets.union(offsets, all_pending)}
  end

  defp drained?({offset, last_offset, pending_offsets}) do
    offset + 1 == last_offset and pending_offsets == []
  end

  @doc """
  Returns if all keys drained.
  """
  @spec all_drained?(t) :: boolean()
  def all_drained?(map) do
    Enum.all?(map, fn {_, v} -> drained?(v) end)
  end

  @doc """
  The ack callback. It simples sends messages to the annotated producer.
  """
  def ack({producer_pid, key}, successful, failed) do
    offsets =
      Enum.map(successful ++ failed, fn %{acknowledger: {_, _, %{offset: offset}}} -> offset end)

    send(producer_pid, {:ack, key, Enum.sort(offsets)})
  end
end
