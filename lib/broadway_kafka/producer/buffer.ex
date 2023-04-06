defmodule BroadwayKafka.Producer.Buffer do
  @moduledoc false

  # Implemenet Ring buffer using :queue
  # On enqueue 
  # Each item in the queue has the full buffer for a given key

  @type t :: :queue.queue()
  @type generation_id :: non_neg_integer
  @type topic :: String.t()
  @type partition :: non_neg_integer
  @type key :: {generation_id, topic, partition}

  @spec new :: t
  def new do
    :queue.new()
  end

  @spec enqueue_with_key(t, key, list :: [Broadway.Message.t()]) :: t
  @doc false
  def enqueue_with_key(buffer, _key, []) do
    buffer
  end

  def enqueue_with_key(buffer, key, list) do
    :queue.in({key, list}, buffer)
  end

  @spec empty?(t) :: boolean
  defdelegate empty?(buffer), to: :queue, as: :is_empty
end
