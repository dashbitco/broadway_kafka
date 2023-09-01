defmodule BroadwayKafka.Producer.Buffer do
  @moduledoc false

  # Implemenet Ring buffer using :queue
  #
  # Each item in the queue contains {key, list}
  # which key is {generation_id, topic, partition} and list is
  # the full messages to be read for this topic/partition.
  # 
  # When dequeuing messages, `dequeue` pops out an item in the queue
  # takes `count` amount of messages for that {key, list} and put the rest back
  # into the end of the queue.
  #
  # This approach ensures a more equitable consumption of all partitions.

  alias Broadway.Message

  @type t :: :queue.queue()
  @type generation_id :: non_neg_integer
  @type topic :: String.t()
  @type partition :: non_neg_integer
  @type key :: {generation_id, topic, partition}

  @spec new :: t
  @doc false
  def new do
    :queue.new()
  end

  @spec enqueue_with_key(t, key, list :: [Message.t()], at :: :rear | :front) :: t
  @doc false
  def enqueue_with_key(buffer, key, list, at \\ :rear)

  def enqueue_with_key(buffer, _key, [], _at) do
    buffer
  end

  def enqueue_with_key(buffer, key, list, at) do
    if at == :rear do
      :queue.in({key, list}, buffer)
    else
      :queue.in_r({key, list}, buffer)
    end
  end

  @spec empty?(t) :: boolean
  @doc false
  defdelegate empty?(buffer), to: :queue, as: :is_empty

  @spec dequeue(t, count :: non_neg_integer) ::
          {t, list({key, dequeued :: list(Message.t()), last_item :: Message.t()}),
           dequeued_count :: non_neg_integer}
  @doc false
  def dequeue(buffer, count) when count > 0 do
    {buffer, reversed_acc, count_credit_left} = do_dequeue_many(buffer, count, [])
    {buffer, reversed_acc, count - count_credit_left}
  end

  defp do_dequeue_many(buffer, _count = 0, acc) do
    {buffer, acc, 0}
  end

  defp do_dequeue_many(buffer, count, acc) do
    case queue_out(buffer, count) do
      {new_buffer, {_key, _items, _last_item, _len = 0}} ->
        {new_buffer, acc, count}

      {new_buffer, {key, items, last_item, len}} ->
        new_count = count - len
        ret = {key, items, last_item}
        do_dequeue_many(new_buffer, new_count, [ret | acc])
    end
  end

  @spec queue_out(t, count :: non_neg_integer) ::
          {t, {key, list(Message.t()), Message.t(), len :: non_neg_integer}}
  defp queue_out(buffer, count) do
    case :queue.out(buffer) do
      {{:value, {key, list}}, buffer} ->
        {items, last_item, len, rest} = split(list, count)
        buffer = enqueue_with_key(buffer, key, rest)
        record = {key, items, last_item, len}
        {buffer, record}

      {:empty, buffer} ->
        {buffer, {_key = nil, _items = [], _last_item = nil, _len = 0}}
    end
  end

  defp split(list, count) do
    split_list(list, count, nil, 0, [])
  end

  defp split_list(rest, 0, last_item, length_of_new_list, acc) do
    {Enum.reverse(acc), last_item, length_of_new_list, rest}
  end

  defp split_list([], _count, last_item, length_of_new_list, acc) do
    {Enum.reverse(acc), last_item, length_of_new_list, []}
  end

  defp split_list([head | tail], count, _last_item, length_of_new_list, acc) do
    split_list(tail, count - 1, head, length_of_new_list + 1, [head | acc])
  end
end
