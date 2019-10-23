defmodule BroadwayKafka.ProducerTest do
  use ExUnit.Case

  import Record, only: [defrecord: 2, extract: 2]
  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  defmodule MessageServer do
    def start_link() do
      Agent.start_link(fn -> %{} end)
    end

    def push_messages(server, messages, opts) do
      topic = Keyword.fetch!(opts, :topic)
      partition = Keyword.fetch!(opts, :partition)
      key = key(topic, partition)
      Agent.update(server, fn queue ->
        Map.put(queue, key, (queue[key] || []) ++ messages)
      end)
    end

    def take_messages(server, topic, partition, amount) do
      key = key(topic, partition)
      Agent.get_and_update(server, fn queue ->
        {messages, rest} = Enum.split(queue[key] || [], amount)
        {messages, Map.put(queue, key, rest)}
      end)
    end

    defp key(topic, partition) do
      "#{topic}-#{partition}"
    end
  end

  defmodule FakeKafkaClient do
    @behaviour BroadwayKafka.KafkaClient

    import Record, only: [defrecord: 2, extract: 2]
    defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

    @impl true
    def init(opts), do: {:ok, opts}

    @impl true
    def setup(_stage_pid, _client_id, config) do
      {:ok, config[:test_pid]}
    end

    @impl true
    def fetch(_client_id, topic, partition, offset, _opts, config) do
      n_messages = config[:max_bytes]
      messages = MessageServer.take_messages(config[:message_server], topic, partition, n_messages)
      send(config[:test_pid], {:messages_fetched, length(messages)})

      kafka_messages =
        for {msg, i} <- Enum.with_index(messages, offset) do
          kafka_message(value: msg, offset: i, key: "", ts: nil)
        end

      {:ok, {offset + length(kafka_messages), kafka_messages}}
    end

    @impl true
    def ack(test_pid, _generation_id, topic, partition, offset) do
      info = %{offset: offset, topic: topic, partition: partition, pid: self()}
      send(test_pid, {:ack, info})
    end
  end

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data, message.metadata})
      message
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  defmacro assert_receive_acks(pattern, opts) do
    offsets = Keyword.fetch!(opts, :offsets)
    timeout = Keyword.get(opts, :timeout, 200)

    quote do
      for offset <- unquote(offsets) do
        receive do
          {:ack, unquote(pattern) = received_message} ->
            assert received_message.offset == offset
        after unquote(timeout) ->
          raise "No message matching #{unquote(Macro.to_string(pattern))} after #{unquote(timeout)}ms."
        end
      end
    end
  end

  test "single producer receiving messages from a single topic/partition" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    producer = get_producer(pid)
    put_assignments(producer, [[topic: "topic", partition: 0]])
    MessageServer.push_messages(message_server, 1..5, topic: "topic", partition: 0)

    for msg <- 1..5 do
      assert_receive {:message_handled, ^msg, %{partition: 0}}
    end

    stop_broadway(pid)
  end

  test "single producer receiving messages from multiple topic/partitions" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    producer = get_producer(pid)
    put_assignments(producer, [
      [topic: "topic_1", partition: 0],
      [topic: "topic_1", partition: 1],
      [topic: "topic_2", partition: 0],
      [topic: "topic_2", partition: 1]
    ])

    MessageServer.push_messages(message_server, 1..5, topic: "topic_1", partition: 0)
    MessageServer.push_messages(message_server, 6..10, topic: "topic_1", partition: 1)
    MessageServer.push_messages(message_server, 11..15, topic: "topic_2", partition: 0)
    MessageServer.push_messages(message_server, 16..20, topic: "topic_2", partition: 1)

    for msg <- 1..5 do
      assert_receive {:message_handled, ^msg, %{partition: 0}}
    end

    for msg <- 6..10 do
      assert_receive {:message_handled, ^msg, %{partition: 1}}
    end

    for msg <- 11..15 do
      assert_receive {:message_handled, ^msg, %{partition: 0}}
    end

    for msg <- 16..20 do
      assert_receive {:message_handled, ^msg, %{partition: 1}}
    end

    stop_broadway(pid)
  end

  test "fetch messages by chuncks according to :max_bytes" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    producer = get_producer(pid)
    put_assignments(producer, [[topic: "topic", partition: 0]])

    MessageServer.push_messages(message_server, 1..25, topic: "topic", partition: 0)

    assert_receive {:messages_fetched, 10}
    assert_receive {:messages_fetched, 10}
    assert_receive {:messages_fetched, 5}

    stop_broadway(pid)
  end

  test "keep trying to receive new messages when the queue is empty" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)

    producer = get_producer(pid)
    put_assignments(producer, [[topic: "topic", partition: 0]])

    assert_receive {:messages_fetched, 0}
    MessageServer.push_messages(message_server, 1..10, topic: "topic", partition: 0)
    assert_receive {:messages_fetched, 10}
    assert_receive {:messages_fetched, 0}

    stop_broadway(pid)
  end

  test "messages from the same topic/partition are forwarded to the same processor" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server, producers_stages: 2, processors_stages: 4)

    producer_1 = get_producer(pid, 0)
    producer_2 = get_producer(pid, 1)

    put_assignments(producer_1, [
      [topic: "topic_1", partition: 0, begin_offset: 100],
      [topic: "topic_2", partition: 1, begin_offset: 400]
    ])

    put_assignments(producer_2, [
      [topic: "topic_1", partition: 1, begin_offset: 200],
      [topic: "topic_2", partition: 0, begin_offset: 300]
    ])

    MessageServer.push_messages(message_server, 1..10, topic: "topic_1", partition: 0)
    MessageServer.push_messages(message_server, 1..10, topic: "topic_1", partition: 1)
    MessageServer.push_messages(message_server, 1..10, topic: "topic_2", partition: 0)
    MessageServer.push_messages(message_server, 1..10, topic: "topic_2", partition: 1)

    assert_receive {:ack, %{topic: "topic_1", partition: 0, pid: processor_1}}
    assert_receive {:ack, %{topic: "topic_1", partition: 1, pid: processor_2}}
    assert_receive {:ack, %{topic: "topic_2", partition: 0, pid: processor_3}}
    assert_receive {:ack, %{topic: "topic_2", partition: 1, pid: processor_4}}

    {_, consumer_name} = Process.info(processor_1, :registered_name)
    assert to_string(consumer_name) =~ "Broadway.Processor_default"

    processors = Enum.uniq([processor_1, processor_2, processor_3, processor_4])
    assert length(processors) == 4

    assert_receive_acks(
      %{topic: "topic_1", partition: 0, pid: ^processor_1}, offsets: 101..109
    )
    assert_receive_acks(
      %{topic: "topic_1", partition: 1, pid: ^processor_2}, offsets: 201..209
    )
    assert_receive_acks(
      %{topic: "topic_2", partition: 0, pid: ^processor_3}, offsets: 301..309
    )
    assert_receive_acks(
      %{topic: "topic_2", partition: 1, pid: ^processor_4}, offsets: 401..409
    )

    stop_broadway(pid)
  end

  test "messages from the same topic/partition are forwarded to the same batch consumer" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server,
      producers_stages: 2,
      processors_stages: 4,
      batchers_stages: 4
    )

    producer_1 = get_producer(pid, 0)
    producer_2 = get_producer(pid, 1)

    put_assignments(producer_1, [
      [topic: "topic_1", partition: 0, begin_offset: 100],
      [topic: "topic_2", partition: 1, begin_offset: 400]
    ])

    put_assignments(producer_2, [
      [topic: "topic_1", partition: 1, begin_offset: 200],
      [topic: "topic_2", partition: 0, begin_offset: 300]
    ])

    MessageServer.push_messages(message_server, 1..10, topic: "topic_1", partition: 0)
    MessageServer.push_messages(message_server, 1..10, topic: "topic_1", partition: 1)
    MessageServer.push_messages(message_server, 1..10, topic: "topic_2", partition: 0)
    MessageServer.push_messages(message_server, 1..10, topic: "topic_2", partition: 1)

    assert_receive {:ack, %{topic: "topic_1", partition: 0, pid: consumer_1}}
    assert_receive {:ack, %{topic: "topic_1", partition: 1, pid: consumer_2}}
    assert_receive {:ack, %{topic: "topic_2", partition: 0, pid: consumer_3}}
    assert_receive {:ack, %{topic: "topic_2", partition: 1, pid: consumer_4}}

    {_, consumer_name} = Process.info(consumer_1, :registered_name)
    assert to_string(consumer_name) =~ "Broadway.Consumer_default"

    processors = Enum.uniq([consumer_1, consumer_2, consumer_3, consumer_4])
    assert length(processors) == 4

    assert_receive_acks(
      %{topic: "topic_1", partition: 0, pid: ^consumer_1}, offsets: 101..109
    )
    assert_receive_acks(
      %{topic: "topic_1", partition: 1, pid: ^consumer_2}, offsets: 201..209
    )
    assert_receive_acks(
      %{topic: "topic_2", partition: 0, pid: ^consumer_3}, offsets: 301..309
    )
    assert_receive_acks(
      %{topic: "topic_2", partition: 1, pid: ^consumer_4}, offsets: 401..409
    )

    stop_broadway(pid)
  end

  test "continue fetching messages after rebalancing" do
    {:ok, message_server} = MessageServer.start_link()
    {:ok, pid} = start_broadway(message_server)
    producer = get_producer(pid)
    put_assignments(producer, [[topic: "topic", partition: 0]])

    assert_receive {:messages_fetched, 0}

    BroadwayKafka.Producer.assignments_revoked(producer)
    put_assignments(producer, [[topic: "topic", partition: 0]])

    assert_receive {:messages_fetched, 0}
    assert_receive {:messages_fetched, 0}

    stop_broadway(pid)
  end

  defp start_broadway(message_server, opts \\ []) do
    producers_stages = Keyword.get(opts, :producers_stages, 1)
    processors_stages = Keyword.get(opts, :processors_stages, 1)
    batchers_stages = Keyword.get(opts, :batchers_stages)

    batchers =
      if batchers_stages do
        [default: [stages: batchers_stages, batch_timeout: 10]]
      else
        []
      end

    Broadway.start_link(Forwarder,
      name: new_unique_name(),
      context: %{test_pid: self()},
      producer: [
        module: {BroadwayKafka.Producer,[
          client: FakeKafkaClient,
          test_pid: self(),
          message_server: message_server,
          receive_interval: 30,
          max_bytes: 10
        ]},
        stages: producers_stages
      ],
      processors: [
        default: [stages: processors_stages]
      ],
      batchers: batchers
    )
  end

  defp put_assignments(producer, assignments) do
    group_member_id = System.unique_integer([:positive])
    group_generation_id = System.unique_integer([:positive])

    kafka_assignments =
      for assignment <- assignments do
        begin_offset = Keyword.get(assignment, :begin_offset, 1)
        brod_received_assignment(topic: assignment[:topic], partition: assignment[:partition], begin_offset: begin_offset)
      end
    BroadwayKafka.Producer.assignments_received(producer, group_member_id, group_generation_id, kafka_assignments)
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp get_producer(broadway, index \\ 0) do
    {_, name} = Process.info(broadway, :registered_name)
    :"#{name}.Broadway.Producer_#{index}"
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
