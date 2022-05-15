defmodule BroadwayKafka.ConsumerTest.Config do
  def n_messages do
    10_000
  end

  def last_messages do
    (n_messages() - 2)..n_messages() |> Enum.map(&to_string/1)
  end
end

defmodule BroadwayKafka.ConsumerTest do
  @moduledoc """
  Kafka integration tests.

  # Setup

  1. Create docker-compose.yml file

  ```
  version: '3.9'
  services:
  zookeeper:
    image: wurstmeister/zookeeper
    ports:
      - "127.0.0.1:2181:2181"
  kafka:
    image: wurstmeister/kafka:2.13-2.7.1
    ports:
      - "127.0.0.1:9092:9092"
    environment:
      KAFKA_LISTENERS: "INTERNAL://:29092,EXTERNAL://:9092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:29092,EXTERNAL://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      - zookeeper
  ```

  1. Run Docker
     $ docker compose up -d

  # Running only integration tests

      mix test --only integration

  # Running all tests

      mix test --include integration
  """

  use ExUnit.Case
  require Logger

  @moduletag :integration

  alias BroadwayKafka.ConsumerTest.Config

  defmodule MyBroadway do
    use Broadway

    alias BroadwayKafka.ConsumerTest.Config

    def start_link(context) do
      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        context: context,
        producer: [
          module:
            {BroadwayKafka.Producer,
             [
               hosts: [localhost: 9092],
               group_id: "brod_my_group",
               topics: ["test"],
               receive_interval: 100,
               group_config: [
                 offset_commit_interval_seconds: 1,
                 rejoin_delay_seconds: 2
               ],
               fetch_config: [
                 max_bytes: 10_240
               ]
             ]},
          concurrency: 3
        ],
        processors: [
          default: [
            concurrency: 3
          ]
        ],
        batchers: [
          default: [
            batch_size: 20,
            batch_timeout: 50,
            concurrency: 4
          ]
        ]
      )
    end

    def handle_message(_, message, %{caller_pid: caller_pid}) do
      if message.data in Config.last_messages() do
        send(caller_pid, {:last_message, message.metadata.partition, message.data})
      end

      message
    end

    def handle_batch(_, messages, _info, %{messages_agent: messages_agent}) do
      Agent.update(messages_agent, fn list -> list ++ messages end)
      last_message = List.last(messages)
      last_offset = last_message.metadata.offset
      partition = last_message.metadata.partition

      IO.puts(
        "Batch handled with #{length(messages)} messages. " <>
          "Partition: #{partition} Last offset: #{last_offset}"
      )

      messages
    end
  end

  setup_all do
    topic = "test"
    hosts = [localhost: 9092]

    reset_topic(topic)

    {broadway_pid, messages_agent} = start_broadway()

    # Let's wait for the assignments before start sending messages
    wait_for_assignments(MyBroadway)

    IO.puts("Sending messages...")
    send_messages(Config.n_messages(), hosts, topic)

    [last_message_2, last_message_0, last_message_1] = Config.last_messages()

    receive do
      {:last_message, 2, ^last_message_2} ->
        IO.puts("Got last message from partition 2")
    end

    receive do
      {:last_message, 0, ^last_message_0} ->
        IO.puts("Got last message from partition 0")
    end

    receive do
      {:last_message, 1, ^last_message_1} ->
        IO.puts("Got last message from partition 1")
    end

    # Let's wait a bit to see if we get more messages
    Process.sleep(1000)

    messages = Agent.get(messages_agent, & &1)

    on_exit(fn ->
      stop_broadway(broadway_pid)
    end)

    {:ok, %{broadway_pid: broadway_pid, messages: messages}}
  end

  test "number of processed messages = total messages ", %{messages: messages} do
    assert length(messages) == Config.n_messages()
  end

  test "messages are not duplicated", %{messages: messages} do
    messages_with_count =
      Enum.reduce(messages, %{}, fn msg, acc ->
        Map.update(acc, msg.data, %{count: 1, list: [msg]}, fn %{count: count, list: list} ->
          %{list: [msg | list], count: count + 1}
        end)
      end)

    duplicated_messages = Enum.filter(messages_with_count, fn {_k, v} -> v.count > 1 end)

    assert duplicated_messages == []
  end

  test "order of messages and offsets", %{messages: messages} do
    assert get_ordering_proplems(messages) == []
  end

  defp reset_topic(topic) do
    brokers = [{"localhost", 9092}]

    :brod.delete_topics(brokers, [topic], 1_000)

    topic_config = [
      %{
        config_entries: [],
        num_partitions: 3,
        replica_assignment: [],
        replication_factor: 1,
        topic: topic
      }
    ]

    :brod.create_topics(brokers, topic_config, %{timeout: 1_000})
  end

  defp send_messages(n_messages, hosts, topic) do
    client_id = :test_client
    :ok = :brod.start_client(hosts, client_id, _client_config = [])
    :ok = :brod.start_producer(client_id, topic, _producer_config = [])

    Enum.each(1..n_messages, fn i ->
      partition = rem(i, 3)
      :ok = :brod.produce_sync(client_id, topic, partition, _key = "", "#{i}")
    end)

    :brod.stop_client(client_id)
  end

  defp start_broadway() do
    {:ok, messages_agent} = Agent.start_link(fn -> [] end)
    context = %{messages_agent: messages_agent, caller_pid: self()}
    {:ok, broadway_pid} = MyBroadway.start_link(context)
    {broadway_pid, messages_agent}
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end

  defp get_ordering_proplems(messages) do
    init_acc = %{last_messages: %{0 => nil, 1 => nil, 2 => nil}, problems: []}

    %{problems: ordering_problems} =
      Enum.reduce(messages, init_acc, fn msg, acc ->
        %{last_messages: last_messages, problems: problems} = acc
        partition = msg.metadata.partition

        problems =
          case last_messages[partition] do
            nil ->
              problems

            last_message ->
              if String.to_integer(msg.data) <= String.to_integer(last_message.data) do
                message =
                  "Data out of order #{msg.data}->#{last_message.data} in partition #{partition}"

                [message | problems]
              else
                problems
              end
          end

        last_messages = Map.put(last_messages, partition, msg)
        %{acc | problems: Enum.reverse(problems), last_messages: last_messages}
      end)

    Enum.reverse(ordering_problems)
  end

  defp wait_for_assignments(broadway_name) do
    producers =
      broadway_name
      |> Broadway.producer_names()
      |> Enum.map(fn producer ->
        pid = Process.whereis(producer)
        :erlang.trace(pid, true, [:receive, tracer: self()])
        pid
      end)

    Enum.each(producers, fn pid ->
      receive do
        {:trace, ^pid, :receive, {:put_assignments, _, _}} ->
          IO.puts("Assignment received. Producer: #{inspect(pid)}")
      end
    end)
  end
end
