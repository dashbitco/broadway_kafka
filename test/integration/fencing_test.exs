defmodule BroadwayKafka.FencingIntegrationTest do
  use ExUnit.Case

  @moduletag :integration

  defmodule Pipeline do
    use Broadway

    def start_link(opts) do
      Broadway.start_link(__MODULE__,
        name: opts[:name],
        context: %{label: opts[:label], test_pid: opts[:test_pid]},
        producer: [
          module:
            {BroadwayKafka.Producer,
             [
               hosts: BroadwayKafka.FencingIntegrationTest.hosts(),
               group_id: opts[:group_id],
               topics: [opts[:topic]],
               receive_interval: 50,
               offset_reset_policy: :earliest,
               group_config: [
                 group_instance_id: opts[:group_instance_id],
                 heartbeat_rate_seconds: 1,
                 session_timeout_seconds: 10,
                 rebalance_timeout_seconds: 10
               ]
             ]},
          concurrency: 1
        ],
        processors: [default: [concurrency: 1]]
      )
    end

    @impl true
    def handle_message(_processor, message, context) do
      send(context.test_pid, {:message, context.label, message.data})
      message
    end
  end

  def hosts do
    port =
      "BROADWAY_KAFKA_TEST_PORT"
      |> System.get_env("9092")
      |> String.to_integer()

    [localhost: port]
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end

  test "a replaced static member stops instead of fencing the replacement" do
    suffix = System.unique_integer([:positive, :monotonic])
    topic = "broadway-kafka-fencing-#{suffix}"
    group_id = "broadway-kafka-fencing-group-#{suffix}"
    group_instance_id = "member-1"
    client_id = :"broadway_kafka_fencing_client_#{suffix}"
    first_name = :"BroadwayKafkaFencingFirst#{suffix}"
    second_name = :"BroadwayKafkaFencingSecond#{suffix}"
    handler_id = {__MODULE__, suffix}

    create_topic(topic)

    :ok = :brod.start_client(hosts(), client_id)
    :ok = :brod.start_producer(client_id, topic, [])

    on_exit(fn -> :brod.stop_client(client_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:broadway_kafka, :fenced_instance_id],
        &__MODULE__.handle_telemetry/4,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, first} =
      Pipeline.start_link(
        name: first_name,
        label: :first,
        test_pid: self(),
        topic: topic,
        group_id: group_id,
        group_instance_id: group_instance_id
      )

    on_exit(fn -> stop_broadway(first) end)

    produce(client_id, topic, "before replacement")
    assert_receive {:message, :first, "before replacement"}, 10_000

    first_producer = first_name |> Broadway.producer_names() |> List.first() |> Process.whereis()

    {:ok, second} =
      Pipeline.start_link(
        name: second_name,
        label: :second,
        test_pid: self(),
        topic: topic,
        group_id: group_id,
        group_instance_id: group_instance_id
      )

    on_exit(fn -> stop_broadway(second) end)

    assert_receive {:telemetry, [:broadway_kafka, :fenced_instance_id],
                    %{system_time: system_time},
                    %{
                      producer: ^first_producer,
                      group_id: ^group_id,
                      group_instance_id: ^group_instance_id
                    }},
                   10_000

    assert is_integer(system_time)
    assert Process.alive?(first_producer)

    produce(client_id, topic, "after replacement")
    assert_receive {:message, :second, "after replacement"}, 10_000
    refute_receive {:message, :first, "after replacement"}

    Process.sleep(2_000)
    refute_receive {:telemetry, [:broadway_kafka, :fenced_instance_id], _, _}
  end

  defp create_topic(topic) do
    topic_config = [
      %{
        num_partitions: 1,
        replication_factor: 1,
        name: topic,
        assignments: [],
        configs: []
      }
    ]

    wait_until_create_topic(topic_config)
  end

  defp wait_until_create_topic(topic_config) do
    case :brod.create_topics(hosts(), topic_config, %{timeout: 1_000}) do
      :ok ->
        :ok

      _error ->
        Process.sleep(10)
        wait_until_create_topic(topic_config)
    end
  end

  defp produce(client_id, topic, message) do
    :ok = :brod.produce_sync(client_id, topic, 0, "", message)
  end

  defp stop_broadway(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :normal)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      end
    end
  end
end
