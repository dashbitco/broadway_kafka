require Logger

defmodule MyBroadway do
  use Broadway

  def start_link(context) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      context: context,
      producer: [
        module: {BroadwayKafka.Producer,[
          hosts: [localhost: 9092],
          group_id: "brod_my_group",
          topics: ["test"],
          receive_interval: 100,
          group_config: [
            offset_commit_interval_seconds: 5,
            rejoin_delay_seconds: 2
          ],
          fetch_config: [
            max_bytes: 10_240,
          ]
        ]},
        stages: 3
      ],
      processors: [
        default: [
          stages: 4,
        ]
      ],
      batchers: [
        default: [
          batch_size: 100,
          batch_timeout: 20,
          stages: 4
        ]
      ]
    )
  end

  def handle_message(_, message, %{caller_pid: caller_pid}) do
    # TODO: Find a better way to do this
    if message.data in ["9998", "9999", "10000"] do
      send caller_pid, {:last_message, message.metadata.partition, message.data}
    end
    message
  end

  def handle_batch(_, messages, _info, %{messages_agent: messages_agent}) do
    Agent.update(messages_agent, fn list -> list ++ messages end)
    IO.puts("Batch handled with #{length(messages)} messages")
    messages
  end
end

defmodule Runner do
  def run do
    topic = "test"
    hosts = [localhost: 9092]
    n_messages = 10_000

    reset_topic(topic)

    send_messages(n_messages, hosts, topic)

    {broadway_pid, messages_agent} = start_broadway()

    # TODO: Find a better way to do this
    receive do
      {:last_message, 2, "9998"} ->
        IO.puts("Got last message from partition 2")
    end
    receive do
      {:last_message, 0, "9999"} ->
        IO.puts("Got last message from partition 0")
    end
    receive do
      {:last_message, 1, "10000"} ->
        IO.puts("Got last message from partition 1")
    end

    IO.puts "Let's wait to see if we get more messages"
    Process.sleep(3000)

    IO.puts "Checking producers' message queues"
    for name <- Broadway.producer_names(broadway_pid) do
      {:messages, messages} =
        name
        |> Process.whereis()
        |> Process.info(:messages)

      if messages != [] do
        Logger.error("Message queue from #{name} not empty: \n#{inspect(messages, pretty: true)}")
      end
    end

    stop_broadway(broadway_pid)
    messages = Agent.get(messages_agent, & &1)

    messages_with_count =
      Enum.reduce(messages, %{}, fn msg, acc ->
        Map.update(acc, msg.data, %{count: 1, list: [msg]}, fn %{count: count, list: list} ->
          %{list: [msg|list], count: count + 1}
        end)
      end)

    IO.write "Checking the number of messages..."
    if length(messages) != n_messages do
      Logger.error("Number of messages should be #{n_messages}. Got: #{length(messages)}")
    else
      IO.puts "OK"
    end

    IO.write "Checking duplicated messages..."
    if length(messages) > n_messages do
      duplicated_messages = Enum.filter(messages_with_count, fn {_k, v} -> v.count > 1 end)
      Logger.error("Duplicated messages: \n#{inspect(duplicated_messages, pretty: true)}")
    else
      IO.puts "OK"
    end

    IO.write "Checking order of messages and offsets..."
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
                message = "Data out of order #{msg.data}->#{last_message.data} in partition #{partition}"
                [message | problems]
              else
                problems
              end
          end

        last_messages = Map.put(last_messages, partition, msg)
        %{acc | problems: Enum.reverse(problems), last_messages: last_messages}
      end)

    if ordering_problems != [] do
      Logger.error("Number of ordering problems: #{length(ordering_problems)}. Showing first 5:")
      for problem <- Enum.reverse(ordering_problems) |> Enum.take(5)  do
        Logger.error(problem)
      end
    else
      IO.puts "OK"
    end
  end

  defp reset_topic(topic) do
    :os.cmd('kafka-topics --delete --zookeeper localhost:2181 --topic #{topic}')
    :os.cmd('kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic #{topic}')
  end

  defp send_messages(n_messages, hosts, topic) do
    client_id = :test_client
    :ok = :brod.start_client(hosts, client_id, _client_config=[])
    :ok = :brod.start_producer(client_id, topic, _producer_config = [])

    Enum.each(1..n_messages, fn i ->
      partition = rem(i, 3)
      :ok = :brod.produce_sync(client_id, topic, partition, _key="", "#{i}")
    end)
    :brod.stop_client(client_id)
  end

  defp start_broadway() do
    {:ok, messages_agent} = Agent.start_link(fn -> [] end)
    context = %{messages_agent: messages_agent, caller_pid: self()}
    {:ok, broadway_pid}  = MyBroadway.start_link(context)
    {broadway_pid, messages_agent}
  end

  defp stop_broadway(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end

Runner.run()
