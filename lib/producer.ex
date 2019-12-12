defmodule BroadwayKafka.Producer do
  @moduledoc """
  A Kafka connector for Broadway.

  BroadwayKafka can subscribe as a consumer to one or more topics and process streams
  of records within the same consumer group. Communication is done through Kafka's
  [Consumer API](https://kafka.apache.org/documentation.html#consumerapi) using the
  [:brod](https://github.com/klarna/brod/) client.

  ## Options

    * `:hosts` - Required. A keyword list of host/port pairs to use for establishing the
       initial connection to Kafka, e.g. [localhost: 9092].

    * `:group_id` - Required. A unique string that identifies the consumer group the
       producer will belong to.

    * `:topics` - Required. A list of topics or a keyword list of topic/partition that
      the producer will subscribe to.

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages. Default is 2000 (2 seconds).

    * `:offset_commit_on_ack` - Optional. Tells Broadway to send or not an offset commit
      request after each acknowledgemnt. Default is `true`. Setting this value to `false` can
      increase performance since commit requests will respect the `:offset_commit_interval_seconds`
      option. However, setting long commit intervals might lead to a large number of duplicated
      records to be processed after a server restart or connection loss. If that's the case, make
      sure your logic is idempotent when consuming records to avoid inconsistencies. Also, bear
      in mind the the negative performance impact might be insignificant if you're using batchers
      since only one commit request will be performed per batch.

    * `:group_config` - Optional. A list of options used to configure the group
      coordinator. See the "Group config options" section below for a list of all available
      options.

    * `:fetch_config` - Optional. A list of options used when fetching messages. See the
      "Fetch config options" section below for a list of all available options.

  ## Group config options

  The available options that will be passed to `:brod`'s group coordinator.

    * `:offset_commit_interval_seconds` - Optional. The time interval between two
       OffsetCommitRequest messages. Default is 5.

    * `:rejoin_delay_seconds` - Optional. Delay in seconds before rejoining the group. Default is 1.

    * `:session_timeout_seconds` - Optional. Time in seconds the group coordinator broker waits
      before considering a member 'down' if no heartbeat or any kind of request is received.
      A group member may also consider the coordinator broker 'down' if no heartbeat response
      is received in the past N seconds. Default is 10 seconds.

  ## Fetch config options

  The available options that will be internally passed to `:brod.fetch/5`.

    * `:min_bytes` - Optional. The minimum amount of data to be fetched from the server.
      If not enough data is available the request will wait for that much data to accumulate
      before answering. Default is 1 byte. Setting this value greater than 1 can improve
      server throughput a bit at the cost of additional latency.

    * `:max_bytes` - Optional. The maximum amount of data to be fetched at a time from a single
      partition. Default is 1048576 (1 MiB). Setting greater values can improve server
      throughput at the cost of more memory consumption.

  > **Note**: Currently, Broadway does not support all options provided by `:brod`. If you
  have a scenario where you need any extra option that is not listed above, please open an
  issue, so we can consider adding it.

  ## Example

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producer: [
          module: {BroadwayKafka.Producer, [
            hosts: [localhost: 9092],
            group_id: "group_1",
            topics: ["test"],
          ]},
          stages: 10
        ],
        processors: [
          default: [
            stages: 10
          ]
        ]
      )

  ## Concurrency and partitioning

  The concurrency model provided by Kafka is based on partitioning, i.e., the more partitions
  you have, the more concurrency you get. However, in order to take advantage of this model
  you need to set up the `:stages` options for your processors and batchers accordingly. Having
  less stages than topic/partitions assigned will result in individual processors handling more
  than one partition, decreasing the overall level of concurrency. Therefore, if you want to
  always be able to process messages at maximum concurrency (assuming you have enough resources
  to do it), you should increase the numbers of stages up front to make sure you have enough
  processors to handle the extra messages received from new partitions assigned.

  > **Note**: Even if you don't plan to add more partitions to a Kafka topic, your pipeline can still
  receive more assignments than planned. For instance, if another consumer crashes, the server
  will reassign all its topic/partition to other available consumers, including any Broadway
  producer subscribed to the same topic.
  """

  use GenStage

  require Logger
  import Record, only: [defrecord: 2, extract: 2]

  alias Broadway.{Message, Acknowledger, Producer}
  alias BroadwayKafka.Allocator
  alias BroadwayKafka.Acknowledger

  @behaviour Producer
  @behaviour :brod_group_member

  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  defrecord :brod_received_assignment,
            extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  @impl GenStage
  def init(opts) do
    client = opts[:client] || BroadwayKafka.BrodClient

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, config} ->
        {_, producer_name} = Process.info(self(), :registered_name)
        client_id = Module.concat([producer_name, Client])

        state = %{
          client: client,
          client_id: client_id,
          group_coordinator: nil,
          receive_timer: nil,
          receive_interval: config.receive_interval,
          reconnect_timeout: config.reconnect_timeout,
          acks: Acknowledger.new(),
          config: config,
          allocator_names: allocator_names(opts[:broadway]),
          revoke_caller: nil,
          demand: 0,
          shutting_down?: false,
          buffer: :queue.new()
        }

        {:producer, connect(state)}
    end
  end

  defp allocator_names(broadway_config) do
    broadway_name = broadway_config[:name]
    broadway_index = broadway_config[:index]

    processors_allocators =
      for {name, _} <- broadway_config[:processors] do
        Module.concat([broadway_name, "Allocator_processor_#{name}"])
      end

    batchers_allocators =
      for {name, _} <- broadway_config[:batchers] do
        Module.concat([broadway_name, "Allocator_batcher_consumer_#{name}"])
      end

    {broadway_index, processors_allocators, batchers_allocators}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    maybe_schedule_poll(%{state | demand: demand + incoming_demand}, 0)
  end

  @impl GenStage
  def handle_call(:drain_after_revoke, from, %{revoke_caller: nil} = state) do
    state = reset_buffer(state)

    if Acknowledger.all_drained?(state.acks) do
      {:reply, :ok, [], %{state | acks: Acknowledger.new()}}
    else
      {:noreply, [], %{state | revoke_caller: from}}
    end
  end

  @impl GenStage
  def handle_info({:poll, key}, %{acks: acks, demand: demand} = state) do
    # We only poll if:
    #
    #   1. We are not shutting down
    #   2. Our assignments have not been revoked
    #   3. We know the key being acked
    #
    # Note the key may be out of date when polling has been scheduled and
    # assignments were revoked afterwards, which is why check 3 is necessary.
    offset = Acknowledger.last_offset(acks, key)

    if not state.shutting_down? and state.revoke_caller == nil and offset != nil do
      messages = fetch_messages_from_kafka(state, key, offset)
      {new_acks, new_demand, messages, pending} = split_demand(messages, acks, key, demand)
      new_buffer = enqueue_many(state.buffer, key, pending)

      new_state = %{state | acks: new_acks, demand: new_demand, buffer: new_buffer}
      {:noreply, messages, new_state}
    else
      {:noreply, [], state}
    end
  end

  @impl GenStage
  def handle_info(:maybe_schedule_poll, state) do
    maybe_schedule_poll(%{state | receive_timer: nil}, state.receive_interval)
  end

  @impl GenStage
  def handle_info({:put_assignments, group_generation_id, assignments}, state) do
    list =
      Enum.map(assignments, fn assignment ->
        brod_received_assignment(
          topic: topic,
          partition: partition,
          begin_offset: begin_offset
        ) = assignment

        offset = if begin_offset == :undefined, do: 0, else: begin_offset
        {group_generation_id, topic, partition, offset}
      end)

    topics_partitions = Enum.map(list, fn {_, topic, partition, _} -> {topic, partition} end)
    {broadway_index, processors_allocators, batchers_allocators} = state.allocator_names

    for allocator_name <- processors_allocators do
      Allocator.allocate(allocator_name, broadway_index, topics_partitions)
    end

    for allocator_name <- batchers_allocators do
      Allocator.allocate(allocator_name, broadway_index, topics_partitions)
    end

    {:noreply, [], %{state | acks: Acknowledger.add(state.acks, list)}}
  end

  @impl GenStage
  def handle_info({:ack, key, offsets}, state) do
    %{group_coordinator: group_coordinator, client: client, acks: acks, config: config} = state
    {generation_id, topic, partition} = key

    {drained?, new_offset, updated_acks} = Acknowledger.update_current_offset(acks, key, offsets)

    if new_offset do
      try do
        client.ack(group_coordinator, generation_id, topic, partition, new_offset, config)
      catch
        kind, reason ->
          Logger.error(Exception.format(kind, reason, System.stacktrace()))
      end
    end

    new_state =
      if drained? && state.revoke_caller && Acknowledger.all_drained?(updated_acks) do
        GenStage.reply(state.revoke_caller, :ok)
        %{state | revoke_caller: nil, acks: Acknowledger.new()}
      else
        %{state | acks: updated_acks}
      end

    {:noreply, [], new_state}
  end

  def handle_info({:DOWN, _ref, _, {client_id, _}, _reason}, %{client_id: client_id} = state) do
    state.client.stop_group_coordinator(state.group_coordinator)
    state = reset_buffer(state)
    schedule_reconnect(state.reconnect_timeout)

    {:noreply, [], %{state | group_coordinator: nil}}
  end

  @impl GenStage
  def handle_info(:reconnect, state) do
    if state.client.connected?(state.client_id) do
      {:noreply, [], connect(state)}
    else
      schedule_reconnect(state.reconnect_timeout)
      {:noreply, [], state}
    end
  end

  @impl GenStage
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl Producer
  def prepare_for_draining(state) do
    # On draining, we will continue scheduling the polls, but they will be a no-op.
    {:noreply, [], %{state | shutting_down?: true}}
  end

  @impl Producer
  def prepare_for_start(_module, opts) do
    broadway_name = opts[:name]

    producers_stages = opts[:producer][:stages]
    [first_processor_entry | other_processors_entries] = opts[:processors]

    {allocator, updated_processor_entry} =
      build_allocator_spec_and_consumer_entry(
        broadway_name,
        :processors,
        "processor",
        producers_stages,
        first_processor_entry
      )

    {allocators, updated_batchers_entries} =
      Enum.reduce(opts[:batchers], {[allocator], []}, fn entry, {allocators, entries} ->
        {allocator, updated_entry} =
          build_allocator_spec_and_consumer_entry(
            broadway_name,
            :batchers,
            "batcher_consumer",
            producers_stages,
            entry
          )

        {[allocator | allocators], [updated_entry | entries]}
      end)

    updated_opts =
      opts
      |> Keyword.put(:processors, [updated_processor_entry | other_processors_entries])
      |> Keyword.put(:batchers, updated_batchers_entries)

    {allocators, updated_opts}
  end

  @impl :brod_group_member
  def get_committed_offsets(_pid, _topics_partitions) do
    {:ok, [], -1}
  end

  @impl :brod_group_member
  def assignments_received(pid, _group_member_id, group_generation_id, received_assignments) do
    send(pid, {:put_assignments, group_generation_id, received_assignments})
    :ok
  end

  @impl :brod_group_member
  def assignments_revoked(producer_pid) do
    GenStage.call(producer_pid, :drain_after_revoke, :infinity)
    :ok
  end

  defp maybe_schedule_poll(%{demand: 0} = state, _interval) do
    {:noreply, [], state}
  end

  defp maybe_schedule_poll(state, interval) do
    %{buffer: buffer, demand: demand, acks: acks, receive_timer: receive_timer} = state

    case dequeue_many(buffer, acks, demand, []) do
      {acks, 0, events, buffer} ->
        {:noreply, events, %{state | demand: 0, buffer: buffer, acks: acks}}

      {acks, demand, events, buffer} ->
        receive_timer = receive_timer || schedule_poll(state, interval)

        state = %{
          state
          | demand: demand,
            buffer: buffer,
            receive_timer: receive_timer,
            acks: acks
        }

        {:noreply, events, state}
    end
  end

  defp schedule_poll(state, interval) do
    for key <- Acknowledger.keys(state.acks) do
      Process.send_after(self(), {:poll, key}, interval)
    end

    Process.send_after(self(), :maybe_schedule_poll, interval)
  end

  defp fetch_messages_from_kafka(state, key, offset) do
    %{
      client: client,
      client_id: client_id,
      config: config
    } = state

    {generation_id, topic, partition} = key

    case client.fetch(client_id, topic, partition, offset, config[:fetch_config], config) do
      {:ok, {_watermark_offset, kafka_messages}} ->
        Enum.map(kafka_messages, fn k_msg ->
          wrap_message(k_msg, topic, partition, generation_id)
        end)

      {:error, reason} ->
        Logger.error("Cannot fetch records from Kafka. Reason: #{inspect(reason)}")
        {[], 0, offset}
    end
  end

  defp wrap_message(kafka_msg, topic, partition, generation_id) do
    kafka_message(value: data, offset: offset, key: key, ts: ts) = kafka_msg

    ack_data = %{offset: offset}
    ack_ref = {self(), {generation_id, topic, partition}}

    message = %Message{
      data: data,
      metadata: %{topic: topic, partition: partition, offset: offset, key: key, ts: ts},
      acknowledger: {Acknowledger, ack_ref, ack_data}
    }

    Message.put_batch_key(message, {topic, partition})
  end

  defp connect(state) do
    %{client: client, client_id: client_id, config: config} = state

    case client.setup(self(), client_id, __MODULE__, config) do
      {:ok, group_coordinator} ->
        %{state | group_coordinator: group_coordinator}

      error ->
        raise "Cannot connect to Kafka. Reason #{inspect(error)}"
    end
  end

  defp build_allocator_spec_and_consumer_entry(
         broadway_name,
         group,
         prefix,
         producers_stages,
         consumer_entry
       ) do
    {consumer_name, consumer_config} = consumer_entry
    validate_partition_by(group, consumer_name, consumer_config)

    consumer_stages = consumer_config[:stages]
    allocator_name = Module.concat([broadway_name, "Allocator_#{prefix}_#{consumer_name}"])
    partition_by = &Allocator.fetch!(allocator_name, {&1.metadata.topic, &1.metadata.partition})
    new_config = Keyword.put(consumer_config, :partition_by, partition_by)
    allocator = {BroadwayKafka.Allocator, {allocator_name, producers_stages, consumer_stages}}
    allocator_spec = Supervisor.child_spec(allocator, id: allocator_name)

    {allocator_spec, {consumer_name, new_config}}
  end

  defp validate_partition_by(group, consumer_name, consumer_config) do
    if Keyword.has_key?(consumer_config, :partition_by) do
      raise ArgumentError,
            "cannot set option :partition_by for #{group} #{inspect(consumer_name)}. " <>
              "The option will be set automatically by BroadwayKafka.Producer"
    end
  end

  ## Buffer handling

  defp split_demand(list, acks, key, demand) do
    case reverse_split_demand(list, demand, []) do
      {demand, [message | _] = front, back} ->
        acks = Acknowledger.update_last_offset(acks, key, message.metadata.offset + 1)
        {acks, demand, Enum.reverse(front), back}

      {demand, [], back} ->
        {acks, demand, [], back}
    end
  end

  defp reverse_split_demand(rest, 0, acc) do
    {0, acc, rest}
  end

  defp reverse_split_demand([], demand, acc) do
    {demand, acc, []}
  end

  defp reverse_split_demand([head | tail], demand, acc) do
    reverse_split_demand(tail, demand - 1, [head | acc])
  end

  defp enqueue_many(queue, _key, []), do: queue
  defp enqueue_many(queue, key, list), do: :queue.in({key, list}, queue)

  defp dequeue_many(queue, acks, demand, acc) when demand > 0 do
    case :queue.out(queue) do
      {{:value, {key, list}}, queue} ->
        {demand, [message | _] = acc, rest} = reverse_split_demand(list, demand, acc)
        acks = Acknowledger.update_last_offset(acks, key, message.metadata.offset + 1)

        case {demand, rest} do
          {0, []} ->
            {acks, demand, Enum.reverse(acc), queue}

          {0, _} ->
            {acks, demand, Enum.reverse(acc), :queue.in_r({key, rest}, queue)}

          {_, []} ->
            dequeue_many(queue, acks, demand, acc)
        end

      {:empty, queue} ->
        {acks, demand, Enum.reverse(acc), queue}
    end
  end

  defp reset_buffer(state) do
    put_in(state.buffer, :queue.new())
  end

  defp schedule_reconnect(timeout) do
    Process.send_after(self(), :reconnect, timeout)
  end
end
