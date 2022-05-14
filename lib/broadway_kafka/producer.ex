defmodule BroadwayKafka.Producer do
  @moduledoc """
  A Kafka connector for Broadway.

  BroadwayKafka can subscribe as a consumer to one or more topics and process streams
  of records within the same consumer group. Communication is done through Kafka's
  [Consumer API](https://kafka.apache.org/documentation.html#consumerapi) using the
  [:brod](https://github.com/klarna/brod/) client.

  ## Options

    * `:hosts` - Required. A list of host and port tuples or a single string of comma
      separated HOST:PORT pairs to use for establishing the initial connection to Kafka,
      e.g. [localhost: 9092]. Examples:

          # Keyword
          ["kafka-vm1": 9092, "kafka-vm2": 9092, "kafka-vm3": 9092]

          # List of tuples
          [{"kafka-vm1", 9092}, {"kafka-vm2", 9092}, {"kafka-vm3", 9092}]

          # String
          "kafka-vm1:9092,kafka-vm2:9092,kafka-vm3:9092"

    * `:group_id` - Required. A unique string that identifies the consumer group the producer
      will belong to.

    * `:topics` - Required. A list of topics that the producer will subscribe to.

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages. Default is 2000 (2 seconds).

    * `:offset_commit_on_ack` - Optional. Tells Broadway to send or not an offset commit
      request after each acknowledgement. Default is `true`. Setting this value to `false` can
      increase performance since commit requests will respect the `:offset_commit_interval_seconds`
      option. However, setting long commit intervals might lead to a large number of duplicated
      records to be processed after a server restart or connection loss. If that's the case, make
      sure your logic is idempotent when consuming records to avoid inconsistencies. Also, bear
      in mind the the negative performance impact might be insignificant if you're using batchers
      since only one commit request will be performed per batch.

    * `:offset_reset_policy` - Optional. Defines the offset to be used when there's no initial
      offset in Kafka or if the current offset has expired. Possible values are `:earliest` or
      `:latest`. Default is `:latest`.

    * `:group_config` - Optional. A list of options used to configure the group
      coordinator. See the ["Group config options"](#module-group-config-options) section below for a list of all available
      options.

    * `:fetch_config` - Optional. A list of options used when fetching messages. See the
      ["Fetch config options"](#module-fetch-config-options) section below for a list of all available options.

    * `:client_config` - Optional. A list of options used when creating the client. See the
      ["Client config options"](#module-client-config-options) section below for a list of all available options.

  ## Group config options

  The available options that will be passed to `:brod`'s group coordinator.

    * `:offset_commit_interval_seconds` - Optional. The time interval between two
       OffsetCommitRequest messages. Default is 5.

    * `:rejoin_delay_seconds` - Optional. Delay in seconds before rejoining the group. Default is 1.

    * `:session_timeout_seconds` - Optional. Time in seconds the group coordinator broker waits
      before considering a member 'down' if no heartbeat or any kind of request is received.
      A group member may also consider the coordinator broker 'down' if no heartbeat response
      is received in the past N seconds. Default is 30 seconds.

    * `:heartbeat_rate_seconds` - Optional. Time in seconds for member to 'ping' group coordinator.
      Heartbeats are used to ensure that the consumer's session stays active and
      to facilitate rebalancing when new consumers join or leave the group.
      The value must be set lower than `:session_timeout_seconds`, typically equal to or lower than 1/3 of that value.
      It can be adjusted even lower to control the expected time for normal rebalances. Default is 5 seconds.

    * `:rebalance_timeout_seconds` - Optional. Time in seconds for each worker to join the group once a rebalance has begun.
      If the timeout is exceeded, then the worker will be removed from the group, which will cause offset commit failures. Default is 30.

  ## Fetch config options

  The available options that will be internally passed to `:brod.fetch/5`.

    * `:min_bytes` - Optional. The minimum amount of data to be fetched from the server.
      If not enough data is available the request will wait for that much data to accumulate
      before answering. Default is 1 byte. Setting this value greater than 1 can improve
      server throughput a bit at the cost of additional latency.

    * `:max_bytes` - Optional. The maximum amount of data to be fetched at a time from a single
      partition. Default is 1048576 (1 MiB). Setting greater values can improve server
      throughput at the cost of more memory consumption.

    * `:max_wait_time` - Optional. Time in millisecond. Max number of milliseconds allowed for the broker to collect
    `min_bytes` of messages in fetch response. Default is 1000ms.

  ## Client config options

  The available options that will be internally passed to `:brod.start_client/3`.

    * `:client_id_prefix` - Optional. A string that will be used to build the client id passed to `:brod`. The example
    value `client_id_prefix: :"\#{Node.self()} -"` would generate the following connection log from our integration
    tests:

          20:41:37.717 [info]      :supervisor: {:local, :brod_sup}
          :started: [
            pid: #PID<0.286.0>,
            id: :"nonode@nohost - Elixir.BroadwayKafka.ConsumerTest.MyBroadway.Broadway.Producer_0.Client",
            mfargs: {:brod_client, :start_link,
             [
               [localhost: 9092],
               :"nonode@nohost - Elixir.BroadwayKafka.ConsumerTest.MyBroadway.Broadway.Producer_0.Client",
               [client_id_prefix: :"nonode@nohost - "]
             ]},
            restart_type: {:permanent, 10},
            shutdown: 5000,
            child_type: :worker
          ]

    * `:sasl` - Optional. A a tuple of mechanism which can be `:plain`, `:scram_sha_256` or `:scram_sha_512`, username and password. See the `:brod`'s
    [`Authentication Support`](https://github.com/klarna/brod#authentication-support) documentation
    for more information. Default is no sasl options.

    * `:ssl` - Optional. A boolean or a list of options to use when connecting via SSL/TLS. See the
    [`tls_client_option`](http://erlang.org/doc/man/ssl.html#type-tls_client_option) documentation
    for more information. Default is no ssl options.

    * `:connect_timeout` - Optional. Time in milliseconds to be used as a timeout for `:brod`'s communication with Kafka.
    Default is to use `:brod`'s default timeout which is currently 5 seconds.

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
          concurrency: 1
        ],
        processors: [
          default: [
            concurrency: 10
          ]
        ]
      )

  ## Concurrency and partitioning

  The concurrency model provided by Kafka is based on partitioning, i.e., the more partitions
  you have, the more concurrency you get. However, in order to take advantage of this model
  you need to set up the `:concurrency` options for your processors and batchers accordingly. Having
  less concurrency than topic/partitions assigned will result in individual processors handling more
  than one partition, decreasing the overall level of concurrency. Therefore, if you want to
  always be able to process messages at maximum concurrency (assuming you have enough resources
  to do it), you should increase the concurrency up front to make sure you have enough
  processors to handle the extra messages received from new partitions assigned.

  > **Note**: Even if you don't plan to add more partitions to a Kafka topic, your pipeline can still
  receive more assignments than planned. For instance, if another consumer crashes, the server
  will reassign all its topic/partition to other available consumers, including any Broadway
  producer subscribed to the same topic.

  ## Handling failed messages

  `BroadwayKafka` never stops the flow of the stream, i.e. it will **always ack** the messages
  even when they fail. Unlike queue-based connectors, where you can mark a single message as failed.
  In Kafka that's not possible due to its single offset per topic/partition ack strategy. If you
  want to reprocess failed messages, you need to roll your own strategy. A possible way to do that
  is to implement `c:Broadway.handle_failed/2` and send failed messages to a separated stream or queue for
  later processing.

  ## Message metadata

  When producing messages, the following information will be passed to
  [`Broadway.Message`](`t:Broadway.Message.t/0`)'s metadata.

    * `topic` - The topic the message was published.

    * `partition` - The topic partition.

    * `offset` - The offset assigned to the message inside the partition.

    * `key` - The partition key.

    * `ts` - A timestamp associated with the message.

    * `headers` - The headers of the message.
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
    Process.flag(:trap_exit, true)

    client = opts[:client] || BroadwayKafka.BrodClient

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, config} ->
        {_, producer_name} = Process.info(self(), :registered_name)

        draining_after_revoke_flag =
          self()
          |> drain_after_revoke_table_name!()
          |> drain_after_revoke_table_init!()

        prefix = get_in(config, [:client_config, :client_id_prefix])
        client_id = :"#{prefix}#{Module.concat([producer_name, Client])}"

        max_demand =
          with [{_first, processor_opts}] <- opts[:broadway][:processors],
               max_demand when is_integer(max_demand) <- processor_opts[:max_demand] do
            max_demand
          else
            _ -> 10
          end

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
          draining_after_revoke_flag: draining_after_revoke_flag,
          demand: 0,
          shutting_down?: false,
          buffer: :queue.new(),
          max_demand: max_demand
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
  def handle_call(:drain_after_revoke, _from, %{group_coordinator: nil} = state) do
    set_draining_after_revoke!(state.draining_after_revoke_flag, false)
    {:reply, :ok, [], state}
  end

  @impl GenStage
  def handle_call(:drain_after_revoke, from, %{revoke_caller: nil} = state) do
    state = reset_buffer(state)

    if Acknowledger.all_drained?(state.acks) do
      set_draining_after_revoke!(state.draining_after_revoke_flag, false)
      {:reply, :ok, [], %{state | acks: Acknowledger.new()}}
    else
      {:noreply, [], %{state | revoke_caller: from}}
    end
  end

  @impl GenStage
  def handle_cast({:update_topics, topics}, state) do
    state.client.update_topics(state.group_coordinator, topics)

    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({:poll, key}, %{acks: acks, demand: demand, max_demand: max_demand} = state) do
    # We only poll if:
    #
    #   1. We are not shutting down
    #   2. We are not waiting for draining after receivd revoke assignment
    #   3. We know the key being acked
    #
    # Note the key may be out of date when polling has been scheduled and
    # assignments were revoked afterwards, which is why check 3 is necessary.
    offset = Acknowledger.last_offset(acks, key)

    if not state.shutting_down? and
         not is_draining_after_revoke?(state.draining_after_revoke_flag) and
         offset != nil do
      messages = fetch_messages_from_kafka(state, key, offset)
      to_send = min(demand, max_demand)
      {new_acks, not_sent, messages, pending} = split_demand(messages, acks, key, to_send)
      new_buffer = enqueue_many(state.buffer, key, pending)

      new_demand = demand - to_send + not_sent
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

        offset_reset_policy = state.config[:offset_reset_policy]

        offset =
          state.client.resolve_offset(
            topic,
            partition,
            begin_offset,
            offset_reset_policy,
            state.config
          )

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
          Logger.error(Exception.format(kind, reason, __STACKTRACE__))
      end
    end

    new_state =
      if drained? && state.revoke_caller && Acknowledger.all_drained?(updated_acks) do
        set_draining_after_revoke!(state.draining_after_revoke_flag, false)
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

  def handle_info({:DOWN, _ref, _, coord, _reason}, %{group_coordinator: coord} = state) do
    state = reset_buffer(state)
    schedule_reconnect(state.reconnect_timeout)

    {:noreply, [], %{state | group_coordinator: nil}}
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, [], state}
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

    producers_concurrency = opts[:producer][:concurrency]
    [first_processor_entry | other_processors_entries] = opts[:processors]

    {allocator, updated_processor_entry} =
      build_allocator_spec_and_consumer_entry(
        broadway_name,
        :processors,
        "processor",
        producers_concurrency,
        first_processor_entry
      )

    {allocators, updated_batchers_entries} =
      Enum.reduce(opts[:batchers], {[allocator], []}, fn entry, {allocators, entries} ->
        {allocator, updated_entry} =
          build_allocator_spec_and_consumer_entry(
            broadway_name,
            :batchers,
            "batcher_consumer",
            producers_concurrency,
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
    raise "not implemented"
  end

  @impl :brod_group_member
  def assignments_received(pid, _group_member_id, group_generation_id, received_assignments) do
    send(pid, {:put_assignments, group_generation_id, received_assignments})
    :ok
  end

  @impl :brod_group_member
  def assignments_revoked(producer_pid) do
    maybe_process_name = fn
      pid when is_pid(pid) -> pid
      name when is_atom(name) -> Process.whereis(name)
    end

    producer_pid
    |> maybe_process_name.()
    |> drain_after_revoke_table_name!()
    |> set_draining_after_revoke!(true)

    GenStage.call(producer_pid, :drain_after_revoke, :infinity)
    :ok
  end

  @impl GenStage
  def terminate(_reason, state) do
    %{client: client, group_coordinator: group_coordinator, client_id: client_id} = state
    client.stop_group_coordinator(group_coordinator)
    client.disconnect(client_id)
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
        raise "cannot fetch records from Kafka (topic=#{topic} partition=#{partition} " <>
                "offset=#{offset}). Reason: #{inspect(reason)}"
    end
  end

  defp wrap_message(kafka_msg, topic, partition, generation_id) do
    kafka_message(value: data, offset: offset, key: key, ts: ts, headers: headers) = kafka_msg

    ack_data = %{offset: offset}
    ack_ref = {self(), {generation_id, topic, partition}}

    message = %Message{
      data: data,
      metadata: %{
        topic: topic,
        partition: partition,
        offset: offset,
        key: key,
        ts: ts,
        headers: headers
      },
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
         producers_concurrency,
         consumer_entry
       ) do
    {consumer_name, consumer_config} = consumer_entry
    validate_partition_by(group, consumer_name, consumer_config)

    consumer_concurrency = consumer_config[:concurrency]
    allocator_name = Module.concat([broadway_name, "Allocator_#{prefix}_#{consumer_name}"])
    partition_by = &Allocator.fetch!(allocator_name, {&1.metadata.topic, &1.metadata.partition})
    new_config = Keyword.put(consumer_config, :partition_by, partition_by)

    allocator =
      {BroadwayKafka.Allocator, {allocator_name, producers_concurrency, consumer_concurrency}}

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
    {rest, demand, reversed, acc} = reverse_split_demand(list, demand, [], [])
    acks = update_last_offset(acks, key, reversed)
    {acks, demand, Enum.reverse(acc), rest}
  end

  defp reverse_split_demand(rest, 0, reversed, acc) do
    {rest, 0, reversed, acc}
  end

  defp reverse_split_demand([], demand, reversed, acc) do
    {[], demand, reversed, acc}
  end

  defp reverse_split_demand([head | tail], demand, reversed, acc) do
    reverse_split_demand(tail, demand - 1, [head | reversed], [head | acc])
  end

  defp enqueue_many(queue, _key, []), do: queue
  defp enqueue_many(queue, key, list), do: :queue.in({key, list}, queue)

  defp dequeue_many(queue, acks, demand, acc) when demand > 0 do
    case :queue.out(queue) do
      {{:value, {key, list}}, queue} ->
        {rest, demand, reversed, acc} = reverse_split_demand(list, demand, [], acc)
        acks = update_last_offset(acks, key, reversed)

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

  defp update_last_offset(acks, key, [message | _] = reversed) do
    last = message.metadata.offset + 1
    offsets = Enum.reduce(reversed, [], &[&1.metadata.offset | &2])
    Acknowledger.update_last_offset(acks, key, last, offsets)
  end

  defp update_last_offset(acks, _key, []) do
    acks
  end

  defp reset_buffer(state) do
    put_in(state.buffer, :queue.new())
  end

  defp schedule_reconnect(timeout) do
    Process.send_after(self(), :reconnect, timeout)
  end

  defp drain_after_revoke_table_name!(pid) do
    {_, producer_name} = Process.info(pid, :registered_name)

    Module.concat([producer_name, DrainingAfterRevoke])
  end

  defp drain_after_revoke_table_init!(table_name) do
    table_name = :ets.new(table_name, [:named_table, :public, :set])

    set_draining_after_revoke!(table_name, false)

    table_name
  end

  defp set_draining_after_revoke!(table_name, value) do
    :ets.insert(table_name, {:draining, value})
  end

  defp is_draining_after_revoke?(table_name) do
    :ets.lookup_element(table_name, :draining, 2)
  end
end
