defmodule BroadwayKafka.Producer do
  @moduledoc """
  A Kafka connector for Broadway.

  BroadwayKafka can subscribe as a consumer to one or more topics and process streams
  of records within the same consumer group. Communication is done through Kafka's
  [Consumer API](https://kafka.apache.org/documentation.html#consumerapi) using the
  [:brod](https://github.com/klarna/brod/) client.


  ## Options

  #{NimbleOptions.docs(BroadwayKafka.ProducerOptions.schema())}

  > #### `:brod` Option Support {: .info}
  > Currently, Broadway does not support all options provided by `:brod`. If you
  > have a scenario where you need any extra option that is not listed above, please open an
  > issue, so we can consider adding it.

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

  ## Telemetry

  This producer emits a few [Telemetry](https://github.com/beam-telemetry/telemetry)
  events which are listed below.

    * `[:broadway_kafka, :assignments_revoked, :start | :stop | :exception]` spans -
      these events are emitted in "span style" when receiving assignments revoked call from consumer group coordinator
      See `:telemetry.span/3`.

  ## Shared Client Performance

  Enabling shared client may drastically decrease performance. Since connection is handled by a single process,
  producers may block each other waiting for the client response.

  This is more likely to be an issue if the producers on your pipeline are fetching message from
  multiple topics and specially if there are very low traffic topics, which may block on batch wait times.

  To mitigate this, you can split your topics between multiple pipelines, but notice that this will
  increase the resource usage as well. By creating one new client/connection for each pipeline,
  you effectively diminishing the `shared_client` resource usage gains. So make sure to measure
  if you enable this option.
  """

  use GenStage

  require Logger
  import Record, only: [defrecordp: 2, extract: 2]

  alias Broadway.{Message, Acknowledger, Producer}
  alias BroadwayKafka.Allocator
  alias BroadwayKafka.Acknowledger

  @behaviour Producer
  @behaviour :brod_group_member

  defrecordp :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  defrecordp :brod_received_assignment,
             extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  # Protocol error codes that indicate a transient condition on the broker.
  # Retrying the fetch in place avoids crashing the producer, which would
  # force a rebalance of the whole consumer group.
  @retriable_fetch_errors [
    :kafka_storage_error,
    :leader_not_available,
    :not_leader_for_partition,
    :not_leader_or_follower,
    :request_timed_out,
    :network_exception,
    :replica_not_available
  ]

  @impl GenStage
  def init(opts) do
    Process.flag(:trap_exit, true)

    config = opts[:initialized_client_config]

    draining_after_revoke_flag =
      self()
      |> drain_after_revoke_table_name!()
      |> drain_after_revoke_table_init!()

    prefix = get_in(config, [:client_config, :client_id_prefix])

    {_, producer_name} = Process.info(self(), :registered_name)

    client_id =
      config[:shared_client_id] || :"#{prefix}#{Module.concat([producer_name, Client])}"

    max_demand =
      with [{_first, processor_opts}] <- opts[:broadway][:processors],
           max_demand when is_integer(max_demand) <- processor_opts[:max_demand] do
        max_demand
      else
        _ -> 10
      end

    state = %{
      client: opts[:client] || BroadwayKafka.BrodClient,
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
      max_demand: max_demand,
      shared_client: config.shared_client
    }

    {:producer, connect(state)}
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

  def handle_info(:maybe_schedule_poll, state) do
    maybe_schedule_poll(%{state | receive_timer: nil}, state.receive_interval)
  end

  def handle_info({:put_assignments, group_generation_id, assignments}, state) do
    list =
      Enum.map(assignments, fn assignment ->
        brod_received_assignment(
          topic: topic,
          partition: partition,
          begin_offset: assigned_begin_offset
        ) = assignment

        offset_reset_policy = state.config[:offset_reset_policy]

        begin_offset =
          case state.config[:begin_offset] do
            :assigned -> assigned_begin_offset
            :reset -> :undefined
          end

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

  def handle_info({:ack, key, offsets}, state) do
    %{group_coordinator: group_coordinator, client: client, acks: acks, config: config} = state
    {generation_id, topic, partition} = key

    {drained?, new_offset, updated_acks} = Acknowledger.update_current_offset(acks, key, offsets)

    if new_offset do
      try do
        client.ack(
          group_coordinator,
          generation_id,
          topic,
          partition,
          new_offset,
          disable_offset_commit_during_revoke_call(config, state)
        )
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
    if coord = state.group_coordinator do
      Process.exit(coord, :shutdown)
    end

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

  def handle_info(:reconnect, state) do
    if state.client.connected?(state.client_id) do
      {:noreply, [], connect(state)}
    else
      schedule_reconnect(state.reconnect_timeout)
      {:noreply, [], state}
    end
  end

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

    {producer_mod, producer_opts} = opts[:producer][:module]

    client = producer_opts[:client] || BroadwayKafka.BrodClient

    case client.init(Keyword.put(producer_opts, :broadway, opts)) do
      {:error, %NimbleOptions.ValidationError{} = error} ->
        raise error

      {:ok, extra_child_specs, config} ->
        new_producer_opts =
          Keyword.put(producer_opts, :initialized_client_config, config)

        updated_opts =
          put_in(updated_opts, [:producer, :module], {producer_mod, new_producer_opts})

        {allocators ++ extra_child_specs, updated_opts}
    end
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

    metadata = %{producer: maybe_process_name.(producer_pid)}

    :telemetry.span([:broadway_kafka, :assignments_revoked], metadata, fn ->
      GenStage.call(producer_pid, :drain_after_revoke, :infinity)
      {:ok, metadata}
    end)
  end

  @impl GenStage
  def terminate(_reason, state) do
    %{client: client, group_coordinator: group_coordinator, client_id: client_id} = state
    group_coordinator && Process.exit(group_coordinator, :shutdown)

    if state.shared_client == false do
      client.disconnect(client_id)
    end

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
    {generation_id, topic, partition} = key

    case fetch_with_retries(state, topic, partition, offset, 0) do
      {:ok, {_watermark_offset, kafka_messages}} ->
        Enum.map(kafka_messages, fn k_msg ->
          wrap_message(k_msg, topic, partition, generation_id)
        end)

      {:error, reason} ->
        raise "cannot fetch records from Kafka (topic=#{topic} partition=#{partition} " <>
                "offset=#{offset}). Reason: #{inspect(reason)}"
    end
  end

  defp fetch_with_retries(state, topic, partition, offset, attempt) do
    %{
      client: client,
      client_id: client_id,
      config: config
    } = state

    fetch_config = config[:fetch_config]
    max_retries = fetch_config[:max_fetch_retries]

    case client.fetch(client_id, topic, partition, offset, fetch_config, config) do
      {:error, reason} when reason in @retriable_fetch_errors and attempt < max_retries ->
        backoff_ms = fetch_config[:fetch_retry_backoff_ms] * Integer.pow(2, attempt)

        Logger.warning(
          "Retriable error while fetching records from Kafka (topic=#{topic} " <>
            "partition=#{partition} offset=#{offset}). Reason: #{inspect(reason)}. " <>
            "Retrying in #{backoff_ms}ms (attempt #{attempt + 1} of #{max_retries})"
        )

        Process.sleep(backoff_ms)
        fetch_with_retries(state, topic, partition, offset, attempt + 1)

      result ->
        result
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
      {:ok, coord_pid, _coord_ref} ->
        %{state | group_coordinator: coord_pid}

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
            {acks, demand, Enum.reverse(acc), :queue.in({key, rest}, queue)}

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

  defp disable_offset_commit_during_revoke_call(config, state) do
    offset_commit_on_ack =
      not is_draining_after_revoke?(state.draining_after_revoke_flag) and
        state.config.offset_commit_on_ack

    %{config | offset_commit_on_ack: offset_commit_on_ack}
  end
end
