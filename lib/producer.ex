defmodule BroadwayKafka.Producer do
  use GenStage

  require Logger
  import Record, only: [defrecord: 2, extract: 2]

  alias Broadway.{Message, Acknowledger, Producer}
  alias BroadwayKafka.Allocator
  alias BroadwayKafka.Assignments

  @behaviour Acknowledger
  @behaviour Producer
  @behaviour :brod_group_member

  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")
  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")

  @default_receive_interval 2000

  @impl GenStage
  def init(opts) do
    client = opts[:client] || BroadwayKafka.BrodClient
    receive_interval = opts[:receive_interval] || @default_receive_interval

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
          receive_interval: receive_interval,
          assignments: Assignments.new,
          config: config,
          broadway_index: opts[:broadway][:index],
          broadway_name: opts[:broadway][:name],
          processors_names: Keyword.keys(opts[:broadway][:processors]),
          batchers_names: Keyword.keys(opts[:broadway][:batchers]),
          draining: false,
          drain_caller: nil,
          demand: 0
        }
        {:producer, connect(state)}
    end
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_call({:offset_acked, offset, key}, _from, state) do
    assignments = Assignments.update_last_acked_offset(state.assignments, key, offset)

    new_state =
      if state.draining && Assignments.drained?(assignments) do
        IO.puts("END DRAINING #{inspect(key)}. Offset: #{offset}")
        GenStage.reply(state.drain_caller, :ok)
        %{state | draining: false, drain_caller: nil, assignments: Assignments.new}
      else
        %{state | assignments: assignments}
      end

    {:reply, :ok, [], new_state}
  end

  @impl GenStage
  def handle_call(:drain_messages, _from, %{draining: true} = state) do
    {:reply, :ok, [], state}
  end

  @impl GenStage
  def handle_call(:drain_messages, from, state) do
    {_, producer_name} = Process.info(self(), :registered_name)
    IO.puts "DRAIN MESSAGES on #{producer_name}"
    if Assignments.drained?(state.assignments) do
      IO.inspect(state.assignments, label: "Current assignments")
      {:reply, :ok, [], %{state | assignments: Assignments.new}}
    else
      {:noreply, [], %{state | draining: true, drain_caller: from}}
    end
  end

  @impl GenStage
  def handle_info(:receive_messages, %{assignments: %{map: map}} = state) when map == %{} do
    {:noreply, [], %{state | receive_timer: nil}}
  end

  @impl GenStage
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  @impl GenStage
  def handle_info({:put_assignments, group_generation_id, assignments}, state) do
    assignments =
      Enum.map(assignments, fn assignment ->
        brod_received_assignment(
          topic: topic,
          partition: partition,
          begin_offset: begin_offset
        ) = assignment

        %{
          generation_id: group_generation_id,
          topic: topic,
          partition: partition,
          # TODO: Is it guaranteed that we can always send `0` for new topics?
          offset: (if begin_offset == :undefined, do: 0, else: begin_offset),
          last_acked_offset: nil,
          key: {group_generation_id, topic, partition}
        }
      end)

    # TODO: Update this to broadway master. Note on master
    # it will receive the same options as prepare_for_start
    # but under the broadway key, so it may be possible that
    # you can share some of this functionality (such as naming)
    # with prepare_for_start.
    broadway_name = state.broadway_name

    [processor_name | _] = state.processors_names
    broadway_index = state.broadway_index
    allocator_name = Module.concat([broadway_name, "Allocator_processor_#{processor_name}"])
    Allocator.allocate(allocator_name, broadway_index, Enum.map(assignments, &{&1.topic, &1.partition}))

    for name <- state.batchers_names do
      allocator_name = Module.concat([broadway_name, "Allocator_batcher_consumer_#{name}"])
      Allocator.allocate(allocator_name, broadway_index, Enum.map(assignments, &{&1.topic, &1.partition}))
    end

    schedule_receive_messages(0)

    {:noreply, [], %{state | assignments: Assignments.add(state.assignments, assignments)}}
  end

  @impl GenStage
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  @impl Acknowledger
  def ack(key, successful, _failed) do
    # TODO: Shouldn't we also ack failed messages? Otherwise if all messages in
    # a partition fail... we will replay them. But if only some fail, we don't
    # replay. My suggestion is to add something `on_failure: {m, f, args}` so
    # people can move it elsewhere in case of failures, then we ack after.
    ack_messages(successful, key)
  end

  @impl Producer
  def prepare_for_draining(_) do
    # TODO: Implement draining or document why one is not necessary.
    :ok
  end

  @impl Producer
  def prepare_for_start(_module, opts) do
    broadway_name = opts[:name]

    producers_stages = opts[:producer][:stages]
    [first_processor_entry | other_processors_entries] = opts[:processors]

    {allocator, updated_processor_entry} =
      build_allocator_spec_and_consumer_entry(broadway_name, "processor", producers_stages, first_processor_entry)

    {allocators, updated_batchers_entries} =
      Enum.reduce(opts[:batchers], {[allocator], []}, fn entry, {allocators, entries} ->
        {allocator, updated_entry} =
          build_allocator_spec_and_consumer_entry(broadway_name, "batcher_consumer", producers_stages, entry)
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
    IO.puts "ASSIGNMETNS RECEIVED"
    send(pid, {:put_assignments, group_generation_id, received_assignments})
    :ok
  end

  @impl :brod_group_member
  def assignments_revoked(producer_pid) do
    IO.puts "ASSIGNMETNS REVOKED"
    GenStage.call(producer_pid, :drain_messages, :infinity)
    :ok
  end

  defp handle_receive_messages(%{assignments: %{map: map}} = state) when map == %{} do
    IO.puts "handle_receive_messages with no assigns"
    {:noreply, [], state}
  end

  defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    {assignment, assignments} = Assignments.next(state.assignments)
    {messages, n_messages, new_offset} = fetch_messages_from_kafka(state, assignment)

    new_demand = max(0, demand - n_messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} ->
          IO.puts("scheduling for #{state.receive_interval}ms since no messages fetched")
          schedule_receive_messages(state.receive_interval)
        {_, 0} ->
          nil
        _ ->
          schedule_receive_messages(0)
      end

    new_state =
      %{state |
        receive_timer: receive_timer,
        assignments: Assignments.update_offset(assignments, assignment.key, new_offset),
        demand: new_demand
      }

    {:noreply, messages, new_state}
  end

  defp handle_receive_messages(state) do
    IO.puts "handle_receive_messages with receive_timer != nil"
    {:noreply, [], state}
  end

  defp fetch_messages_from_kafka(%{draining: true}, assignment) do
    IO.puts "fetch_messages_from_kafka with draining=true"
    {[], 0, assignment.offset}
  end

  defp fetch_messages_from_kafka(state, assignment) do
    %{
      client: client,
      client_id: client_id,
      group_coordinator: group_coordinator,
      config: config
    } = state

    %{
      generation_id: generation_id,
      topic: topic,
      partition: partition,
      offset: offset
    } = assignment

    # TODO: convert to map on `init/1`
    fetch_config_map = Map.new(config[:fetch_config] || [])

    IO.puts "Fetching messages for {#{topic}, #{partition}} from offset #{offset}"
    case client.fetch(client_id, topic, partition, offset, fetch_config_map, config) do
      {:ok, {_watermark_offset, kafka_messages}} ->
        {messages, n_messages, new_offset} =
          Enum.reduce(kafka_messages, {[], 0, offset}, fn k_msg, {messages, count, _new_offset} ->
            msg = wrap_message(k_msg, topic, partition, generation_id, group_coordinator, client)
            {[msg | messages], count + 1, msg.metadata.offset + 1}
          end)

        IO.inspect(%{key: {topic, partition}, offset: offset, new_offset: new_offset, length: n_messages}, label: "FETCHED")
        {Enum.reverse(messages), n_messages, new_offset}

      {:error, reason} ->
        # TODO: Treat the error properly
        IO.inspect(reason, label: "ERROR")
        {[], 0, offset}
    end
  end

  defp wrap_message(kafka_msg, topic, partition, generation_id, group_coordinator, client) do
    kafka_message(value: data, offset: offset, key: key, ts: ts) = kafka_msg

    ack_data = %{
      generation_id: generation_id,
      group_coordinator: group_coordinator,
      offset: offset,
      client: client,
      producer_pid: self()
    }

    message =
      %Message{
        data: data,
        metadata: %{topic: topic, partition: partition, offset: offset, key: key, ts: ts},
        acknowledger: {__MODULE__, {generation_id, topic, partition}, ack_data}
      }

    Message.put_batch_key(message, {topic, partition})
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end

  defp connect(state) do
    %{client: client, client_id: client_id, config: config} = state

    case client.setup(self(), client_id, config) do
      {:ok, group_coordinator} ->
        %{state | group_coordinator: group_coordinator}

      error ->
        #TODO
        raise "Setup failed: #{inspect(error)}"
    end
  end

  defp build_allocator_spec_and_consumer_entry(broadway_name, prefix, producers_stages, consumer_entry) do
    {consumer_name, consumer_config} = consumer_entry
    consumer_stages = consumer_config[:stages]
    allocator_name = Module.concat([broadway_name, "Allocator_#{prefix}_#{consumer_name}"])
    partition_by = &Allocator.fetch!(allocator_name, {&1.metadata.topic, &1.metadata.partition})
    # TODO: we should raise if the user set the partition_by themselves
    new_config = Keyword.put(consumer_config, :partition_by, partition_by)
    allocator = {BroadwayKafka.Allocator, {allocator_name, producers_stages, consumer_stages}}
    allocator_spec = Supervisor.child_spec(allocator, id: allocator_name)

    {allocator_spec, {consumer_name, new_config}}
  end

  defp ack_messages(messages, {generation_id, topic, partition}) do
    Enum.each(messages, fn msg ->
      {_, _, ack_data} = msg.acknowledger

      # TODO: Can some of those terms be moved to the TermStorage?
      try do
        %{
          group_coordinator: group_coordinator,
          offset: offset,
          client: client,
          producer_pid: producer_pid
        } = ack_data

        client.ack(group_coordinator, generation_id, topic, partition, offset)
        GenServer.call(producer_pid, {:offset_acked, offset, {generation_id, topic, partition}})
      catch
        kind, reason ->
          Logger.error(Exception.format(kind, reason, System.stacktrace()))
      end
    end)
  end
end
