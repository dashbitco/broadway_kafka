defmodule BroadwayKafka.Producer do
  use GenStage

  require Logger
  import Record, only: [defrecord: 2, extract: 2]

  alias Broadway.{Message, Acknowledger, Producer}
  alias BroadwayKafka.Allocator

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
          demand: 0,
          receive_timer: nil,
          receive_interval: receive_interval,
          assignments: [],
          config: config,
          broadway_index: opts[:broadway_index],
          processors_name: opts[:processors_name],
          batchers_names: opts[:batchers_names],
          broadway_name: opts[:broadway_name]
        }
        {:producer, connect(state)}
    end
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  @impl GenStage
  def handle_info({:put_assignments, _group_generation_id, []}, state) do
    {:noreply, [], %{state | assignments: []}}
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
          begin_offset: begin_offset,
          offset: begin_offset,
          key: {topic, partition},
          pending_messages: []
        }
      end)

    broadway_name = state.broadway_name

    group_name = state.processors_name
    broadway_index = state.broadway_index
    allocator_name = Module.concat([broadway_name, "Allocator_processor_#{group_name}"])
    Allocator.allocate(allocator_name, broadway_index, Enum.map(assignments, & &1.key))

    for name <- state.batchers_names do
      allocator_name = Module.concat([broadway_name, "Allocator_batcher_consumer_#{name}"])
      Allocator.allocate(allocator_name, broadway_index, Enum.map(assignments, & &1.key))
    end

    schedule_receive_messages(0)
    {:noreply, [], %{state | assignments: assignments}}
  end

  @impl GenStage
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  defp handle_receive_messages(%{assignments: []} = state) do
    schedule_receive_messages(state.receive_interval)
    {:noreply, [], state}
  end

  defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    %{assignments: assignments} = state

    [assignment | other_assignments] = assignments

    {messages, pending_messages, new_offset, new_demand} = fetch_messages(state, assignment, demand)

    updated_assignment = %{assignment | pending_messages: pending_messages, offset: new_offset}

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    new_state =
      %{state |
        demand: new_demand,
        receive_timer: receive_timer,
        assignments: other_assignments ++ [updated_assignment]
      }
    {:noreply, messages, new_state}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp fetch_messages(state, assignment, demand) do
    %{offset: offset, pending_messages: pending_messages} = assignment

    case fetch_pending_message(pending_messages, [], demand) do
      {messages, new_pending_messages, 0} ->
        {messages, new_pending_messages, offset, 0}

      {messages, [], new_demand} ->
        {fetched_messages, new_offset} = fetch_messages_from_kafka(state, assignment)
        {messages, new_pending_messages, new_demand} =
          fetch_pending_message(fetched_messages, Enum.reverse(messages), new_demand)
        {messages, new_pending_messages, new_offset, new_demand}
    end
  end

  defp fetch_pending_message([message | pending_messages], messages, demand) when demand > 0 do
    fetch_pending_message(pending_messages, [message | messages], demand - 1)
  end

  defp fetch_pending_message(pending_messages, messages, demand) do
    {Enum.reverse(messages), pending_messages, demand}
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

    fetch_config_map = Map.new(config[:fetch_config] || [])

    case client.fetch(client_id, topic, partition, offset, fetch_config_map, config) do
      {:ok, {_new_offset, messages}} ->
        messages =
          Enum.map(messages, fn msg ->
            wrap_message(msg, topic, partition, generation_id, group_coordinator, client)
          end)
        # TODO: This is suboptimal. It works for now but we need to find out why the
        # `new_offeset` returned by `:brod_utils.fetch/5` doesn't increment the value
        # properly sometimes. That's why we're retrieving from the last message for now.
        # In case we can't find a solution and we have to keep this, let's at least
        # replace the `map` above with a `reduce` so we traverse the list only once.
        new_offset =
          if messages == [] do
            offset
          else
            last = List.last(messages)
            {_, _, %{offset: last_offset}} = last.acknowledger
            last_offset + 1
          end
        {messages, new_offset}

      {:error, reason} ->
        # TODO: Treat the error properly
        IO.inspect(reason, label: "ERROR")
        {[], offset}
    end
  end

  defp wrap_message(kafka_msg, topic, partition, generation_id, group_coordinator, client) do
    kafka_message(value: data, offset: offset, key: key, ts: ts) = kafka_msg

    ack_data = %{
      generation_id: generation_id,
      group_coordinator: group_coordinator,
      offset: offset,
      client: client
    }

    message =
      %Message{
        data: data,
        metadata: %{topic: topic, partition: partition, key: key, ts: ts},
        acknowledger: {__MODULE__, {topic, partition}, ack_data}
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

  @impl Acknowledger
  def ack(key, successful, _failed) do
    ack_messages(successful, key)
  end

  @impl Producer
  def prepare_for_draining(_) do
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
    send(pid, {:put_assignments, group_generation_id, received_assignments})
    :ok
  end

  @impl :brod_group_member
  def assignments_revoked(pid) do
    send(pid, {:put_assignments, nil, []})
    :ok
  end

  defp build_allocator_spec_and_consumer_entry(broadway_name, prefix, producers_stages, consumer_entry) do
    {consumer_name, consumer_config} = consumer_entry
    consumer_stages = consumer_config[:stages]
    allocator_name = Module.concat([broadway_name, "Allocator_#{prefix}_#{consumer_name}"])
    partition_by = &Allocator.fetch!(allocator_name, {&1.metadata.topic, &1.metadata.partition})
    new_config = Keyword.put(consumer_config, :partition_by, partition_by)
    allocator = {BroadwayKafka.Allocator, {allocator_name, producers_stages, consumer_stages}}
    allocator_spec = Supervisor.child_spec(allocator, id: allocator_name)

    {allocator_spec, {consumer_name, new_config}}
  end

  defp ack_messages(messages, {topic, partition}) do
    Enum.each(messages, fn msg ->
      {_, _, ack_data} = msg.acknowledger

      try do
        %{group_coordinator: group_coordinator, generation_id: generation_id, offset: offset, client: client} = ack_data
        client.ack(group_coordinator, generation_id, topic, partition, offset)
      catch
        kind, reason ->
          Logger.error(Exception.format(kind, reason, System.stacktrace()))
      end
    end)
  end
end
