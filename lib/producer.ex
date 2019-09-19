defmodule BroadwayKafka.Producer do
  use GenStage

  require Logger
  import Record, only: [defrecord: 2, extract: 2]

  alias Broadway.{Message, Acknowledger, Producer}

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
          pending_messages: [],
          config: config
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
          key: {topic, partition}
        }
      end)
    schedule_receive_messages(0)
    {:noreply, [], %{state | assignments: state.assignments ++ assignments}}
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
    %{pending_messages: pending_messages, assignments: assignments} = state

    [assignment | other_assignments] = assignments
    # IO.puts("Fetching messages for assignment #{inspect(assignment)}")

    {new_assignment, messages} = fetch_messages_from_kafka(state, assignment)
    {messages, pending_messages} = Enum.split(pending_messages ++ messages, demand)
    new_demand = demand - length(messages)

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
        pending_messages: pending_messages,
        assignments: other_assignments ++ [new_assignment]
      }
    {:noreply, messages, new_state}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
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

    case client.fetch(client_id, topic, partition, offset, %{max_bytes: 100_000}, config) do
      {:ok, {new_offset, messages}} ->
        messages =
          Enum.map(messages, fn msg ->
            wrap_message(msg, topic, partition, generation_id, group_coordinator, client)
          end)

        # if messages != [] do
        #   IO.inspect(messages, label: "RECEIVED MESSAGES on #{inspect(self())}")
        # end

        {%{assignment | offset: new_offset}, messages}

      {:error, reason} ->
        # TODO: Treat the error properly
        IO.inspect(reason, label: "ERROR")
        {assignment, []}
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
    %Message{
      data: data,
      metadata: %{partition: partition, key: key, ts: ts},
      acknowledger: {__MODULE__, {topic, partition}, ack_data}
    }
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
  def dispatch_key(message) do
    %{metadata: %{partition: partition}} = message
    partition
  end

  defp ack_messages(messages, {topic, partition}) do
    Enum.each(messages, fn msg ->
      {_, _, ack_data} = msg.acknowledger

      try do
        %{group_coordinator: group_coordinator, generation_id: generation_id, offset: offset, client: client} = ack_data
        client.ack(group_coordinator, generation_id, topic, partition, offset)
        # IO.inspect(%{generation_id: generation_id, topic: topic, partition: partition, offset: offset, data: msg.data}, label: "Acking")
      catch
        kind, reason ->
          Logger.error(Exception.format(kind, reason, System.stacktrace()))
      end
    end)
  end

  @impl :brod_group_member
  def get_committed_offsets(_pid, _topics_partitions) do
    {:ok, [], -1}
  end

  @impl :brod_group_member
  def assignments_received(pid, _group_member_id, group_generation_id, received_assignments) do
    # IO.inspect({pid, group_member_id, group_generation_id, received_assignments}, label: :ASSIGNMENTS_RECEIVED)
    send(pid, {:put_assignments, group_generation_id, received_assignments})
    :ok
  end

  @impl :brod_group_member
  def assignments_revoked(pid) do
    # IO.inspect(pid, label: "ASSIGNMENTS_REVOKED")
    send(pid, {:put_assignments, nil, []})
    :ok
  end
end
