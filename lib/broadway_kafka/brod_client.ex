defmodule BroadwayKafka.BrodClient do
  @moduledoc false

  @behaviour BroadwayKafka.KafkaClient

  alias BroadwayKafka.ProducerOptions

  # We only accept :commit_to_kafka_v2 for now so we hard coded the value
  # to avoid problems in case :brod's default policy changes in the future
  @offset_commit_policy :commit_to_kafka_v2
  @offset_resolution_attempts 3
  @offset_resolution_backoff_ms 100

  @impl true
  def init(opts) do
    broadway_opts = opts[:broadway]

    with {:ok, opts} <-
           opts
           |> Keyword.delete(:broadway)
           |> NimbleOptions.validate(ProducerOptions.schema()) do
      config = %{
        hosts: opts[:hosts],
        group_id: opts[:group_id],
        topics: opts[:topics],
        receive_interval: opts[:receive_interval],
        reconnect_timeout: opts[:reconnect_timeout],
        offset_commit_on_ack: opts[:offset_commit_on_ack],
        offset_reset_policy: opts[:offset_reset_policy],
        begin_offset: opts[:begin_offset],
        group_config: [{:offset_commit_policy, @offset_commit_policy} | opts[:group_config]],
        fetch_config: Map.new(opts[:fetch_config]),
        client_config: opts[:client_config],
        shared_client: opts[:shared_client],
        shared_client_id: build_shared_client_id(opts, broadway_opts)
      }

      {:ok, shared_client_child_spec(config), config}
    end
  end

  @impl true
  def setup(stage_pid, client_id, callback_module, config) do
    with :ok <- do_start_brod_client(config.hosts, client_id, config.client_config),
         {:ok, group_coordinator} <-
           start_link_group_coordinator(stage_pid, client_id, callback_module, config) do
      Process.monitor(client_id)
      ref = Process.monitor(group_coordinator)
      Process.unlink(group_coordinator)
      {:ok, group_coordinator, ref}
    end
  end

  @impl true
  def fetch(client_id, topic, partition, offset, opts, _config) do
    # These options drive BroadwayKafka's own retry logic and are not valid
    # :brod.fetch/5 options.
    opts = Map.drop(opts, [:max_fetch_retries, :fetch_retry_backoff_ms])

    :brod.fetch(client_id, topic, partition, offset, opts)
  end

  @impl true
  def ack(group_coordinator, generation_id, topic, partition, offset, config) do
    if group_coordinator do
      :brod_group_coordinator.ack(group_coordinator, generation_id, topic, partition, offset)

      if config.offset_commit_on_ack do
        :brod_group_coordinator.commit_offsets(group_coordinator, [{{topic, partition}, offset}])
      end
    end

    :ok
  end

  @impl true
  def connected?(client_id) do
    try do
      match?({:ok, _}, :brod_client.get_metadata(client_id, :all))
    catch
      _type, _reason ->
        false
    end
  end

  @impl true
  def disconnect(client_id) do
    :ok = :brod.stop_client(client_id)
    :ok
  end

  @impl true
  def resolve_offset(topic, partition, current_offset, offset_reset_policy, config) do
    policy = offset_reset_policy_value(offset_reset_policy)

    # This is only for testing.
    brod = Map.get(config, :brod_module, :brod)

    if current_offset == :undefined do
      lookup_offset(config.hosts, topic, partition, policy, config.client_config, brod)
    else
      result =
        retry_offset_resolution(fn ->
          brod.fetch({config.hosts, config.client_config}, topic, partition, current_offset)
        end)

      case result do
        {:ok, _} ->
          current_offset

        {:error, :offset_out_of_range} ->
          lookup_offset(config.hosts, topic, partition, policy, config.client_config, brod)

        {:error, reason} ->
          raise "cannot resolve offset (hosts=#{inspect(config.hosts)} topic=#{topic} " <>
                  "partition=#{partition}). Reason: #{inspect(reason)}"
      end
    end
  end

  defp shared_client_child_spec(%{shared_client: false}), do: []

  defp shared_client_child_spec(%{shared_client: true} = config) do
    [
      %{
        id: config.shared_client_id,
        start:
          {:brod, :start_link_client,
           [config.hosts, config.shared_client_id, config.client_config]}
      }
    ]
  end

  defp lookup_offset(hosts, topic, partition, policy, client_config, brod) do
    result =
      retry_offset_resolution(fn ->
        brod.resolve_offset(hosts, topic, partition, policy, client_config)
      end)

    case result do
      {:ok, -1} ->
        # `:brod.resolve_offset` returns -1 when asked to resolve a timestamp newer
        # than all the messages in the partition.
        # -1 is not a valid offset you can use with `:brod.fetch` so we need to
        # resolve the latest offset instead
        lookup_offset(hosts, topic, partition, :latest, client_config, brod)

      {:ok, offset} ->
        offset

      {:error, reason} ->
        raise "cannot resolve begin offset (hosts=#{inspect(hosts)} topic=#{topic} " <>
                "partition=#{partition}). Reason: #{inspect(reason)}"
    end
  end

  defp retry_offset_resolution(fun, attempts_left \\ @offset_resolution_attempts) do
    case fun.() do
      {:error, reason} when reason != :offset_out_of_range and attempts_left > 1 ->
        Process.sleep(@offset_resolution_backoff_ms)
        retry_offset_resolution(fun, attempts_left - 1)

      result ->
        result
    end
  end

  @impl true
  def update_topics(group_coordinator, topics) do
    if group_coordinator do
      :brod_group_coordinator.update_topics(group_coordinator, topics)
    end

    :ok
  end

  defp start_link_group_coordinator(stage_pid, client_id, callback_module, config) do
    :brod_group_coordinator.start_link(
      client_id,
      config.group_id,
      config.topics,
      config.group_config,
      callback_module,
      stage_pid
    )
  end

  defp offset_reset_policy_value(policy) do
    case policy do
      :earliest ->
        -2

      :latest ->
        -1

      {:timestamp, timestamp} when is_integer(timestamp) and timestamp >= 0 ->
        timestamp
    end
  end

  defp build_shared_client_id(opts, broadway_opts) do
    if opts[:shared_client] do
      prefix = get_in(opts, [:client_config, :client_id_prefix])
      broadway_name = broadway_opts[:name]
      :"#{prefix}#{Module.concat(broadway_name, SharedClient)}"
    end
  end

  defp do_start_brod_client(hosts, client_id, client_config) do
    case :brod.start_client(hosts, client_id, client_config) do
      :ok ->
        :ok

      # Because  we are starting the client on the broadway supervison tree
      # instead of the :brod supervisor, the already_started error
      # is not properly handled by :brod.start_client/3 for shared clients
      # So we must handle it here.
      {:error, {{:already_started, _}, _}} ->
        :ok

      error ->
        error
    end
  end
end
