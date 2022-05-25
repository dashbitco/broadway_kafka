defmodule BroadwayKafka.BrodClient do
  @moduledoc false

  require Logger

  @behaviour BroadwayKafka.KafkaClient

  # We only accept :commit_to_kafka_v2 for now so we hard coded the value
  # to avoid problems in case :brod's default policy changes in the future
  @offset_commit_policy :commit_to_kafka_v2

  @supported_group_config_options [
    :offset_commit_interval_seconds,
    :rejoin_delay_seconds,
    :session_timeout_seconds,
    :heartbeat_rate_seconds,
    :rebalance_timeout_seconds
  ]

  @supported_fetch_config_options [
    :min_bytes,
    :max_bytes,
    :max_wait_time
  ]

  @supported_client_config_options [
    :ssl,
    :sasl,
    :connect_timeout,
    :client_id_prefix
  ]

  @default_receive_interval 2000

  # Private option. Not exposed to the user
  @default_reconnect_timeout 1000

  @default_offset_commit_on_ack true

  @offset_reset_policy_values [:earliest, :latest]

  @default_offset_reset_policy :latest

  @impl true
  def init(opts) do
    with {:ok, hosts} <- validate(opts, :hosts, required: true),
         {:ok, group_id} <- validate(opts, :group_id, required: true),
         {:ok, topics} <- validate(opts, :topics, required: true),
         {:ok, receive_interval} <-
           validate(opts, :receive_interval, default: @default_receive_interval),
         {:ok, reconnect_timeout} <-
           validate(opts, :reconnect_timeout, default: @default_reconnect_timeout),
         {:ok, offset_commit_on_ack} <-
           validate(opts, :offset_commit_on_ack, default: @default_offset_commit_on_ack),
         {:ok, offset_reset_policy} <-
           validate(opts, :offset_reset_policy, default: @default_offset_reset_policy),
         {:ok, group_config} <- validate_group_config(opts),
         {:ok, fetch_config} <- validate_fetch_config(opts),
         {:ok, client_config} <- validate_client_config(opts) do
      {:ok,
       %{
         hosts: parse_hosts(hosts),
         group_id: group_id,
         topics: topics,
         receive_interval: receive_interval,
         reconnect_timeout: reconnect_timeout,
         offset_commit_on_ack: offset_commit_on_ack,
         offset_reset_policy: offset_reset_policy,
         group_config: [{:offset_commit_policy, @offset_commit_policy} | group_config],
         fetch_config: Map.new(fetch_config || []),
         client_config: client_config
       }}
    end
  end

  @impl true
  def setup(stage_pid, client_id, callback_module, config) do
    with :ok <- :brod.start_client(config.hosts, client_id, config.client_config),
         {:ok, group_coordinator} <-
           start_link_group_coordinator(stage_pid, client_id, callback_module, config) do
      Process.monitor(client_id)
      Process.monitor(group_coordinator)
      Process.unlink(group_coordinator)
      {:ok, group_coordinator}
    end
  end

  @impl true
  def fetch(client_id, topic, partition, offset, opts, _config) do
    :brod.fetch(client_id, topic, partition, offset, opts)
  end

  @impl true
  def ack(group_coordinator, generation_id, topic, partition, offset, config) do
    :brod_group_coordinator.ack(group_coordinator, generation_id, topic, partition, offset)

    if group_coordinator && config.offset_commit_on_ack do
      :brod_group_coordinator.commit_offsets(group_coordinator, [{{topic, partition}, offset}])
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
  def stop_group_coordinator(nil) do
    :ok
  end

  def stop_group_coordinator(group_coordinator) do
    ref = Process.monitor(group_coordinator)
    Process.exit(group_coordinator, :kill)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end

    :ok
  end

  @impl true
  def disconnect(client_id) do
    :ok = :brod.stop_client(client_id)
    :ok
  end

  @impl true
  def resolve_offset(topic, partition, current_offset, offset_reset_policy, config) do
    policy = offset_reset_policy_value(offset_reset_policy)

    if current_offset == :undefined do
      lookup_offset(config.hosts, topic, partition, policy, config.client_config)
    else
      case :brod.fetch({config.hosts, config.client_config}, topic, partition, current_offset) do
        {:ok, _} ->
          current_offset

        {:error, :offset_out_of_range} ->
          lookup_offset(config.hosts, topic, partition, policy, config.client_config)
      end
    end
  end

  defp lookup_offset(hosts, topic, partition, policy, client_config) do
    case :brod.resolve_offset(hosts, topic, partition, policy, client_config) do
      {:ok, offset} ->
        offset

      {:error, reason} ->
        raise "cannot resolve begin offset (hosts=#{inspect(hosts)} topic=#{topic} " <>
                "partition=#{partition}). Reason: #{inspect(reason)}"
    end
  end

  @impl true
  def update_topics(group_coordinator, topics) do
    :brod_group_coordinator.update_topics(group_coordinator, topics)
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

  defp validate(opts, key, options \\ []) when is_list(opts) do
    has_key = Keyword.has_key?(opts, key)
    required = Keyword.get(options, :required, false)
    default = Keyword.get(options, :default)

    cond do
      has_key ->
        validate_option(key, opts[key])

      required ->
        {:error, "#{inspect(key)} is required"}

      default != nil ->
        validate_option(key, default)

      true ->
        {:ok, nil}
    end
  end

  defp validate_option(:hosts, value) do
    if supported_hosts?(value) do
      {:ok, value}
    else
      validation_error(
        :hosts,
        "a list of host/port pairs or a single string of comma separated HOST:PORT pairs",
        value
      )
    end
  end

  defp validate_option(:group_id, value) when not is_binary(value) or value == "",
    do: validation_error(:group_id, "a non empty string", value)

  defp validate_option(:topics, value) do
    if is_list(value) && Enum.all?(value, &is_binary/1) do
      {:ok, value}
    else
      validation_error(:topics, "a list of strings", value)
    end
  end

  defp validate_option(:receive_interval, value) when not is_integer(value) or value < 0,
    do: validation_error(:receive_interval, "a non-negative integer", value)

  defp validate_option(:reconnect_timeout, value) when not is_integer(value) or value < 0,
    do: validation_error(:reconnect_timeout, "a non-negative integer", value)

  defp validate_option(:offset_commit_on_ack, value) when not is_boolean(value),
    do: validation_error(:offset_commit_on_ack, "a boolean", value)

  defp validate_option(:offset_reset_policy, value)
       when value not in @offset_reset_policy_values do
    validation_error(
      :offset_reset_policy,
      "one of #{inspect(@offset_reset_policy_values)}",
      value
    )
  end

  defp validate_option(:offset_commit_interval_seconds, value)
       when not is_integer(value) or value < 1,
       do: validation_error(:offset_commit_interval_seconds, "a positive integer", value)

  defp validate_option(:rejoin_delay_seconds, value) when not is_integer(value) or value < 0,
    do: validation_error(:rejoin_delay_seconds, "a non-negative integer", value)

  defp validate_option(:session_timeout_seconds, value) when not is_integer(value) or value < 1,
    do: validation_error(:session_timeout_seconds, "a positive integer", value)

  defp validate_option(:heartbeat_rate_seconds, value) when not is_integer(value) or value < 1,
    do: validation_error(:heartbeat_rate_seconds, "a positive integer", value)

  defp validate_option(:rebalance_timeout_seconds, value) when not is_integer(value) or value < 1,
    do: validation_error(:rebalance_timeout_seconds, "a positive integer", value)

  defp validate_option(:min_bytes, value) when not is_integer(value) or value < 1,
    do: validation_error(:min_bytes, "a positive integer", value)

  defp validate_option(:max_bytes, value) when not is_integer(value) or value < 1,
    do: validation_error(:max_bytes, "a positive integer", value)

  defp validate_option(:max_wait_time, value) when not is_integer(value) or value < 1,
    do: validation_error(:max_wait_time, "a positive integer", value)

  defp validate_option(:client_id_prefix, value) when not is_binary(value),
    do: validation_error(:client_id_prefix, "a string", value)

  defp validate_option(:sasl, :undefined),
    do: {:ok, :undefined}

  defp validate_option(:sasl, value = {:callback, _callback_module, _opts}),
    do: {:ok, value}

  defp validate_option(:sasl, value) do
    with {mechanism, username, password}
         when mechanism in [:plain, :scram_sha_256, :scram_sha_512] and
                is_binary(username) and
                is_binary(password) <- value do
      {:ok, value}
    else
      _value -> validation_error(:sasl, "a tuple of SASL mechanism, username and password", value)
    end
  end

  defp validate_option(:ssl, value) when is_boolean(value), do: {:ok, value}

  defp validate_option(:ssl, value) do
    if Keyword.keyword?(value) do
      {:ok, value}
    else
      validation_error(:ssl, "a keyword list of SSL/TLS client options", value)
    end
  end

  defp validate_option(:connect_timeout, value) when not is_integer(value) or value < 1,
    do: validation_error(:connect_timeout, "a positive integer", value)

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_group_config(opts) do
    with {:ok, [_ | _] = config} <-
           validate_supported_opts(opts, :group_config, @supported_group_config_options),
         {:ok, _} <- validate(config, :offset_commit_interval_seconds),
         {:ok, _} <- validate(config, :rejoin_delay_seconds),
         {:ok, _} <- validate(config, :session_timeout_seconds),
         {:ok, _} <- validate(config, :heartbeat_rate_seconds),
         {:ok, _} <- validate(config, :rebalance_timeout_seconds) do
      {:ok, config}
    end
  end

  defp validate_fetch_config(opts) do
    with {:ok, [_ | _] = config} <-
           validate_supported_opts(opts, :fetch_config, @supported_fetch_config_options),
         {:ok, _} <- validate(config, :min_bytes),
         {:ok, _} <- validate(config, :max_bytes),
         {:ok, _} <- validate(config, :max_wait_time) do
      {:ok, config}
    end
  end

  defp validate_client_config(opts) do
    with {:ok, [_ | _] = config} <-
           validate_supported_opts(opts, :client_config, @supported_client_config_options),
         {:ok, _} <- validate(config, :client_id_prefix),
         {:ok, _} <- validate(config, :sasl),
         {:ok, _} <- validate(config, :ssl),
         {:ok, _} <- validate(config, :connect_timeout) do
      {:ok, config}
    end
  end

  defp validate_supported_opts(all_opts, group_name, supported_opts) do
    opts = Keyword.get(all_opts, group_name, [])

    opts
    |> Keyword.keys()
    |> Enum.reject(fn k -> k in supported_opts end)
    |> case do
      [] -> {:ok, opts}
      keys -> {:error, "Unsupported options #{inspect(keys)} for #{inspect(group_name)}"}
    end
  end

  defp offset_reset_policy_value(policy) do
    case policy do
      :earliest ->
        -2

      :latest ->
        -1
    end
  end

  defp supported_hosts?(hosts_single_binary) when is_binary(hosts_single_binary) do
    String.match?(hosts_single_binary, ~r/^(.+:[\d]+)(,.+:[\d]+)?$/)
  end

  defp supported_hosts?([{key, _value} | rest]) when is_binary(key) or is_atom(key),
    do: supported_hosts?(rest)

  defp supported_hosts?([]), do: true
  defp supported_hosts?(_other), do: false

  defp parse_hosts(hosts_single_binary) when is_binary(hosts_single_binary) do
    hosts_single_binary
    |> String.split(",")
    |> Enum.map(fn host_port ->
      [host, port] = String.split(host_port, ":")
      {host, String.to_integer(port)}
    end)
  end

  defp parse_hosts(hosts), do: hosts
end
