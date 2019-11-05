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
    :session_timeout_seconds
  ]

  @supported_fetch_config_options [
    :min_bytes,
    :max_bytes
  ]

  @impl true
  def init(opts) do
    with {:ok, hosts} <- validate(opts, :hosts),
         {:ok, group_id} <- validate(opts, :group_id),
         {:ok, topics} <- validate(opts, :topics),
         {:ok, group_config} <- validate_group_config(opts),
         {:ok, fetch_config} <- validate_fetch_config(opts) do
      {:ok,
       %{
          hosts: hosts,
          group_id: group_id,
          topics: topics,
          group_config: [{:offset_commit_policy, @offset_commit_policy} | group_config],
          fetch_config: Map.new(fetch_config || [])
       }}
    end
  end

  @impl true
  def setup(stage_pid, client_id, callback_module, config) do
    with :ok <- :brod.start_client(config.hosts, client_id, _client_config=[]),
         {:ok, group_coordinator} <- start_link_group_coordinator(stage_pid, client_id, callback_module, config) do
      {:ok, group_coordinator}
    end
  end

  @impl true
  def fetch(client_id, topic, partition, offset, opts, _config) do
    :brod.fetch(client_id, topic, partition, offset, opts)
  end

  @impl true
  def ack(group_coordinator, generation_id, topic, partition, offset) do
    :brod_group_coordinator.ack(group_coordinator, generation_id, topic, partition, offset)
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

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(:group_id, value) when not is_binary(value) or value == "",
    do: validation_error(:group_id, "a non empty string", value)

  defp validate_option(:topics, value) when is_list(value) do
    if Enum.all?(value, &is_binary/1),
      do: {:ok, value},
      else: validation_error(:metadata, "a list of strings", value)
  end

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_group_config(opts) do
    validate_supported_opts(opts, :group_config, @supported_group_config_options)
  end

  defp validate_fetch_config(opts) do
    validate_supported_opts(opts, :fetch_config, @supported_fetch_config_options)
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
end
