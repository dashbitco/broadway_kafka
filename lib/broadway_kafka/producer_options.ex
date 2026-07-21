defmodule BroadwayKafka.ProducerOptions do
  @moduledoc false

  group_config_schema = [
    offset_commit_interval_seconds: [
      type: :pos_integer,
      doc: """
      The time in seconds between two offset commit requests.
      """
    ],
    rejoin_delay_seconds: [
      type: :non_neg_integer,
      doc: """
      The delay in seconds before rejoining the group.
      """
    ],
    session_timeout_seconds: [
      type: :pos_integer,
      doc: """
      The time in seconds that the group coordinator waits before it considers a member down.
      A group member may also consider the coordinator down if it receives no heartbeat response
      within this time.
      """
    ],
    heartbeat_rate_seconds: [
      type: :pos_integer,
      doc: """
      The time in seconds between heartbeats sent to the group coordinator. This must be less
      than `:session_timeout_seconds` and is often no more than one third of that value.
      """
    ],
    rebalance_timeout_seconds: [
      type: :pos_integer,
      doc: """
      The time in seconds that each worker has to join the group after a rebalance starts.
      If a worker takes longer, Kafka removes it from the group.
      """
    ]
  ]

  fetch_config_schema = [
    min_bytes: [
      type: :pos_integer,
      doc: """
      The least amount of data that the server should return. The request waits until this much
      data is ready. A larger value may improve throughput at the cost of more delay.
      """
    ],
    max_bytes: [
      type: :pos_integer,
      doc: """
      The most data to fetch from one partition at a time. A larger value may improve throughput
      at the cost of more memory use.
      """
    ],
    max_wait_time: [
      type: :pos_integer,
      doc: """
      The most time, in milliseconds, that the broker may wait for `:min_bytes` of data.
      """
    ]
  ]

  client_config_schema = [
    client_id_prefix: [
      type: :string,
      doc: """
      A string added to the client ID that BroadwayKafka builds for `:brod`.
      """
    ],
    sasl: [
      type:
        {:or,
         [
           {:in, [:undefined]},
           {:custom, __MODULE__, :validate_sasl_tuple, []}
         ]},
      doc: """
      The SASL settings. Pass `{mechanism, username, password}` or `{mechanism, path}`, where
      `mechanism` is `:plain`, `:scram_sha_256`, or `:scram_sha_512`. You may also pass
      `{:callback, module, options}` for a SASL plug-in. Defaults to `:undefined`.
      """
    ],
    ssl: [
      type: {:or, [:boolean, :keyword_list]},
      doc: """
      A boolean or a keyword list of SSL/TLS client options.
      See `t::ssl.tls_client_option/0`.
      """
    ],
    connect_timeout: [
      type: :pos_integer,
      doc: """
      The time, in milliseconds, allowed for a connection to Kafka.
      """
    ],
    request_timeout: [
      type: {:custom, __MODULE__, :validate_integer_gte, [:request_timeout, 1_000]},
      doc: """
      The time, in milliseconds, allowed for a response from Kafka. It must be at least `1_000`.
      """
    ],
    query_api_versions: [
      type: :boolean,
      doc: """
      Whether to ask Kafka which API versions it supports when a connection starts. Set this to
      `false` for Kafka versions before 0.10.
      """
    ],
    extra_sock_opts: [
      type: {:list, :any},
      doc: """
      Extra `gen_tcp` socket options. For example, use `[:inet6]` for an IPv6 broker.
      Defaults to `[]`.
      """
    ],
    allow_topic_auto_creation: [
      type: :boolean,
      doc: """
      Whether `:brod` may send metadata requests that can create a topic when the broker allows
      automatic topic creation. Defaults to `true`.
      """
    ]
  ]

  schema = [
    hosts: [
      type: {:custom, __MODULE__, :validate_hosts, []},
      required: true,
      doc: """
      A list of host and port pairs, or one string of comma-separated `HOST:PORT` pairs, used
      to make the first connection to Kafka. For example:

          [localhost: 9092]
          [{"kafka-vm1", 9092}, {"kafka-vm2", 9092}]
          "kafka-vm1:9092,kafka-vm2:9092"
      """
    ],
    group_id: [
      type: {:custom, __MODULE__, :validate_nonempty_string, [:group_id]},
      required: true,
      doc: """
      A non-empty string that names the consumer group that the producer will join.
      """
    ],
    topics: [
      type: {:list, :string},
      required: true,
      doc: """
      A list of topics that the producer will subscribe to.
      """
    ],
    receive_interval: [
      type: :non_neg_integer,
      default: 2_000,
      doc: """
      The time, in milliseconds, that the producer waits before asking for more messages.
      """
    ],
    reconnect_timeout: [
      type: :non_neg_integer,
      default: 1_000,
      doc: false
    ],
    offset_commit_on_ack: [
      type: :boolean,
      default: true,
      doc: """
      Whether Broadway sends an offset commit request after each acknowledgement. Setting this
      to `false` may improve throughput because commits will follow
      `:offset_commit_interval_seconds`. A long commit interval may cause more records to run
      again after a restart or lost connection, so message handling should be safe to repeat.
      """
    ],
    offset_reset_policy: [
      type: {:custom, __MODULE__, :validate_offset_reset_policy, []},
      default: :latest,
      doc: """
      The offset to use when Kafka has no saved offset or the saved offset has expired. Use
      `:earliest`, `:latest`, or `{:timestamp, timestamp}`, where `timestamp` is a non-negative
      time in milliseconds. Defaults to `:latest`.
      """
    ],
    begin_offset: [
      type: {:in, [:assigned, :reset]},
      default: :assigned,
      doc: """
      How consumers choose their first offset. `:assigned` uses the offsets from the Kafka
      partition assignments. `:reset` uses `:offset_reset_policy`. Defaults to `:assigned`.
      """
    ],
    shared_client: [
      type: :boolean,
      default: false,
      doc: """
      Whether all producers share one client. Sharing can cut memory and other resource use,
      but it may cause a large drop in throughput.
      """
    ],
    group_config: [
      type: :keyword_list,
      default: [],
      keys: group_config_schema,
      doc: """
      Options passed to `:brod`'s group coordinator.
      """
    ],
    fetch_config: [
      type: :keyword_list,
      default: [],
      keys: fetch_config_schema,
      doc: """
      Options passed to `:brod.fetch/5` when it fetches messages.
      """
    ],
    client_config: [
      type: :keyword_list,
      default: [],
      keys: client_config_schema,
      doc: """
      Options passed to `:brod.start_client/3` when it starts a client.
      """
    ]
  ]

  @schema schema

  def schema, do: @schema

  @doc false
  def validate_hosts(hosts) when is_binary(hosts) do
    hosts
    |> String.split(",")
    |> Enum.reduce_while({:ok, []}, fn host, {:ok, parsed_hosts} ->
      case String.split(host, ":") do
        [host, port] when host != "" ->
          case Integer.parse(port) do
            {port, ""} when port > 0 ->
              {:cont, {:ok, [{host, port} | parsed_hosts]}}

            _other ->
              {:halt, :error}
          end

        _other ->
          {:halt, :error}
      end
    end)
    |> case do
      {:ok, parsed_hosts} ->
        {:ok, Enum.reverse(parsed_hosts)}

      :error ->
        {:error,
         "expected :hosts to be a comma-separated string of HOST:PORT pairs, got: #{inspect(hosts)}"}
    end
  end

  def validate_hosts(hosts) when is_list(hosts) do
    if Enum.all?(hosts, &valid_host?/1) do
      {:ok, hosts}
    else
      {:error, "expected :hosts to be a list of host/port pairs, got: #{inspect(hosts)}"}
    end
  end

  def validate_hosts(hosts) do
    {:error, "expected :hosts to be a list or a string, got: #{inspect(hosts)}"}
  end

  @doc false
  def validate_integer_gte(val, _opt, floor) when is_integer(val) and val >= floor,
    do: {:ok, val}

  def validate_integer_gte(val, opt, floor),
    do:
      {:error,
       "expected #{inspect(opt)} to be a positive integer >= #{inspect(floor)}, got: #{inspect(val)}"}

  @doc false
  def validate_nonempty_string(val, _opt) when is_binary(val) and val != "", do: {:ok, val}

  def validate_nonempty_string(val, opt) do
    {:error, "expected #{inspect(opt)} to be a non empty string, got: #{inspect(val)}"}
  end

  @doc false
  def validate_offset_reset_policy(policy) when policy in [:earliest, :latest], do: {:ok, policy}

  def validate_offset_reset_policy({:timestamp, timestamp} = policy)
      when is_integer(timestamp) and timestamp >= 0,
      do: {:ok, policy}

  def validate_offset_reset_policy(policy) do
    {:error,
     "expected :offset_reset_policy to be one of [:earliest, :latest] or `{:timestamp, timestamp}` where timestamp is a non-negative integer, got: #{inspect(policy)}"}
  end

  @doc false
  def validate_sasl_tuple({:callback, callback_module, _options} = sasl)
      when is_atom(callback_module),
      do: {:ok, sasl}

  def validate_sasl_tuple({mechanism, username, password} = sasl)
      when mechanism in [:plain, :scram_sha_256, :scram_sha_512] and
             is_binary(username) and is_binary(password),
      do: {:ok, sasl}

  def validate_sasl_tuple({mechanism, path} = sasl)
      when mechanism in [:plain, :scram_sha_256, :scram_sha_512] and is_binary(path),
      do: {:ok, sasl}

  def validate_sasl_tuple(sasl) do
    {:error,
     "expected :sasl to be a tuple of SASL mechanism, username and password, or mechanism and path, got: #{inspect(sasl)}"}
  end

  defp valid_host?({host, port})
       when (is_binary(host) or is_atom(host)) and is_integer(port) and port > 0,
       do: true

  defp valid_host?(_host), do: false
end
