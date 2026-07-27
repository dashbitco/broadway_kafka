defmodule BroadwayKafka.ProducerOptions do
  @moduledoc false

  group_config_schema = [
    group_instance_id: [
      type: {:custom, __MODULE__, :validate_nonempty_string, [:group_instance_id]},
      doc: """
      A unique, non-empty string that identifies this consumer group member across restarts.
      This enables [static group membership](https://kafka.apache.org/39/design/design/#static-membership)
      and requires `:brod` 4.4.2 or later. *Available since v0.6.0*.
      """
    ],
    offset_commit_interval_seconds: [
      type: :pos_integer,
      default: 5,
      doc: """
      The time *in seconds* between two `OffsetCommitRequest` messages.
      """
    ],
    rejoin_delay_seconds: [
      type: :non_neg_integer,
      default: 1,
      doc: """
      The delay *in seconds* before rejoining the group.
      """
    ],
    session_timeout_seconds: [
      type: :pos_integer,
      default: 30,
      doc: """
      The time *in seconds* that the group coordinator waits before it considers a member "down"
      (if no heartbeat or any kind of request is received). A group member may also consider
      the coordinator "down" if it receives no heartbeat response within this time.
      """
    ],
    heartbeat_rate_seconds: [
      type: :pos_integer,
      default: 5,
      doc: """
      The time *in seconds* between heartbeats ("ping" requests) sent to the group coordinator.
      Heartbeats are used to ensure that the consumer's session stays active and
      to facilitate rebalancing when new consumers join or leave the group.
      The value must be set lower than `:session_timeout_seconds`, typically equal to or lower
      than ⅓ of that value. It can be adjusted even lower to control the expected time
      for normal rebalances.
      """
    ],
    rebalance_timeout_seconds: [
      type: :pos_integer,
      default: 30,
      doc: """
      The time *in seconds* that each worker has to join the group after a rebalance starts.
      If the timeout is exceeded, then the worker will be removed from the group,
      which will cause offset commit failures.
      """
    ]
  ]

  fetch_config_schema = [
    min_bytes: [
      type: :pos_integer,
      default: 1,
      doc: """
      The minimum amount of data to be fetched from the server.
      If not enough data is available the request will wait for that much data to accumulate
      before answering. Setting this value greater than `1` can improve
      server throughput a bit at the cost of additional latency.
      """
    ],
    max_bytes: [
      type: :pos_integer,
      default: _1mb = 1024 * 1024,
      doc: """
      The most data to fetch from one partition at a time. A larger value may improve throughput
      at the cost of more memory use.
      """
    ],
    max_wait_time: [
      type: :pos_integer,
      default: 1_000,
      doc: """
      The most time (in millisecond) that the broker may wait for `:min_bytes` of data.
      """
    ],
    max_fetch_retries: [
      type: :non_neg_integer,
      default: 3,
      doc: """
      How many times a failed fetch is retried in place when Kafka returns a retriable error,
      before the producer raises as it does for any other error. Set to `0` to disable retries.
      BroadwayKafka uses this option itself and does not pass it to `:brod.fetch/5`.
      """
    ],
    fetch_retry_backoff_ms: [
      type: :non_neg_integer,
      default: 500,
      doc: """
      The time, in milliseconds, to wait before the first retry of a failed fetch
      (see `:max_fetch_retries`). The wait time doubles on each later retry. BroadwayKafka uses
      this option itself and does not pass it to `:brod.fetch/5`.
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
      A tuple of "mechanism" (`:plain`, `:scram_sha_256`, or `:scram_sha_512`),
      username, and password. See `:brod`'s
      [`Authentication Support`](https://github.com/kafka4beam/brod#authentication-support)
      documentation for more information. You may also pass `{:callback, module, options}`
      for a SASL plug-in. Set this to `:undefined` to disable SASL.
      """
    ],
    ssl: [
      type: {:or, [:boolean, :keyword_list]},
      doc: """
      A boolean or a keyword list of SSL/TLS client options. See the
      [`tls_client_option`](http://erlang.org/doc/man/ssl.html#type-tls_client_option)
      documentation for more information.
      """
    ],
    connect_timeout: [
      type: :pos_integer,
      doc: """
      The time (in milliseconds) allowed for a connection to Kafka. Default is what
      `:brod` defaults to (5s at the time of writing).
      """
    ],
    request_timeout: [
      type: {:custom, __MODULE__, :validate_integer_gte, [:request_timeout, 1_000]},
      doc: """
      The time (in milliseconds) allowed for a response from Kafka. It must be at least `1_000`.
      Default is to use `:brod`'s default timeout which is currently 4 minutes (`240_000`).
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
      Extra `gen_tcp` socket options. [More info](https://www.erlang.org/doc/man/gen_tcp.html#type-option).
      Set to `[:inet6]` if your Kafka broker uses IPv6.
      """
    ],
    allow_topic_auto_creation: [
      type: :boolean,
      doc: """
      Whether `:brod` may send metadata requests that can create a topic when the broker allows
      automatic topic creation.
      """
    ]
  ]

  schema = [
    hosts: [
      type: {:custom, __MODULE__, :validate_hosts, []},
      required: true,
      doc: """
      A list of host and port pairs, or one string of comma-separated `HOST:PORT` pairs, used
      to make the first connection to Kafka. For example: `[localhost: 9092]`,
      `[{"kafka-vm1", 9092}, {"kafka-vm2", 9092}]`, `"kafka-vm1:9092,kafka-vm2:9092"`.
      """
    ],
    group_id: [
      type: {:custom, __MODULE__, :validate_nonempty_string, [:group_id]},
      required: true,
      doc: """
      A (non-empty) unique string that identifies the consumer group that the producer will join.
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
      The duration (in milliseconds) that the producer waits before making
      a request for more messages.
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
      Tells Broadway to send or not an offset commit request after each acknowledgement.
      Setting this value to `false` can increase performance since commit requests will
      respect the `:offset_commit_interval_seconds` option. However, setting long commit
      intervals might lead to a large number of duplicated records to be processed after
      a server restart or connection loss. If that's the case, make sure your logic is
      **idempotent** when consuming records to avoid inconsistencies. Also, bear
      in mind the the negative performance impact might be insignificant if you're using
      batchers since only one commit request will be performed per batch.
      """
    ],
    offset_reset_policy: [
      type: {:custom, __MODULE__, :validate_offset_reset_policy, []},
      default: :latest,
      doc: """
      The offset to use when Kafka has no saved offset or the saved offset has expired. Use
      `:earliest`, `:latest`, or `{:timestamp, timestamp}`, where `timestamp` is a non-negative
      time in milliseconds.
      """
    ],
    begin_offset: [
      type: {:in, [:assigned, :reset]},
      default: :assigned,
      doc: """
      How consumers choose their first offset. When set to `:assigned`, the starting offset
      will be the one returned in the Kafka partition assignments (the latest committed
      offsets for the consumer group). When set to `:reset`, the starting offset will be
      dictated by the `:offset_reset_policy` option, either starting from the `:earliest`
      or the `:latest` offsets of the topic.
      """
    ],
    shared_client: [
      type: :boolean,
      default: false,
      doc: """
      When `false`, it starts one `:brod` client per producer. When `true`, it starts a
      single shared `:brod` client across all producers (which may reduce
      memory/resource usage). May cause severe performance degradation, see
      ["Shared Client Performance"](#module-shared-client-performance) for details.
      """
    ],
    group_config: [
      type: :keyword_list,
      default: [],
      keys: group_config_schema,
      subsection: "### Group config",
      doc: """
      Options passed to `:brod`'s group coordinator.
      """
    ],
    fetch_config: [
      type: :keyword_list,
      default: [],
      keys: fetch_config_schema,
      subsection: "### Fetch config",
      doc: """
      The available options to configure how messages are fetched. Unless noted
      otherwise, they are internally passed to `:brod.fetch/5`.
      """
    ],
    client_config: [
      type: :keyword_list,
      default: [],
      keys: client_config_schema,
      subsection: "### Client config",
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
