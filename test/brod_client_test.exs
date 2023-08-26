defmodule BroadwayKafka.BrodClientTest do
  use ExUnit.Case

  alias BroadwayKafka.BrodClient

  @opts [
    group_id: "group",
    hosts: [host: 9092],
    topics: ["topic"],
    group_config: [],
    fetch_config: [],
    client_config: []
  ]

  describe "validate init options" do
    test ":hosts is a required value" do
      opts = Keyword.delete(@opts, :hosts)
      assert BrodClient.init(opts) == {:error, ":hosts is required"}
    end

    test ":hosts is valid as a keyword list or a list of tuples or a single binary" do
      expected_msg =
        "expected :hosts to be a list of host/port pairs or a single " <>
          "string of comma separated HOST:PORT pairs, got: "

      opts = Keyword.put(@opts, :hosts, :an_atom)
      assert BrodClient.init(opts) == {:error, expected_msg <> ":an_atom"}

      opts = Keyword.put(@opts, :hosts, "host")
      assert BrodClient.init(opts) == {:error, expected_msg <> ~s/"host"/}

      opts = Keyword.put(@opts, :hosts, "host:9092,")
      assert BrodClient.init(opts) == {:error, expected_msg <> ~s/"host:9092,"/}

      opts = Keyword.put(@opts, :hosts, host: 9092)
      assert {:ok, %{hosts: [host: 9092]}} = BrodClient.init(opts)

      opts = Keyword.put(@opts, :hosts, [{"host", 9092}])
      assert {:ok, %{hosts: [{"host", 9092}]}} = BrodClient.init(opts)

      opts = Keyword.put(@opts, :hosts, "host:9092")
      assert {:ok, %{hosts: [{"host", 9092}]}} = BrodClient.init(opts)

      opts = Keyword.put(@opts, :hosts, "host1:9092,host2:9092")
      assert {:ok, %{hosts: [{"host1", 9092}, {"host2", 9092}]}} = BrodClient.init(opts)
    end

    test ":group_id is a required string" do
      opts = Keyword.delete(@opts, :group_id)
      assert BrodClient.init(opts) == {:error, ":group_id is required"}

      opts = Keyword.put(@opts, :group_id, :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :group_id to be a non empty string, got: :an_atom"}

      opts = Keyword.put(@opts, :group_id, "my_group")
      assert {:ok, %{group_id: "my_group"}} = BrodClient.init(opts)
    end

    test ":topics is a required list of strings" do
      opts = Keyword.delete(@opts, :topics)
      assert BrodClient.init(opts) == {:error, ":topics is required"}

      opts = Keyword.put(@opts, :topics, :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :topics to be a list of strings, got: :an_atom"}

      opts = Keyword.put(@opts, :topics, ["topic_1", "topic_2"])
      assert {:ok, %{topics: ["topic_1", "topic_2"]}} = BrodClient.init(opts)
    end

    test ":receive_interval is a non-negative integer with default value 2000" do
      opts = Keyword.delete(@opts, :receive_interval)
      assert {:ok, %{receive_interval: 2000}} = BrodClient.init(opts)

      opts = Keyword.put(@opts, :receive_interval, :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :receive_interval to be a non-negative integer, got: :an_atom"}

      opts = Keyword.put(@opts, :receive_interval, 1000)
      assert {:ok, %{receive_interval: 1000}} = BrodClient.init(opts)
    end

    test ":reconnect_timeout is a non-negative integer with default value 1000" do
      assert {:ok, %{reconnect_timeout: 1000}} = BrodClient.init(@opts)

      opts = Keyword.put(@opts, :reconnect_timeout, :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :reconnect_timeout to be a non-negative integer, got: :an_atom"}

      opts = Keyword.put(@opts, :reconnect_timeout, 2000)
      assert {:ok, %{reconnect_timeout: 2000}} = BrodClient.init(opts)
    end

    test ":offset_commit_on_ack is a boolean with default value true" do
      assert {:ok, %{offset_commit_on_ack: true}} = BrodClient.init(@opts)

      opts = Keyword.put(@opts, :offset_commit_on_ack, :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :offset_commit_on_ack to be a boolean, got: :an_atom"}

      opts = Keyword.put(@opts, :offset_commit_on_ack, false)
      assert {:ok, %{offset_commit_on_ack: false}} = BrodClient.init(opts)
    end

    test ":offset_reset_policy can be :earliest or :latest. Default is :latest" do
      assert {:ok, %{offset_reset_policy: :latest}} = BrodClient.init(@opts)

      opts = Keyword.put(@opts, :offset_reset_policy, :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :offset_reset_policy to be one of [:earliest, :latest], got: :an_atom"}

      opts = Keyword.put(@opts, :offset_reset_policy, :earliest)
      assert {:ok, %{offset_reset_policy: :earliest}} = BrodClient.init(opts)

      opts = Keyword.put(@opts, :offset_reset_policy, :latest)
      assert {:ok, %{offset_reset_policy: :latest}} = BrodClient.init(opts)
    end

    test ":begin_offset can be :assigned or :reset. Default is :assigned" do
      assert {:ok, %{begin_offset: :assigned}} = BrodClient.init(@opts)

      opts = Keyword.put(@opts, :begin_offset, :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :begin_offset to be one of [:assigned, :reset], got: :an_atom"}

      opts = Keyword.put(@opts, :begin_offset, :assigned)
      assert {:ok, %{begin_offset: :assigned}} = BrodClient.init(opts)

      opts = Keyword.put(@opts, :begin_offset, :reset)
      assert {:ok, %{begin_offset: :reset}} = BrodClient.init(opts)
    end

    test ":offset_commit_interval_seconds is an optional non-negative integer" do
      opts = put_in(@opts, [:group_config, :offset_commit_interval_seconds], :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :offset_commit_interval_seconds to be " <>
                  "a positive integer, got: :an_atom"}

      opts = put_in(@opts, [:group_config, :offset_commit_interval_seconds], 3)
      {:ok, %{group_config: group_config}} = BrodClient.init(opts)
      assert group_config[:offset_commit_interval_seconds] == 3
    end

    test ":rejoin_delay_seconds is an optional non-negative integer" do
      opts = put_in(@opts, [:group_config, :rejoin_delay_seconds], :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :rejoin_delay_seconds to be a non-negative integer, got: :an_atom"}

      opts = put_in(@opts, [:group_config, :rejoin_delay_seconds], 3)
      {:ok, %{group_config: group_config}} = BrodClient.init(opts)
      assert group_config[:rejoin_delay_seconds] == 3
    end

    test ":session_timeout_seconds is an optional positive integer" do
      opts = put_in(@opts, [:group_config, :session_timeout_seconds], :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :session_timeout_seconds to be a positive integer, got: :an_atom"}

      opts = put_in(@opts, [:group_config, :session_timeout_seconds], 3)
      {:ok, %{group_config: group_config}} = BrodClient.init(opts)
      assert group_config[:session_timeout_seconds] == 3
    end

    test ":heartbeat_rate_seconds is an optional positive integer" do
      opts = put_in(@opts, [:group_config, :heartbeat_rate_seconds], :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :heartbeat_rate_seconds to be a positive integer, got: :an_atom"}

      opts = put_in(@opts, [:group_config, :heartbeat_rate_seconds], 3)
      {:ok, %{group_config: group_config}} = BrodClient.init(opts)
      assert group_config[:heartbeat_rate_seconds] == 3
    end

    test ":rebalance_timeout_seconds is an optional positive integer" do
      opts = put_in(@opts, [:group_config, :rebalance_timeout_seconds], :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :rebalance_timeout_seconds to be a positive integer, got: :an_atom"}

      opts = put_in(@opts, [:group_config, :rebalance_timeout_seconds], 3)
      {:ok, %{group_config: group_config}} = BrodClient.init(opts)
      assert group_config[:rebalance_timeout_seconds] == 3
    end

    test ":min_bytes is an optional positive integer" do
      opts = put_in(@opts, [:fetch_config, :min_bytes], :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :min_bytes to be a positive integer, got: :an_atom"}

      opts = put_in(@opts, [:fetch_config, :min_bytes], 3)
      {:ok, %{fetch_config: fetch_config}} = BrodClient.init(opts)
      assert fetch_config[:min_bytes] == 3
    end

    test ":max_bytes is optional non-negative integer" do
      opts = put_in(@opts, [:fetch_config, :max_bytes], :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :max_bytes to be a positive integer, got: :an_atom"}

      opts = put_in(@opts, [:fetch_config, :max_bytes], 3)
      {:ok, %{fetch_config: fetch_config}} = BrodClient.init(opts)
      assert fetch_config[:max_bytes] == 3
    end

    test ":max_wait_time is optional non-negative integer" do
      opts = put_in(@opts, [:fetch_config, :max_wait_time], :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :max_wait_time to be a positive integer, got: :an_atom"}

      {:ok, %{fetch_config: fetch_config}} = BrodClient.init(@opts)
      assert not Map.has_key?(fetch_config, :max_wait_time)

      opts = put_in(@opts, [:fetch_config, :max_wait_time], 3)
      {:ok, %{fetch_config: fetch_config}} = BrodClient.init(opts)
      assert fetch_config[:max_wait_time] == 3
    end

    test ":client_id_prefix is an optional atom value" do
      opts = put_in(@opts, [:client_config, :client_id_prefix], :wrong_type)

      assert BrodClient.init(opts) ==
               {:error, "expected :client_id_prefix to be a string, got: :wrong_type"}

      opts = put_in(@opts, [:client_config, :client_id_prefix], "a string")

      assert {:ok,
              %{
                client_config: [
                  client_id_prefix: "a string"
                ]
              }} = BrodClient.init(opts)
    end

    test ":sasl is an optional tuple of SASL mechanism, username and password" do
      opts = put_in(@opts, [:client_config, :sasl], :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :sasl to be a tuple of SASL mechanism, username and password, got: :an_atom"}

      opts = put_in(@opts, [:client_config, :sasl], {:an_atom, "username", "password"})

      assert BrodClient.init(opts) ==
               {:error,
                "expected :sasl to be a tuple of SASL mechanism, username and password, got: {:an_atom, \"username\", \"password\"}"}

      opts = put_in(@opts, [:client_config, :sasl], {:plain, "username", "password"})

      assert {:ok,
              %{
                client_config: [
                  sasl: {:plain, "username", "password"}
                ]
              }} = BrodClient.init(opts)
    end

    test ":sasl is an optional tuple of :callback, SASL Authentication Plugin module and opts" do
      opts = put_in(@opts, [:client_config, :sasl], {:callback, FakeSaslMechanismPlugin, {}})

      assert {:ok,
              %{
                client_config: [
                  sasl: {:callback, FakeSaslMechanismPlugin, {}}
                ]
              }} = BrodClient.init(opts)
    end

    test ":ssl is an optional boolean or keyword list" do
      opts = put_in(@opts, [:client_config, :ssl], :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :ssl to be a keyword list of SSL/TLS client options, got: :an_atom"}

      opts =
        put_in(@opts, [:client_config, :ssl],
          cacertfile: "ca.crt",
          keyfile: "client.key",
          certfile: "client.crt"
        )

      assert {:ok,
              %{
                client_config: [
                  ssl: [cacertfile: "ca.crt", keyfile: "client.key", certfile: "client.crt"]
                ]
              }} = BrodClient.init(opts)

      opts = put_in(@opts, [:client_config, :ssl], true)

      assert {:ok,
              %{
                client_config: [ssl: true]
              }} = BrodClient.init(opts)
    end

    test ":connect_timeout is an optional positive integer" do
      opts = put_in(@opts, [:client_config, :connect_timeout], "5000")

      assert BrodClient.init(opts) ==
               {:error, "expected :connect_timeout to be a positive integer, got: \"5000\""}

      opts = put_in(@opts, [:client_config, :connect_timeout], 5000)

      assert {:ok,
              %{
                client_config: [
                  connect_timeout: 5000
                ]
              }} = BrodClient.init(opts)
    end

    test ":request_timeout is an optional positive integer >= 1000" do
      opts = put_in(@opts, [:client_config, :request_timeout], "5000")

      assert BrodClient.init(opts) ==
               {:error,
                "expected :request_timeout to be a positive integer >= 1000, got: \"5000\""}

      opts = put_in(@opts, [:client_config, :request_timeout], 300)

      assert BrodClient.init(opts) ==
               {:error, "expected :request_timeout to be a positive integer >= 1000, got: 300"}

      opts = put_in(@opts, [:client_config, :request_timeout], 5000)

      assert {:ok,
              %{
                client_config: [
                  request_timeout: 5000
                ]
              }} = BrodClient.init(opts)
    end

    test ":query_api_versions is an optional boolean" do
      opts = put_in(@opts, [:client_config, :query_api_versions], "true")

      assert BrodClient.init(opts) ==
               {:error, "expected :query_api_versions to be a boolean, got: \"true\""}

      opts = put_in(@opts, [:client_config, :query_api_versions], false)

      assert {:ok, %{client_config: [query_api_versions: false]}} = BrodClient.init(opts)
    end
  end

  defmodule FakeSaslMechanismPlugin do
    @behaviour :kpro_auth_backend

    @impl true
    def auth(_host, _sock, _mod, _client_id, _timeout, _sasl_opts = {}) do
      :ok
    end
  end
end
