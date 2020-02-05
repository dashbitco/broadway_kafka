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
    test ":hosts is a required keyword list" do
      opts = Keyword.delete(@opts, :hosts)
      assert BrodClient.init(opts) == {:error, ":hosts is required"}

      opts = Keyword.put(@opts, :hosts, :an_atom)

      assert BrodClient.init(opts) ==
               {:error, "expected :hosts to be a keyword list of host/port pairs, got: :an_atom"}

      opts = Keyword.put(@opts, :hosts, host: 9092)
      assert {:ok, %{hosts: [host: 9092]}} = BrodClient.init(opts)
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

    test ":topics is a required list of strings or keyword list" do
      opts = Keyword.delete(@opts, :topics)
      assert BrodClient.init(opts) == {:error, ":topics is required"}

      opts = Keyword.put(@opts, :topics, :an_atom)

      assert BrodClient.init(opts) ==
               {:error,
                "expected :topics to be a list of strings or a keyword " <>
                  "list of host/port pairs, got: :an_atom"}

      opts = Keyword.put(@opts, :topics, ["topic_1", "topic_2"])
      assert {:ok, %{topics: ["topic_1", "topic_2"]}} = BrodClient.init(opts)

      opts = Keyword.put(@opts, :topics, topic_1: 1, topic_2: 2)
      assert {:ok, %{topics: [topic_1: 1, topic_2: 2]}} = BrodClient.init(opts)
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

    test ":ssl is an optional keyword list" do
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
    end
  end
end
