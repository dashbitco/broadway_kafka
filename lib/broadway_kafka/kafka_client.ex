defmodule BroadwayKafka.KafkaClient do
  @moduledoc false

  @typep config :: %{
           hosts: [:brod.endpoint()],
           client_id: :brod.client(),
           group_id: :brod.group_id(),
           reconnect_timeout: non_neg_integer,
           offset_commit_on_ack: boolean,
           topics: [:brod.topic()],
           group_config: keyword,
           client_config: keyword
         }

  @typep offset_reset_policy :: :earliest | :latest

  @callback init(opts :: any) :: {:ok, config} | {:error, any}
  @callback setup(
              stage_pid :: pid,
              client_id :: :brod.client(),
              callback_module :: module,
              config
            ) ::
              {:ok, group_coordinator :: pid} | {:error, any}
  @callback ack(
              group_coordinator :: pid,
              generation_id :: integer,
              topic :: binary,
              partition :: integer,
              offset :: integer,
              config
            ) :: :ok
  @callback fetch(
              client_id :: atom,
              topic :: binary,
              partition :: integer,
              offset :: integer,
              opts :: any,
              config :: any
            ) ::
              {:ok, {offset :: integer, [:brod.message()]}} | {:error, any()}

  @callback resolve_offset(
              topic :: binary,
              partition :: integer,
              offset :: integer,
              offset_reset_policy :: offset_reset_policy(),
              config
            ) ::
              offset :: integer | no_return()

  @callback update_topics(:brod.group_coordinator(), [:brod.topic()]) :: :ok
  @callback connected?(:brod.client()) :: boolean
  @callback disconnect(:brod.client()) :: :ok
end
