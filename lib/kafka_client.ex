defmodule BroadwayKafka.KafkaClient do
  @moduledoc false

  @typep config :: %{
           hosts: any,
           client_id: any,
           group_id: any,
           topics: any,
           group_config: keyword,
           client_config: keyword
         }

  @callback init(opts :: any) :: {:ok, config} | {:error, any}
  @callback setup(stage_pid :: pid, client_id :: atom, callback_module :: module, config) ::
              {:ok, group_coordinator :: pid} | {:error, any}
  @callback ack(
              group_coordinator :: pid,
              generation_id :: integer,
              topic :: binary,
              partition :: integer,
              offset :: integer
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
end
