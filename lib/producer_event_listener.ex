defmodule BroadwayKafka.ProducerEventListener do
  @doc """
  Custom function to be invoked right before Broadway starts draining in-flight
  messages during shutdown.
  """
  @callback on_drain(state :: any) :: :ok | {:error, String.t()}
end
