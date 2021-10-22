defmodule BroadwayKafka.ProducerEventListener do

  @doc """
  Custom function to be invoked when Broadway processed new partitions assignments
  """
  @callback on_partitions_assign(topic_partitions :: any) :: :ok | {:error, String.t()}

  @doc """
  Custom function to be invoked right before Broadway starts draining in-flight
  messages during partitions revocation or shutdown
  """
  @callback on_partitions_revoke(topic_partitions :: any) :: :ok | {:error, String.t()}

end
