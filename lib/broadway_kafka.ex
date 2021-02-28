defmodule BroadwayKafka do
  @moduledoc """
  Helpers for BroadwayKafka

  For the producer, view `BroadwayKafka.Producer`
  """

  @doc """
  Changes topics from a running BroadwayKafka instance

  Updates topics in all producers, one after another

  ## Examples

      BroadwayKafka.update_topics(MyBroadway, ["topic_a", "topic_b"])
      :ok

      BroadwayKafka.update_topics(MyBroadway, [])
      :ok
  """
  def update_topics(name, topics) do
    each_producer(name, &GenServer.cast(&1, {:update_topics, topics}))
  end

  defp each_producer(server, fun) when is_function(fun, 1) do
    server
    |> Broadway.Server.producer_names()
    |> Enum.each(fun)
  end
end
