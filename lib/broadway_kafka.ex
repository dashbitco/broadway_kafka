defmodule BroadwayKafka do
  @moduledoc """
  API for BroadwayKafka
  """

  @doc """
  Changes topics from a running BroadwayKafka instance
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
