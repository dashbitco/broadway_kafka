defmodule BroadwayKafka do
  @moduledoc """
  Helpers for BroadwayKafka.

  You can find the Broadway producer in `BroadwayKafka.Producer`.
  """

  @doc """
  Sequentially updates topics in all Broadway producers in the
  pipeline given by `name`.

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
    |> Broadway.producer_names()
    |> Enum.each(fun)
  end
end
