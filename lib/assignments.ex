defmodule BroadwayKafka.Assignments do
  def new do
    %{
      map: %{},
      keys: []
    }
  end

  def add(state, []) do
    state
  end

  def add(state, [item | rest]) do
    new_state =
      %{state |
        map: Map.put(state.map, item.key, item),
        keys: state.keys ++ [item.key]
      }

    add(new_state, rest)
  end

  def next(%{keys: [key | other_keys]} = state) do
    {state.map[key], %{state | keys: other_keys ++ [key]}}
  end

  def update_last_acked_offset(state, key, value) do
    new_map = Map.update(state.map, key, nil, &Map.put(&1, :last_acked_offset, value))
    %{state | map: new_map}
  end

  def update_offset(state, key, value) do
    new_map = Map.update(state.map, key, nil, &Map.put(&1, :offset, value))
    %{state | map: new_map}
  end

  def drained?(state) do
    Enum.all?(state.map, fn {_k, v} ->
      !v.last_acked_offset || (v.offset == v.last_acked_offset + 1)
    end)
  end
end
