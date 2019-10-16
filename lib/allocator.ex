defmodule BroadwayKafka.Allocator do
  @moduledoc false

  # The allocator is responsible to allocate Kafka partitions
  # for each layer of stages in Broadway. We have an allocator
  # for each processor (vertical) and an allocator for each
  # batcher (horizontal).
  #
  # The allocator is stateful, as it must avoid rellocations,
  # as that would imply lost of ordering.

  use GenServer

  @doc """
  Starts a new allocator.
  """
  def start_link({name, producers, processors})
      when is_atom(name) and producers > 0 and processors > 0 do
    GenServer.start_link(__MODULE__, {name, producers, processors}, name: name)
  end

  @doc """
  Fetches the partition for a given `key`.
  """
  def fetch!(name, key) when is_atom(name), do: :ets.lookup_element(name, key, 2)

  @doc """
  Alocates the given `new_entries` for `producer`.
  """
  def allocate(name, producer, new_entries)
      when is_atom(name) and producer >= 0 and is_list(new_entries),
      do: GenServer.call(name, {:allocate, producer, new_entries}, :infinity)

  @doc """
  Returns the allocation map with partitions as keys and
  a list of allocated keys as values. Used for testing.
  """
  def to_map(name), do: GenServer.call(name, :to_map, :infinity)

  @impl true
  def init({name, producers, processors}) when producers > 0 and processors > 0 do
    partitions = for i <- 0..(processors - 1), into: %{}, do: {i, %{}}
    producers = for i <- 0..(producers - 1), into: %{}, do: {i, %{}}
    keys = :ets.new(name, [:named_table, :set, :protected, read_concurrency: true])
    old_keys = %{}
    {:ok, {producers, producers, partitions, keys, old_keys}}
  end

  @impl true
  def handle_call(:to_map, _from, {_, _, partitions, _, _} = state) do
    map =
      for {partition, map} <- partitions, into: %{} do
        {partition, Map.keys(map)}
      end

    {:reply, map, state}
  end

  @impl true
  def handle_call(
        {:allocate, producer, new_entries},
        _from,
        {producers, old_producers, partitions, keys, old_keys}
      ) do
    # Update the producer allocation
    old_entries = Map.fetch!(old_producers, producer)
    current_entries = Map.fetch!(producers, producer)
    old_producers = Map.put(producers, producer, current_entries)
    producers = Map.put(producers, producer, new_entries)

    # We remove the current entries that are no longer used
    to_remove_entries = Enum.reject(current_entries, &in_a_producer?(producers, &1))
    partitions = remove_unused(to_remove_entries, partitions, keys)

    # We remove the old entries that are no longer used
    fun = &(in_a_producer?(producers, &1) or in_a_producer?(old_producers, &1))
    old_keys = Map.drop(old_keys, Enum.reject(old_entries, fun))

    # Now we reject anything that is currently assigned and
    # split into unseen and seen entries.
    {seen_entries, unseen_entries} =
      new_entries
      |> Enum.reject(&:ets.member(keys, &1))
      |> Enum.split_with(&Map.has_key?(old_keys, &1))

    # We first assign the ones that we know the location
    # so we can do a better distribution for the unseen ones
    partitions = assign_seen_entries(seen_entries, partitions, old_keys, keys)

    # Now we assign unseen ones
    {_, partitions, old_keys} = assign_unseen_entries(unseen_entries, partitions, old_keys, keys)

    {:reply, :ok, {producers, old_producers, partitions, keys, old_keys}}
  end

  defp in_a_producer?(producers, entry) do
    Enum.any?(producers, fn {_, entries} -> entry in entries end)
  end

  defp remove_unused(to_remove, partitions, keys) do
    Enum.reduce(to_remove, partitions, fn entry, partitions ->
      [{^entry, partition}] = :ets.take(keys, entry)
      {true, partitions} = pop_in(partitions[partition][entry])
      partitions
    end)
  end

  defp assign_seen_entries(seen_entries, partitions, old_keys, keys) do
    Enum.reduce(seen_entries, partitions, fn entry, partitions ->
      partition = Map.fetch!(old_keys, entry)
      :ets.insert(keys, {entry, partition})
      put_in(partitions[partition][entry], true)
    end)
  end

  defp assign_unseen_entries(unseen_entries, partitions, old_keys, keys) do
    # Get the partitions with fewer allocations first and assign from there
    sorted = sort_partitions_by_allocation(partitions)

    Enum.reduce(unseen_entries, {sorted, partitions, old_keys}, fn
      entry, {[{size, partition} | sorted], partitions, old_keys} ->
        sorted = add_to_sorted({size + 1, partition}, sorted)
        partitions = put_in(partitions[partition][entry], true)
        old_keys = put_in(old_keys[entry], partition)
        :ets.insert(keys, {entry, partition})
        {sorted, partitions, old_keys}
    end)
  end

  defp sort_partitions_by_allocation(partitions) do
    partitions
    |> Enum.map(fn {partition, map} -> {map_size(map), partition} end)
    |> Enum.sort()
  end

  defp add_to_sorted({size, partition}, [{next_size, _} | _] = rest) when size <= next_size,
    do: [{size, partition} | rest]

  defp add_to_sorted(tuple, [pair | rest]),
    do: [pair | add_to_sorted(tuple, rest)]

  defp add_to_sorted(tuple, []),
    do: [tuple]
end
