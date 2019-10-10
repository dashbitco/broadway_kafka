defmodule BroadwayKafka.AllocatorTest do
  use ExUnit.Case, async: true

  import BroadwayKafka.Allocator

  test "evenly allocates partitions at once" do
    start_link({:a1, 1, 4})
    allocate(:a1, 0, [:a, :b])
    assert verify!(:a1) == %{0 => [:a], 1 => [:b], 2 => [], 3 => []}

    start_link({:a2, 1, 4})
    allocate(:a2, 0, [:a, :b, :c, :d])
    assert verify!(:a2) == %{0 => [:a], 1 => [:b], 2 => [:c], 3 => [:d]}

    start_link({:a3, 1, 4})
    allocate(:a3, 0, [:a, :b, :c, :d, :e, :f])
    assert verify!(:a3) == %{0 => [:a], 1 => [:b], 2 => [:c, :f], 3 => [:d, :e]}

    start_link({:a4, 1, 4})
    allocate(:a4, 0, [:a, :b, :c, :d, :e, :f, :g, :h])
    assert verify!(:a4) == %{0 => [:a, :h], 1 => [:b, :g], 2 => [:c, :f], 3 => [:d, :e]}

    start_link({:a5, 1, 4})
    allocate(:a5, 0, [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j])
    assert verify!(:a5) == %{0 => [:a, :h, :i], 1 => [:b, :g, :j], 2 => [:c, :f], 3 => [:d, :e]}
  end

  test "re-allocates partitions keeping current allocation", %{test: name} do
    start_link({name, 1, 4})
    allocate(name, 0, [:a, :b, :c, :d])
    assert verify!(name) == %{0 => [:a], 1 => [:b], 2 => [:c], 3 => [:d]}

    allocate(name, 0, [:c, :d, :e, :f])
    assert verify!(name) == %{0 => [:e], 1 => [:f], 2 => [:c], 3 => [:d]}
  end

  test "re-allocates partitions keeping last generation allocation", %{test: name} do
    start_link({name, 1, 4})
    allocate(name, 0, [:a, :b, :c, :d])
    allocate(name, 0, [:e, :f, :g, :h])
    allocate(name, 0, [:c, :b, :d, :a, :f, :g, :h, :e])
    assert verify!(name) == %{0 => [:a, :e], 1 => [:b, :f], 2 => [:c, :g], 3 => [:d, :h]}
  end

  test "does not remove entries when they are reallocated" do
    start_link({:b1, 2, 4})
    allocate(:b1, 0, [:a, :b, :c, :d])
    allocate(:b1, 0, [:e, :f, :g, :h])
    allocate(:b1, 1, [:a, :b, :c, :d])
    assert verify!(:b1) == %{0 => [:a, :e], 1 => [:b, :f], 2 => [:c, :g], 3 => [:d, :h]}

    start_link({:b2, 2, 4})
    allocate(:b2, 0, [:a, :b, :c, :d])
    allocate(:b2, 1, [:a, :b, :c, :d])
    allocate(:b2, 0, [:e, :f, :g, :h])
    assert verify!(:b2) == %{0 => [:a, :e], 1 => [:b, :f], 2 => [:c, :g], 3 => [:d, :h]}
  end

  test "rebalances highly skewed partitions considering previous allocations", %{test: name} do
    start_link({name, 1, 4})
    allocate(name, 0, [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j])
    allocate(name, 0, [:a, :c, :f, :h, :i, :j, :k, :l, :m, :n])
    assert verify!(name) == %{0 => [:a, :h, :i], 1 => [:j, :m, :n], 2 => [:c, :f], 3 => [:k, :l]}
  end

  defp verify!(name) do
    map = to_map(name)

    for {partition, entries} <- map,
        entry <- entries,
        do: ^partition = fetch!(name, entry)

    map
  end
end
