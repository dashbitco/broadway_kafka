defmodule BroadwayKafka.AcknowledgerTest do
  use ExUnit.Case, async: true

  alias BroadwayKafka.Acknowledger, as: Ack

  @foo {1, "foo", 1}
  @bar {1, "bar", 2}
  @ack Ack.add(Ack.new(), [{1, "foo", 1, 10}, {1, "bar", 2, 0}])

  test "new" do
    assert Ack.new() == %{}
  end

  test "add" do
    assert @ack == %{
             @foo => {[], 10, []},
             @bar => {[], 0, []}
           }
  end

  test "keys" do
    assert Ack.keys(@ack) |> Enum.sort() == [@bar, @foo]
  end

  test "last_offset" do
    assert Ack.last_offset(@ack, @foo) == 10
    assert Ack.last_offset(@ack, @bar) == 0
  end

  test "update_current_offset" do
    ack = Ack.update_last_offset(@ack, @foo, 20, Enum.to_list(10..19))
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, Enum.to_list(9..19))

    ack = Ack.update_last_offset(@ack, @foo, 20, Enum.to_list(10..19))
    assert {false, 10, ack} = Ack.update_current_offset(ack, @foo, [10, 13, 14])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [11, 12, 15, 16, 17, 18, 19])

    ack = Ack.update_last_offset(@ack, @foo, 20, Enum.to_list(10..19))
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [13, 14])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [11, 12, 15, 16, 17, 18, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [10])

    ack = Ack.update_last_offset(@ack, @foo, 20, Enum.to_list(10..19))
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [13, 14])
    assert {false, 16, ack} = Ack.update_current_offset(ack, @foo, [10, 11, 12, 15, 16, 18, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [17])
  end

  test "update_current_offset with gaps" do
    ack = Ack.update_last_offset(@ack, @foo, 20, [11, 13, 15, 17, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [9, 11, 13, 15, 17, 19])

    ack = Ack.update_last_offset(@ack, @foo, 20, [11, 13, 15, 17, 19])
    assert {false, 12, ack} = Ack.update_current_offset(ack, @foo, [11, 15])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [13, 17, 19])

    ack = Ack.update_last_offset(@ack, @foo, 20, [11, 13, 15, 17, 19])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [13])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [15, 17, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [11])

    ack = Ack.update_last_offset(@ack, @foo, 20, [11, 13, 15, 17, 19])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [13])
    assert {false, 16, ack} = Ack.update_current_offset(ack, @foo, [11, 15, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [17])
  end

  test "all_drained?" do
    ack = @ack
    assert Ack.all_drained?(ack)

    ack = Ack.update_last_offset(ack, @foo, 100, Enum.to_list(10..99))
    refute Ack.all_drained?(ack)

    assert {false, 49, ack} = Ack.update_current_offset(ack, @foo, Enum.to_list(10..49))
    refute Ack.all_drained?(ack)

    assert {true, 99, ack} = Ack.update_current_offset(ack, @foo, Enum.to_list(50..99))
    assert Ack.all_drained?(ack)
  end

  # Some poor man's property based testing.
  describe "property based testing" do
    # We generate a list from 10..99 and we break it into 1..9 random parts.
    test "drained?" do
      ack = Ack.update_last_offset(@ack, @foo, 100, Enum.to_list(10..99))

      for n_parts <- 1..9 do
        groups = Enum.group_by(10..99, fn _ -> :rand.uniform(n_parts) end)
        offsets = Map.values(groups)

        {drained?, _, ack} =
          Enum.reduce(offsets, {false, :unused, ack}, fn offset, {false, _, ack} ->
            Ack.update_current_offset(ack, @foo, Enum.sort(offset))
          end)

        assert drained?
        assert Ack.all_drained?(ack)
      end
    end
  end
end
