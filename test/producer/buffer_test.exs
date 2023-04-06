defmodule BroadwayKafka.Producer.BufferTest do
  use ExUnit.Case

  alias BroadwayKafka.Producer.Buffer

  describe "new/0" do
    test "returns empty :queue" do
      assert Buffer.new() == :queue.new()
    end
  end

  describe "enqueue_with_key/3" do
    test "enqueues an empty list" do
      buffer = Buffer.new()

      key = {0, "topic", 1}
      new_buffer = Buffer.enqueue_with_key(buffer, key, [])

      assert new_buffer == Buffer.new()
    end

    test "enqueues a list with elements" do
      buffer = Buffer.new()

      key = {0, "topic", 1}
      messages = [%Broadway.Message{data: nil, acknowledger: nil}]

      new_buffer = Buffer.enqueue_with_key(buffer, key, messages)

      refute Buffer.empty?(new_buffer)
    end

    test "enqueues a list with its key" do
      buffer = Buffer.new()

      key = {0, "topic", 1}
      messages = [%Broadway.Message{data: nil, acknowledger: nil}]

      new_buffer = Buffer.enqueue_with_key(buffer, key, messages)

      assert {:value, {^key, ^messages}} = :queue.out(new_buffer) |> elem(0)
    end
  end
end
