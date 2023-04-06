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

  describe "dequeue/2" do
    setup do
      key01 = {1239, "topic", 1}

      message_batch01 =
        for i <- 1..3, do: %Broadway.Message{data: nil, acknowledger: nil, metadata: %{offset: i}}

      key02 = {1239, "topic", 2}

      message_batch02 =
        for i <- 11..13,
            do: %Broadway.Message{data: nil, acknowledger: nil, metadata: %{offset: i}}

      buffer =
        Buffer.new()
        |> Buffer.enqueue_with_key(key01, message_batch01)
        |> Buffer.enqueue_with_key(key02, message_batch02)

      {:ok, buffer: buffer}
    end

    test "dequeues elements based on requested number", %{buffer: buffer} do
      {new_buffer, dequeued_items, dequeued_count} = Buffer.dequeue(buffer, 2)

      assert dequeued_count == 2

      assert [
               {{1239, "topic", 1},
                [
                  %Broadway.Message{metadata: %{offset: 1}},
                  %Broadway.Message{metadata: %{offset: 2}}
                ], %Broadway.Message{metadata: %{offset: 2}}}
             ] = dequeued_items

      refute new_buffer == buffer
    end

    test "after dequeues elements, on the next run should dequeues from another key", %{
      buffer: buffer
    } do
      {new_buffer, dequeued_items, _dequeued_count} = Buffer.dequeue(buffer, 1)
      popped_key01 = List.first(dequeued_items) |> elem(0)

      {_new_buffer, dequeued_items, _dequeued_count} = Buffer.dequeue(new_buffer, 1)
      popped_key02 = List.first(dequeued_items) |> elem(0)

      refute popped_key01 == popped_key02
    end

    test "dequeues all elements if count is greater than the number of elements", %{
      buffer: buffer
    } do
      {new_buffer, _dequeued_items, dequeued_count} = Buffer.dequeue(buffer, 10)
      assert dequeued_count == 6

      assert Buffer.empty?(new_buffer)
    end

    test "after dequeues all elements for a given key, should remove the key from the queue", %{
      buffer: buffer
    } do
      assert {new_buffer, dequeued_items, 3} = Buffer.dequeue(buffer, 3)
      assert length(dequeued_items) == 1
      assert {{:value, {_, _}}, new_buffer} = :queue.out(new_buffer)
      assert Buffer.empty?(new_buffer)
    end
  end
end
