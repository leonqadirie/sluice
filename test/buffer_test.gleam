import gleam/int
import gleam/list
import sluice.{KeepFirst, KeepLast}
import sluice/internal/buffer.{Bounded, Unbounded}

pub fn store_and_take_in_order_test() {
  let #(event_buffer, discarded) =
    buffer.new(capacity: Unbounded, keep: KeepLast)
    |> buffer.store([1, 2, 3, 4, 5])
  assert discarded == 0
  assert buffer.size(event_buffer) == 5

  let #(taken, event_buffer) = buffer.take(event_buffer, 3)
  assert taken == [1, 2, 3]
  let #(taken, event_buffer) = buffer.take(event_buffer, 10)
  assert taken == [4, 5]
  assert buffer.is_empty(event_buffer)
}

pub fn take_from_empty_test() {
  let #(taken, _) = buffer.take(buffer.new(Unbounded, KeepLast), 3)
  assert taken == []
}

pub fn keep_first_drops_incoming_test() {
  let #(event_buffer, discarded) =
    buffer.new(capacity: Bounded(3), keep: KeepFirst)
    |> buffer.store([1, 2, 3, 4, 5])
  assert discarded == 2
  let #(taken, _) = buffer.take(event_buffer, 10)
  assert taken == [1, 2, 3]
}

pub fn keep_last_drops_oldest_test() {
  let #(event_buffer, discarded) =
    buffer.new(capacity: Bounded(3), keep: KeepLast)
    |> buffer.store([1, 2, 3, 4, 5])
  assert discarded == 2
  let #(taken, _) = buffer.take(event_buffer, 10)
  assert taken == [3, 4, 5]
}

pub fn keep_first_full_buffer_discards_everything_test() {
  let #(event_buffer, _) =
    buffer.new(capacity: Bounded(2), keep: KeepFirst)
    |> buffer.store([1, 2])
  let #(event_buffer, discarded) = buffer.store(event_buffer, [3, 4])
  assert discarded == 2
  let #(taken, _) = buffer.take(event_buffer, 10)
  assert taken == [1, 2]
}

pub fn keep_last_overflow_across_stores_test() {
  let #(event_buffer, _) =
    buffer.new(capacity: Bounded(2), keep: KeepLast)
    |> buffer.store([1, 2])
  let #(event_buffer, discarded) = buffer.store(event_buffer, [3])
  assert discarded == 1
  let #(taken, _) = buffer.take(event_buffer, 10)
  assert taken == [2, 3]
}

pub fn store_front_preserves_order_and_never_drops_test() {
  let #(event_buffer, _) =
    buffer.new(capacity: Bounded(3), keep: KeepFirst)
    |> buffer.store([10, 11, 12])
  let event_buffer = buffer.store_front(event_buffer, [1, 2])
  assert buffer.size(event_buffer) == 5
  let #(taken, _) = buffer.take(event_buffer, 10)
  assert taken == [1, 2, 10, 11, 12]
}

pub fn unbounded_never_discards_test() {
  let events =
    int.range(from: 1, to: 100_001, with: [], run: list.prepend)
    |> list.reverse
  let #(event_buffer, discarded) =
    buffer.new(capacity: Unbounded, keep: KeepLast) |> buffer.store(events)
  assert discarded == 0
  assert buffer.size(event_buffer) == 100_000
}
