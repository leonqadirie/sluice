//// A FIFO event buffer with a size limit and an overflow policy.

import gleam/bool
import gleam/deque.{type Deque}
import gleam/list
import sluice.{type Keep, KeepFirst, KeepLast}

pub type Capacity {
  Bounded(Int)
  Unbounded
}

pub opaque type Buffer(event) {
  Buffer(deque: Deque(event), size: Int, capacity: Capacity, keep: Keep)
}

pub fn new(capacity capacity: Capacity, keep keep: Keep) -> Buffer(event) {
  Buffer(deque: deque.new(), size: 0, capacity:, keep:)
}

pub fn size(buffer: Buffer(event)) -> Int {
  buffer.size
}

pub fn is_empty(buffer: Buffer(event)) -> Bool {
  buffer.size == 0
}

/// Add events to the back of the buffer. Apply the overflow policy.
/// Return the new buffer and the quantity of discarded events.
pub fn store(
  onto buffer: Buffer(event),
  events events: List(event),
) -> #(Buffer(event), Int) {
  case buffer.capacity {
    Unbounded -> #(push_all(buffer, events), 0)
    Bounded(capacity) ->
      case buffer.keep {
        // Keep the oldest events: accept only the quantity that is
        // possible. Discard the remaining part of the new batch.
        KeepFirst -> {
          let room = capacity - buffer.size
          let #(accepted, discarded) = case room <= 0 {
            True -> #([], events)
            False -> list.split(events, room)
          }
          #(push_all(buffer, accepted), list.length(discarded))
        }
        // Keep the newest events: accept all events. Then discard events
        // from the front until the content is not more than the capacity.
        KeepLast -> {
          let buffer = push_all(buffer, events)
          drop_front_until(from: buffer, capacity: capacity, dropped: 0)
        }
      }
  }
}

/// Put events at the front of the buffer. Their sequence stays, before
/// all buffered events. Use this for events that the buffer accepted
/// before. Thus this function does not discard events, also when the
/// content is more than the capacity.
pub fn store_front(
  onto buffer: Buffer(event),
  events events: List(event),
) -> Buffer(event) {
  list.fold(list.reverse(events), buffer, fn(buffer, event) {
    Buffer(
      ..buffer,
      deque: deque.push_front(buffer.deque, event),
      size: buffer.size + 1,
    )
  })
}

/// Remove a maximum of `count` events from the front of the buffer, in
/// sequence.
pub fn take(
  from buffer: Buffer(event),
  count count: Int,
) -> #(List(event), Buffer(event)) {
  take_loop(from: buffer, count: count, taken: [])
}

fn take_loop(
  from buffer: Buffer(event),
  count count: Int,
  taken taken: List(event),
) -> #(List(event), Buffer(event)) {
  use <- bool.guard(when: count <= 0, return: #(list.reverse(taken), buffer))
  case deque.pop_front(buffer.deque) {
    Error(Nil) -> #(list.reverse(taken), buffer)
    Ok(#(event, deque)) ->
      take_loop(
        from: Buffer(..buffer, deque:, size: buffer.size - 1),
        count: count - 1,
        taken: [event, ..taken],
      )
  }
}

fn push_all(buffer: Buffer(event), events: List(event)) -> Buffer(event) {
  list.fold(events, buffer, fn(buffer, event) {
    Buffer(
      ..buffer,
      deque: deque.push_back(buffer.deque, event),
      size: buffer.size + 1,
    )
  })
}

fn drop_front_until(
  from buffer: Buffer(event),
  capacity capacity: Int,
  dropped dropped: Int,
) -> #(Buffer(event), Int) {
  use <- bool.guard(when: buffer.size <= capacity, return: #(buffer, dropped))
  case deque.pop_front(buffer.deque) {
    Error(Nil) -> #(buffer, dropped)
    Ok(#(_, deque)) ->
      drop_front_until(
        from: Buffer(..buffer, deque:, size: buffer.size - 1),
        capacity: capacity,
        dropped: dropped + 1,
      )
  }
}
