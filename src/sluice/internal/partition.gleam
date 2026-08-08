//// The partition dispatcher. A hash function gives each event a
//// partition, and each partition has a maximum of one subscriber. Events
//// for a partition without open demand wait in an internal queue for that
//// partition. Thus a slow partition does not stop the other partitions.
//// The queues have no limit. The demand connection to the subscribers
//// keeps them small.

import gleam/bool
import gleam/deque.{type Deque}
import gleam/dict.{type Dict}
import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{None, Some}
import gleam/result
import sluice/internal/dispatcher.{
  type Delivery, type Dispatcher, Delivery, DispatchResult, Dispatcher,
}
import sluice/internal/protocol.{type ConsumerMessage}

type Slot(event) {
  Slot(subscriber: Subject(ConsumerMessage(event)), demand: Int)
}

type State(event) {
  State(
    count: Int,
    by: fn(event) -> Int,
    slots: Dict(Int, Slot(event)),
    queues: Dict(Int, Deque(event)),
  )
}

pub fn new(count: Int, by: fn(event) -> Int) -> Dispatcher(event) {
  make(State(
    count: int.max(count, 1),
    by:,
    slots: dict.new(),
    queues: dict.new(),
  ))
}

fn make(state: State(event)) -> Dispatcher(event) {
  Dispatcher(
    subscribe: fn(from, metadata) {
      case metadata.partition {
        None ->
          Error(
            "a partition dispatcher needs a partition index on the subscription: set it with sluice.partition",
          )
        Some(index) -> accept(state:, from:, index:)
      }
    },
    ask: fn(demand, from) {
      case find_slot(state, from) {
        Error(Nil) -> #(0, [], make(state))
        Ok(#(index, slot)) -> {
          let slot = Slot(..slot, demand: slot.demand + demand)
          let #(deliveries, slot, state) =
            supply_from_queue(state: state, index: index, slot: slot)
          let state =
            State(..state, slots: dict.insert(state.slots, index, slot))
          #(demand, deliveries, make(state))
        }
      }
    },
    cancel: fn(from) {
      // The queue of the partition stays: the next subscriber of this
      // partition receives the waiting events.
      case find_slot(state, from) {
        Error(Nil) -> #(0, make(state))
        Ok(#(index, _slot)) -> #(
          0,
          make(State(..state, slots: dict.delete(state.slots, index))),
        )
      }
    },
    dispatch: fn(events, _incoming) {
      let #(state, delivered) =
        list.fold(events, #(state, dict.new()), route_event)
      let deliveries =
        dict.fold(delivered, [], fn(deliveries, index, backwards) {
          case dict.get(state.slots, index) {
            Error(Nil) -> deliveries
            Ok(slot) -> [
              Delivery(slot.subscriber, list.reverse(backwards)),
              ..deliveries
            ]
          }
        })
      DispatchResult(deliveries:, leftover: [], reask: [], next: make(state))
    },
    total_demand: fn() {
      dict.fold(state.slots, 0, fn(sum, _index, slot) { sum + slot.demand })
    },
  )
}

// nolint: stringly_typed_error -- the dispatcher contract carries refusal reasons as text
fn accept(
  state state: State(event),
  from from: Subject(ConsumerMessage(event)),
  index index: Int,
) -> Result(Dispatcher(event), String) {
  use <- bool.guard(
    when: index < 0 || index >= state.count,
    return: Error(
      "the partition index must be at least 0 and less than "
      <> int.to_string(state.count),
    ),
  )
  use <- bool.guard(
    when: dict.has_key(state.slots, index),
    return: Error(
      "partition " <> int.to_string(index) <> " already has a subscriber",
    ),
  )
  let slots = dict.insert(state.slots, index, Slot(from, 0))
  Ok(make(State(..state, slots:)))
}

fn find_slot(
  state: State(event),
  from: Subject(ConsumerMessage(event)),
) -> Result(#(Int, Slot(event)), Nil) {
  dict.fold(state.slots, Error(Nil), fn(found, index, slot) {
    case slot.subscriber == from {
      True -> Ok(#(index, slot))
      False -> found
    }
  })
}

// New demand first takes the events that wait in the queue of the
// partition, in sequence.
fn supply_from_queue(
  state state: State(event),
  index index: Int,
  slot slot: Slot(event),
) -> #(List(Delivery(event)), Slot(event), State(event)) {
  let queue = dict.get(state.queues, index) |> result.unwrap(deque.new())
  let #(taken, queue) =
    take_from_queue(queue: queue, count: slot.demand, taken: [])
  case taken {
    [] -> #([], slot, state)
    _ -> #(
      [Delivery(slot.subscriber, taken)],
      Slot(..slot, demand: slot.demand - list.length(taken)),
      State(..state, queues: dict.insert(state.queues, index, queue)),
    )
  }
}

fn take_from_queue(
  queue queue: Deque(event),
  count count: Int,
  taken taken: List(event),
) -> #(List(event), Deque(event)) {
  use <- bool.guard(when: count <= 0, return: #(list.reverse(taken), queue))
  case deque.pop_front(queue) {
    Error(Nil) -> #(list.reverse(taken), queue)
    Ok(#(event, queue)) ->
      take_from_queue(queue: queue, count: count - 1, taken: [event, ..taken])
  }
}

// One event goes to its partition: to the subscriber when the partition
// has open demand, to the queue of the partition when it does not.
fn route_event(
  accumulated: #(State(event), Dict(Int, List(event))),
  event: event,
) -> #(State(event), Dict(Int, List(event))) {
  let #(state, delivered) = accumulated
  let index = int.modulo(state.by(event), state.count) |> result.unwrap(0)
  let slot = dict.get(state.slots, index)
  case slot {
    Ok(Slot(_, demand) as slot) if demand > 0 -> {
      let slots =
        dict.insert(state.slots, index, Slot(..slot, demand: demand - 1))
      let backwards =
        dict.get(delivered, index) |> result.unwrap([]) |> list.prepend(event)
      #(State(..state, slots:), dict.insert(delivered, index, backwards))
    }
    _ -> {
      let queue =
        dict.get(state.queues, index)
        |> result.unwrap(deque.new())
        |> deque.push_back(event)
      #(
        State(..state, queues: dict.insert(state.queues, index, queue)),
        delivered,
      )
    }
  }
}
