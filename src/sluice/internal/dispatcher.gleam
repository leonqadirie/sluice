//// A dispatcher decides which subscriber receives which events. A
//// dispatcher is a record of closures. Each closure returns the subsequent
//// dispatcher. Thus the dispatcher state stays hidden. The stages do not
//// carry a type parameter for the dispatcher state.
////
//// `dispatch` is pure. It calculates the deliveries. It does not send
//// messages. The stage runtime sends the deliveries. The stage runtime
//// also puts the `leftover` events into the buffer.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import sluice/internal/platform
import sluice/internal/protocol.{type ConsumerMessage, type SubscribeMetadata}

pub type Delivery(event) {
  Delivery(to: Subject(ConsumerMessage(event)), events: List(event))
}

pub type DispatchResult(event) {
  DispatchResult(
    deliveries: List(Delivery(event)),
    leftover: List(event),
    next: Dispatcher(event),
  )
}

pub type Dispatcher(event) {
  Dispatcher(
    subscribe: fn(Subject(ConsumerMessage(event)), SubscribeMetadata(event)) ->
      Result(Dispatcher(event), String),
    /// Record the demand from a subscriber. Return the part of the demand
    /// that is new demand on the stage.
    ask: fn(Int, Subject(ConsumerMessage(event))) -> #(Int, Dispatcher(event)),
    /// Remove a subscriber. Return the demand that the stage removes.
    cancel: fn(Subject(ConsumerMessage(event))) -> #(Int, Dispatcher(event)),
    /// Give events to the subscribers that have recorded demand. The
    /// events that no subscriber has demand for come back as `leftover`.
    dispatch: fn(List(event), Int) -> DispatchResult(event),
    total_demand: fn() -> Int,
  )
}

/// The default dispatcher. Events go to the subscriber that has the
/// largest open demand. Thus, with time, the load becomes equal across the
/// subscribers.
///
/// When a subscriber cancels, its open demand stays as `pending`. The
/// `pending` value decreases the subsequent asks. Thus the stage does not
/// make more events than the quantity that all subscribers asked for.
pub fn demand() -> Dispatcher(event) {
  make_demand(DemandState(demands: [], pending: 0, max_seen: None))
}

// The demand dispatcher balances best when all subscribers use the same
// `max_demand`. Write a warning when the values differ.
fn observe_max_demand(
  max_seen: Option(Int),
  max_demand: Option(Int),
) -> Option(Int) {
  case max_seen, max_demand {
    seen, None -> seen
    None, Some(value) -> Some(value)
    Some(seen), Some(value) if seen == value -> max_seen
    Some(seen), Some(value) -> {
      platform.log_warning(
        "sluice: subscribers to one producer use different max_demand values ("
        <> int.to_string(seen)
        <> " and "
        <> int.to_string(value)
        <> "). The demand dispatcher balances best when the values are equal.",
      )
      max_seen
    }
  }
}

type DemandState(event) {
  DemandState(
    demands: List(#(Int, Subject(ConsumerMessage(event)))),
    pending: Int,
    max_seen: Option(Int),
  )
}

fn make_demand(state: DemandState(event)) -> Dispatcher(event) {
  Dispatcher(
    subscribe: fn(from, metadata) {
      let demands = list.append(state.demands, [#(0, from)])
      let max_seen = observe_max_demand(state.max_seen, metadata.max_demand)
      Ok(make_demand(DemandState(..state, demands:, max_seen:)))
    },
    ask: fn(demand, from) {
      case list.any(state.demands, fn(entry) { entry.1 == from }) {
        // Discard an ask from an unknown subscriber. This can occur when
        // the ask and the cancellation of one subscriber cross. The stage
        // does not stop.
        False -> #(0, make_demand(state))
        True -> {
          let absorbed = int.min(demand, state.pending)
          let demands =
            add_demand(demands: state.demands, from: from, demand: demand)
          let next =
            make_demand(
              DemandState(..state, demands:, pending: state.pending - absorbed),
            )
          #(demand - absorbed, next)
        }
      }
    },
    cancel: fn(from) {
      let demands = list.filter(state.demands, fn(entry) { entry.1 != from })
      let orphaned = total(state.demands) - total(demands)
      let next =
        make_demand(
          DemandState(..state, demands:, pending: state.pending + orphaned),
        )
      #(0, next)
    },
    dispatch: fn(events, _incoming) {
      let #(deliveries, leftover, demands) =
        dispatch_loop(
          events: events,
          demands: sort_descending(state.demands),
          delivered: [],
        )
      DispatchResult(
        deliveries:,
        leftover:,
        next: make_demand(DemandState(..state, demands:)),
      )
    },
    total_demand: fn() { total(state.demands) },
  )
}

fn dispatch_loop(
  events events: List(event),
  demands demands: List(#(Int, Subject(ConsumerMessage(event)))),
  delivered delivered: List(Delivery(event)),
) -> #(
  List(Delivery(event)),
  List(event),
  List(#(Int, Subject(ConsumerMessage(event)))),
) {
  case events, demands {
    [], _ -> #(list.reverse(delivered), [], demands)
    _, [] -> #(list.reverse(delivered), events, demands)
    _, [#(0, _), ..] -> #(list.reverse(delivered), events, demands)
    _, [#(demand, subscriber), ..remaining_demands] -> {
      let count = int.min(demand, list.length(events))
      let #(batch, remaining_events) = list.split(events, count)
      let demands =
        sort_descending([#(demand - count, subscriber), ..remaining_demands])
      dispatch_loop(events: remaining_events, demands: demands, delivered: [
        Delivery(subscriber, batch),
        ..delivered
      ])
    }
  }
}

fn add_demand(
  demands demands: List(#(Int, Subject(ConsumerMessage(event)))),
  from from: Subject(ConsumerMessage(event)),
  demand demand: Int,
) -> List(#(Int, Subject(ConsumerMessage(event)))) {
  list.map(demands, fn(entry) {
    case entry.1 == from {
      True -> #(entry.0 + demand, from)
      False -> entry
    }
  })
  |> sort_descending
}

fn sort_descending(
  demands: List(#(Int, Subject(ConsumerMessage(event)))),
) -> List(#(Int, Subject(ConsumerMessage(event)))) {
  list.sort(demands, fn(first, second) { int.compare(second.0, first.0) })
}

fn total(demands: List(#(Int, Subject(ConsumerMessage(event))))) -> Int {
  list.fold(demands, 0, fn(sum, entry) { sum + entry.0 })
}
