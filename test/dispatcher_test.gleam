import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import sluice/internal/dispatcher.{type Dispatcher, Delivery}
import sluice/internal/protocol.{type ConsumerMessage}

fn subscriber() -> Subject(ConsumerMessage(Int)) {
  process.new_subject()
}

fn subscribed(
  dispatcher: Dispatcher(event),
  from: Subject(ConsumerMessage(event)),
) -> Dispatcher(event) {
  let assert Ok(dispatcher) =
    dispatcher.subscribe(from, protocol.default_metadata())
  dispatcher
}

pub fn no_subscribers_everything_is_leftover_test() {
  let demand_dispatcher = dispatcher.demand()
  let result = demand_dispatcher.dispatch([1, 2, 3], 3)
  assert result.deliveries == []
  assert result.leftover == [1, 2, 3]
}

pub fn no_demand_everything_is_leftover_test() {
  let consumer = subscriber()
  let demand_dispatcher = dispatcher.demand() |> subscribed(consumer)
  let result = demand_dispatcher.dispatch([1, 2, 3], 3)
  assert result.deliveries == []
  assert result.leftover == [1, 2, 3]
}

pub fn dispatch_caps_at_recorded_demand_test() {
  let consumer = subscriber()
  let demand_dispatcher = dispatcher.demand() |> subscribed(consumer)
  let #(delta, demand_dispatcher) = demand_dispatcher.ask(2, consumer)
  assert delta == 2
  let result = demand_dispatcher.dispatch([1, 2, 3, 4], 4)
  assert result.deliveries == [Delivery(consumer, [1, 2])]
  assert result.leftover == [3, 4]
  assert result.next.total_demand() == 0
}

pub fn demand_carries_over_between_dispatches_test() {
  let consumer = subscriber()
  let demand_dispatcher = dispatcher.demand() |> subscribed(consumer)
  let #(_, demand_dispatcher) = demand_dispatcher.ask(5, consumer)
  let result = demand_dispatcher.dispatch([1, 2], 2)
  assert result.deliveries == [Delivery(consumer, [1, 2])]
  assert result.next.total_demand() == 3
  let result = result.next.dispatch([3, 4, 5, 6], 4)
  assert result.deliveries == [Delivery(consumer, [3, 4, 5])]
  assert result.leftover == [6]
}

pub fn events_go_to_greatest_demand_first_test() {
  let hungry = subscriber()
  let modest = subscriber()
  let demand_dispatcher =
    dispatcher.demand() |> subscribed(hungry) |> subscribed(modest)
  let #(_, demand_dispatcher) = demand_dispatcher.ask(10, hungry)
  let #(_, demand_dispatcher) = demand_dispatcher.ask(2, modest)
  // There are 5 events. All are in the demand of 10 from `hungry`.
  let result = demand_dispatcher.dispatch([1, 2, 3, 4, 5], 5)
  assert result.deliveries == [Delivery(hungry, [1, 2, 3, 4, 5])]
  assert result.leftover == []
}

pub fn overflow_spills_to_next_subscriber_test() {
  let first = subscriber()
  let second = subscriber()
  let demand_dispatcher =
    dispatcher.demand() |> subscribed(first) |> subscribed(second)
  let #(_, demand_dispatcher) = demand_dispatcher.ask(3, first)
  let #(_, demand_dispatcher) = demand_dispatcher.ask(3, second)
  let result = demand_dispatcher.dispatch([1, 2, 3, 4, 5], 5)
  let total_delivered =
    list.fold(result.deliveries, 0, fn(sum, delivery) {
      sum + list.length(delivery.events)
    })
  assert total_delivered == 5
  assert result.leftover == []
  assert result.next.total_demand() == 1
}

pub fn cancel_reports_zero_revoked_and_absorbs_future_asks_test() {
  let leaver = subscriber()
  let stayer = subscriber()
  let demand_dispatcher =
    dispatcher.demand() |> subscribed(leaver) |> subscribed(stayer)
  let #(delta, demand_dispatcher) = demand_dispatcher.ask(4, leaver)
  assert delta == 4
  let #(revoked, demand_dispatcher) = demand_dispatcher.cancel(leaver)
  // The stage received an ask for 4 events before. The dispatcher does
  // not remove that demand. It keeps the demand as `pending`. The
  // `pending` value decreases the subsequent asks. Thus the stage does not
  // make too many events.
  assert revoked == 0
  assert demand_dispatcher.total_demand() == 0
  let #(delta, demand_dispatcher) = demand_dispatcher.ask(3, stayer)
  assert delta == 0
  let #(delta, _) = demand_dispatcher.ask(3, stayer)
  assert delta == 2
}

pub fn ask_from_unknown_subscriber_is_ignored_test() {
  let stranger = subscriber()
  let demand_dispatcher = dispatcher.demand()
  let #(delta, demand_dispatcher) = demand_dispatcher.ask(5, stranger)
  assert delta == 0
  assert demand_dispatcher.total_demand() == 0
}

pub fn dispatch_preserves_event_order_across_subscribers_test() {
  let first = subscriber()
  let second = subscriber()
  let demand_dispatcher =
    dispatcher.demand() |> subscribed(first) |> subscribed(second)
  let #(_, demand_dispatcher) = demand_dispatcher.ask(2, first)
  let #(_, demand_dispatcher) = demand_dispatcher.ask(2, second)
  let result = demand_dispatcher.dispatch([1, 2, 3, 4], 4)
  let all_events =
    list.flat_map(result.deliveries, fn(delivery) { delivery.events })
  assert list.sort(all_events, by: int.compare) == [1, 2, 3, 4]
}
