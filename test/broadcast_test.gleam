//// Tests for the broadcast dispatcher: delivery to all subscribers, the
//// speed of the slowest subscriber, per-subscriber filters, and the
//// demand that a removal releases.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor.{type Started}
import sluice
import sluice/dispatcher
import sluice/gate
import sluice/sink
import sluice/source

fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}

fn broadcast_counter() {
  source.new(init: 0, on_demand: fn(counter, demand) {
    source.emit(count_up(counter, demand), counter + demand)
  })
  |> source.dispatcher(dispatcher.broadcast())
}

fn collector(batch_probe: Subject(List(Int))) {
  sink.new(init: Nil, on_events: fn(state, events, _subscription) {
    process.send(batch_probe, events)
    sink.continue(state)
  })
}

fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

// The manual mode makes the sequence deterministic: the flow starts only
// when both subscribers have open demand, and both receive the same
// events.
pub fn broadcast_delivers_to_all_subscribers_test() {
  let first_probe = process.new_subject()
  let second_probe = process.new_subject()
  let assert Ok(counter) = broadcast_counter() |> source.start()
  let assert Ok(first) = collector(first_probe) |> sink.start()
  let assert Ok(second) = collector(second_probe) |> sink.start()

  let assert Ok(first_subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: first.data)
  let assert Ok(second_subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: second.data)

  // One subscriber with demand is not sufficient: the broadcast moves at
  // the speed of the slowest subscriber.
  sluice.ask(subscription: first_subscription, count: 4)
  assert process.receive(first_probe, 150) == Error(Nil)

  // When the second subscriber asks, both receive the same events.
  sluice.ask(subscription: second_subscription, count: 4)
  let assert Ok([0, 1, 2, 3]) = process.receive(first_probe, 1000)
  let assert Ok([0, 1, 2, 3]) = process.receive(second_probe, 1000)

  shutdown(second)
  shutdown(first)
  shutdown(counter)
}

pub fn broadcast_selector_filters_per_subscriber_test() {
  let all_probe = process.new_subject()
  let even_probe = process.new_subject()
  let assert Ok(counter) = broadcast_counter() |> source.start()
  let assert Ok(all_events) = collector(all_probe) |> sink.start()
  let assert Ok(even_events) = collector(even_probe) |> sink.start()

  let assert Ok(all_subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: all_events.data)
  let assert Ok(even_subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.selector(keep: fn(event) { event % 2 == 0 })
    |> sluice.subscribe(consumer: even_events.data)

  sluice.ask(subscription: all_subscription, count: 4)
  sluice.ask(subscription: even_subscription, count: 4)
  let assert Ok([0, 1, 2, 3]) = process.receive(all_probe, 1000)
  let assert Ok([0, 2]) = process.receive(even_probe, 1000)

  shutdown(even_events)
  shutdown(all_events)
  shutdown(counter)
}

// A subscriber with a filter must not stall the automatic demand: the
// filtered events do not use its demand, so the producer account and
// the consumer account stay equal across many rounds.
pub fn selector_does_not_stall_automatic_demand_test() {
  let even_probe = process.new_subject()
  let assert Ok(counter) = broadcast_counter() |> source.start()
  let assert Ok(all_events) = collector(process.new_subject()) |> sink.start()
  let assert Ok(even_events) = collector(even_probe) |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: all_events.data)
  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.selector(keep: fn(event) { event % 2 == 0 })
    |> sluice.subscribe(consumer: even_events.data)

  // Ten even events need several demand rounds. Before the demand
  // refund for filtered events, the flow stopped after the first round.
  // The filtered subscriber joins a flow that possibly moves, so the
  // test checks the shape of the sequence, not the first value.
  let received = collect_events(even_probe, 10, [])
  assert list.length(received) == 10
  assert consecutive_evens(received)

  shutdown(even_events)
  shutdown(all_events)
  shutdown(counter)
}

fn consecutive_evens(events: List(Int)) -> Bool {
  case events {
    [] -> True
    [only] -> only % 2 == 0
    [first, second, ..remaining] ->
      first % 2 == 0
      && second == first + 2
      && consecutive_evens([second, ..remaining])
  }
}

fn collect_events(
  probe: Subject(List(Int)),
  target: Int,
  received: List(Int),
) -> List(Int) {
  case list.length(received) >= target {
    True -> list.take(received, target)
    False ->
      case process.receive(probe, 1000) {
        Error(Nil) -> received
        Ok(events) ->
          collect_events(probe, target, list.append(received, events))
      }
  }
}

// A gate can also broadcast: each subscriber of the gate receives the
// full changed flow.
pub fn gate_with_broadcast_dispatcher_test() {
  let first_probe = process.new_subject()
  let second_probe = process.new_subject()
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      source.emit(count_up(counter, demand), counter + demand)
    })
    |> source.start()
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(list.map(events, fn(event) { event * 2 }), state)
    })
    |> gate.dispatcher(dispatcher.broadcast())
    |> gate.subscribe(
      sluice.subscription(to: counter.data)
      |> sluice.min_demand(2)
      |> sluice.max_demand(6),
    )
    |> gate.start()
  let assert Ok(first) = collector(first_probe) |> sink.start()
  let assert Ok(second) = collector(second_probe) |> sink.start()

  let assert Ok(first_subscription) =
    sluice.subscription(to: gate.outlet(doubler.data))
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: first.data)
  let assert Ok(second_subscription) =
    sluice.subscription(to: gate.outlet(doubler.data))
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: second.data)

  sluice.ask(subscription: first_subscription, count: 3)
  sluice.ask(subscription: second_subscription, count: 3)
  let assert Ok([0, 2, 4]) = process.receive(first_probe, 1000)
  let assert Ok([0, 2, 4]) = process.receive(second_probe, 1000)

  shutdown(second)
  shutdown(first)
  shutdown(doubler)
  shutdown(counter)
}

// The removal of the slowest subscriber releases demand: the other
// subscribers receive events without a new ask.
pub fn broadcast_removal_of_slowest_releases_demand_test() {
  let fast_probe = process.new_subject()
  let assert Ok(counter) = broadcast_counter() |> source.start()
  let assert Ok(fast) = collector(fast_probe) |> sink.start()
  let assert Ok(slow) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.start()

  let assert Ok(fast_subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: fast.data)
  let assert Ok(slow_subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.cancel_mode(sluice.Temporary)
    |> sluice.subscribe(consumer: slow.data)

  // The fast subscriber asks for six events. The slow one asks for two.
  // The flow stops after two events.
  sluice.ask(subscription: fast_subscription, count: 6)
  sluice.ask(subscription: slow_subscription, count: 2)
  let assert Ok([0, 1]) = process.receive(fast_probe, 1000)
  assert process.receive(fast_probe, 150) == Error(Nil)

  // The slow subscriber leaves: the remaining demand of the fast
  // subscriber is released, without a new ask.
  sluice.cancel(slow_subscription)
  let assert Ok([2, 3, 4, 5]) = process.receive(fast_probe, 1000)

  shutdown(slow)
  shutdown(fast)
  shutdown(counter)
}
