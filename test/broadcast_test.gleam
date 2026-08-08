//// Tests for the broadcast dispatcher: delivery to all subscribers, the
//// speed of the slowest subscriber, per-subscriber filters, and the
//// demand that a removal releases.

import gleam/erlang/process
import gleam/list
import sluice
import sluice/dispatcher
import sluice/gate
import sluice/sink
import sluice/source
import support

fn broadcast_counter() {
  source.new(init: 0, on_demand: fn(counter, demand) {
    source.emit(support.count_up(counter, demand), counter + demand)
  })
  |> source.dispatcher(dispatcher.broadcast())
}

// The manual mode makes the sequence deterministic: the flow starts only
// when both subscribers have open demand, and both receive the same
// events.
pub fn broadcast_delivers_to_all_subscribers_test() {
  let first_probe = process.new_subject()
  let second_probe = process.new_subject()
  let assert Ok(counter) = broadcast_counter() |> source.start()
  let assert Ok(first) = support.collector(first_probe) |> sink.start()
  let assert Ok(second) = support.collector(second_probe) |> sink.start()

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

  support.shutdown(second)
  support.shutdown(first)
  support.shutdown(counter)
}

pub fn broadcast_selector_filters_per_subscriber_test() {
  let all_probe = process.new_subject()
  let even_probe = process.new_subject()
  let assert Ok(counter) = broadcast_counter() |> source.start()
  let assert Ok(all_events) = support.collector(all_probe) |> sink.start()
  let assert Ok(even_events) = support.collector(even_probe) |> sink.start()

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

  support.shutdown(even_events)
  support.shutdown(all_events)
  support.shutdown(counter)
}

// A subscriber with a filter must not stall the automatic demand: the
// filtered events do not use its demand, so the producer account and
// the consumer account stay equal across many rounds.
pub fn selector_does_not_stall_automatic_demand_test() {
  let even_probe = process.new_subject()
  let assert Ok(counter) = broadcast_counter() |> source.start()
  let assert Ok(all_events) =
    support.collector(process.new_subject()) |> sink.start()
  let assert Ok(even_events) = support.collector(even_probe) |> sink.start()

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
  let received = support.collect_events(even_probe, 10, [])
  assert list.length(received) == 10
  assert consecutive_evens(received)

  support.shutdown(even_events)
  support.shutdown(all_events)
  support.shutdown(counter)
}

// Replacement demand must return through the source mailbox. If it is
// produced recursively, a selector that rejects every event keeps the source
// inside one actor turn forever, so it cannot process a stop message.
pub fn rejecting_selector_does_not_monopolise_source_test() {
  let emitter_probe = process.new_subject()
  let demand_probe = process.new_subject()
  let assert Ok(counter) =
    source.new_with_emitter(init: fn(emitter) {
      process.send(emitter_probe, emitter)
      Ok(0)
    })
    |> source.on_demand(fn(counter, demand) {
      process.send(demand_probe, demand)
      source.emit(support.count_up(counter, demand), counter + demand)
    })
    |> source.dispatcher(dispatcher.broadcast())
    |> source.start()
  let assert Ok(emitter) = process.receive(emitter_probe, 1000)
  let assert Ok(rejector) =
    support.collector(process.new_subject()) |> sink.start()
  process.unlink(counter.pid)
  let monitor = process.monitor(counter.pid)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.cancel_mode(sluice.Temporary)
    |> sluice.selector(keep: fn(_event) { False })
    |> sluice.subscribe(consumer: rejector.data)

  // The source enters replacement production. It must still return to its
  // mailbox and process this external stop.
  let assert Ok(_) = process.receive(demand_probe, 1000)
  source.finish(emitter)
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(down_selector, 1000)

  support.shutdown(rejector)
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

// A gate can also broadcast: each subscriber of the gate receives the
// full changed flow.
pub fn gate_with_broadcast_dispatcher_test() {
  let first_probe = process.new_subject()
  let second_probe = process.new_subject()
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      source.emit(support.count_up(counter, demand), counter + demand)
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
  let assert Ok(first) = support.collector(first_probe) |> sink.start()
  let assert Ok(second) = support.collector(second_probe) |> sink.start()

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

  support.shutdown(second)
  support.shutdown(first)
  support.shutdown(doubler)
  support.shutdown(counter)
}

// The removal of the slowest subscriber releases demand: the other
// subscribers receive events without a new ask.
pub fn broadcast_removal_of_slowest_releases_demand_test() {
  let fast_probe = process.new_subject()
  let assert Ok(counter) = broadcast_counter() |> source.start()
  let assert Ok(fast) = support.collector(fast_probe) |> sink.start()
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

  support.shutdown(slow)
  support.shutdown(fast)
  support.shutdown(counter)
}
