//// Tests for the subscription hooks: the subscriber changes on a
//// producer, the subscription hook on a sink, and the end hook that
//// replaces the cancel mode.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/otp/actor.{type Started}
import sluice
import sluice/gate
import sluice/sink
import sluice/source

fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}

fn counter_source() {
  source.new(init: 0, on_demand: fn(counter, demand) {
    source.emit(count_up(counter, demand), counter + demand)
  })
}

fn quiet_sink() {
  sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
    sink.continue(state)
  })
}

fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

fn small_demand(options: sluice.SubscriptionOptions(event)) {
  options |> sluice.min_demand(2) |> sluice.max_demand(6)
}

pub fn producer_sees_subscriber_changes_test() {
  let change_probe = process.new_subject()
  let assert Ok(counter) =
    counter_source()
    |> source.on_subscribers(fn(state, change) {
      process.send(change_probe, change)
      state
    })
    |> source.start()
  let assert Ok(first) = quiet_sink() |> sink.start()
  let assert Ok(second) = quiet_sink() |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.subscribe(consumer: first.data)
  let assert Ok(sluice.SubscriberArrived(1)) =
    process.receive(change_probe, 1000)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.subscribe(consumer: second.data)
  let assert Ok(sluice.SubscriberArrived(2)) =
    process.receive(change_probe, 1000)

  shutdown(first)
  let assert Ok(sluice.SubscriberLeft(1)) = process.receive(change_probe, 1000)

  shutdown(second)
  shutdown(counter)
}

pub fn gate_sees_subscriber_changes_test() {
  let change_probe = process.new_subject()
  let assert Ok(counter) = counter_source() |> source.start()
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(events, state)
    })
    |> gate.on_subscribers(fn(state, change) {
      process.send(change_probe, change)
      state
    })
    |> gate.subscribe(sluice.subscription(to: counter.data) |> small_demand)
    |> gate.start()
  let assert Ok(collector) = quiet_sink() |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: gate.outlet(doubler.data))
    |> small_demand
    |> sluice.subscribe(consumer: collector.data)
  let assert Ok(sluice.SubscriberArrived(1)) =
    process.receive(change_probe, 1000)

  shutdown(collector)
  let assert Ok(sluice.SubscriberLeft(0)) = process.receive(change_probe, 1000)

  shutdown(doubler)
  shutdown(counter)
}

// The subscription hook gives the sink its subscription at the start.
// Thus a sink with manual demand can make its first ask by itself.
pub fn sink_hook_makes_the_first_ask_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) = counter_source() |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
    })
    |> sink.on_subscribed(fn(state, subscription) {
      sluice.ask(subscription:, count: 2)
      sink.continue(state)
    })
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok([0, 1]) = process.receive(batch_probe, 1000)

  shutdown(collector)
  shutdown(counter)
}

// The end hook replaces the cancel mode: a Permanent subscription would
// stop the sink, but the hook decides to continue.
pub fn end_hook_replaces_the_cancel_mode_test() {
  let end_probe = process.new_subject()
  let assert Ok(finite) =
    source.new(init: False, on_demand: fn(exhausted, _demand) {
      case exhausted {
        True -> source.stop()
        False -> source.emit(count_up(0, 4), True)
      }
    })
    |> source.start()
  let assert Ok(collector) =
    quiet_sink()
    |> sink.on_cancelled(fn(state, end) {
      process.send(end_probe, end)
      sink.continue(state)
    })
    |> sink.start()
  process.unlink(finite.pid)

  let assert Ok(_) =
    sluice.subscription(to: finite.data)
    |> small_demand
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(sluice.ProducerStopped) = process.receive(end_probe, 2000)
  assert process.is_alive(collector.pid)

  shutdown(collector)
}

pub fn end_hook_reports_a_failure_test() {
  let end_probe = process.new_subject()
  let assert Ok(crasher) =
    source.new(init: Nil, on_demand: fn(_state, _demand) {
      source.stop_abnormal("boom")
    })
    |> source.start()
  let assert Ok(collector) =
    quiet_sink()
    |> sink.on_cancelled(fn(_state, end) {
      process.send(end_probe, end)
      sink.stop()
    })
    |> sink.start()
  process.unlink(crasher.pid)
  process.unlink(collector.pid)

  let assert Ok(_) =
    sluice.subscription(to: crasher.data)
    |> small_demand
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(sluice.ProducerFailed(_)) = process.receive(end_probe, 2000)
}
