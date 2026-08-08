//// Tests for the subscription hooks: the subscriber changes on a
//// producer, the subscription hook on a sink, and the end hook that
//// replaces the cancel mode.

import gleam/erlang/process
import sluice
import sluice/gate
import sluice/sink
import sluice/source
import support

pub fn producer_sees_subscriber_changes_test() {
  let change_probe = process.new_subject()
  let assert Ok(counter) =
    support.counter_source()
    |> source.on_subscribers(fn(state, change) {
      process.send(change_probe, change)
      state
    })
    |> source.start()
  let assert Ok(first) = support.quiet_sink() |> sink.start()
  let assert Ok(second) = support.quiet_sink() |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: first.data)
  let assert Ok(sluice.SubscriberArrived(1)) =
    process.receive(change_probe, 1000)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: second.data)
  let assert Ok(sluice.SubscriberArrived(2)) =
    process.receive(change_probe, 1000)

  support.shutdown(first)
  let assert Ok(sluice.SubscriberLeft(1)) = process.receive(change_probe, 1000)

  support.shutdown(second)
  support.shutdown(counter)
}

pub fn gate_sees_subscriber_changes_test() {
  let change_probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(events, state)
    })
    |> gate.on_subscribers(fn(state, change) {
      process.send(change_probe, change)
      state
    })
    |> gate.subscribe(
      sluice.subscription(to: counter.data) |> support.small_demand,
    )
    |> gate.start()
  let assert Ok(collector) = support.quiet_sink() |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: gate.outlet(doubler.data))
    |> support.small_demand
    |> sluice.subscribe(consumer: collector.data)
  let assert Ok(sluice.SubscriberArrived(1)) =
    process.receive(change_probe, 1000)

  support.shutdown(collector)
  let assert Ok(sluice.SubscriberLeft(0)) = process.receive(change_probe, 1000)

  support.shutdown(doubler)
  support.shutdown(counter)
}

// The subscription hook gives the sink its subscription at the start.
// Thus a sink with manual demand can make its first ask by itself.
pub fn sink_hook_makes_the_first_ask_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
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

  support.shutdown(collector)
  support.shutdown(counter)
}

// The end hook replaces the cancel mode: a Permanent subscription would
// stop the sink, but the hook decides to continue.
pub fn end_hook_replaces_the_cancel_mode_test() {
  let end_probe = process.new_subject()
  let assert Ok(finite) =
    source.new(init: False, on_demand: fn(exhausted, _demand) {
      case exhausted {
        True -> source.stop()
        False -> source.emit(support.count_up(0, 4), True)
      }
    })
    |> source.start()
  let assert Ok(collector) =
    support.quiet_sink()
    |> sink.on_cancelled(fn(state, end) {
      process.send(end_probe, end)
      sink.continue(state)
    })
    |> sink.start()
  process.unlink(finite.pid)

  let assert Ok(_) =
    sluice.subscription(to: finite.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(sluice.ProducerStopped) = process.receive(end_probe, 2000)
  assert process.is_alive(collector.pid)

  support.shutdown(collector)
}

pub fn end_hook_reports_a_failure_test() {
  let end_probe = process.new_subject()
  let assert Ok(crasher) =
    source.new(init: Nil, on_demand: fn(_state, _demand) {
      source.stop_abnormal("boom")
    })
    |> source.start()
  let assert Ok(collector) =
    support.quiet_sink()
    |> sink.on_cancelled(fn(_state, end) {
      process.send(end_probe, end)
      sink.stop()
    })
    |> sink.start()
  process.unlink(crasher.pid)
  process.unlink(collector.pid)

  let assert Ok(_) =
    sluice.subscription(to: crasher.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(sluice.ProducerFailed(_)) = process.receive(end_probe, 2000)
}
