//// Tests for the life of a subscription: each cancel mode against each
//// type of producer stop, manual cancellation, the subscribe and start
//// timeouts, and the isolation of the subscribers from each other.

import gleam/erlang/atom
import gleam/erlang/process.{type ExitReason, type Monitor, type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor.{type Started}
import sluice
import sluice/sink
import sluice/source

fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}

fn counter_source(demand_probe: Subject(Int)) {
  source.new(init: 0, on_demand: fn(counter, demand) {
    process.send(demand_probe, demand)
    source.emit(count_up(counter, demand), counter + demand)
  })
}

/// A source that fails at the first ask for events.
fn crashing_source() {
  source.new(init: Nil, on_demand: fn(_state, _demand) {
    source.stop_abnormal("boom")
  })
}

fn lockstep_sink(batch_probe: Subject(#(List(Int), Subject(Nil)))) {
  sink.new(init: Nil, on_events: fn(_state, events, _subscription) {
    let step = process.new_subject()
    process.send(batch_probe, #(events, step))
    let assert Ok(Nil) = process.receive(step, 10_000)
    sink.continue(Nil)
  })
}

fn quiet_sink() {
  sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
    sink.continue(state)
  })
}

/// Disconnect the link between a stage and the test process. Thus a stop
/// of the stage can not stop the test. Monitor the stage instead.
fn watch(started: Started(data)) -> Monitor {
  process.unlink(started.pid)
  process.monitor(started.pid)
}

fn await_down(monitor: Monitor) -> ExitReason {
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, reason)) =
    process.selector_receive(selector, 2000)
  reason
}

fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

fn small_demand(options: sluice.SubscriptionOptions(event)) {
  options |> sluice.min_demand(2) |> sluice.max_demand(6)
}

/// Show that a sink is alive and that it operates: subscribe it to a new
/// counter source, and see that events come.
fn assert_sink_still_works(
  collector: Started(sluice.Inlet(Int)),
  batch_probe: Subject(#(List(Int), Subject(Nil))),
) -> Nil {
  let assert Ok(replacement) =
    counter_source(process.new_subject()) |> source.start()
  let assert Ok(_) =
    sluice.subscription(to: replacement.data)
    |> small_demand
    |> sluice.cancel_mode(sluice.Temporary)
    |> sluice.subscribe(consumer: collector.data)
  let assert Ok(#(_events, step)) = process.receive(batch_probe, 1000)
  process.send(step, Nil)
  shutdown(replacement)
}

pub fn permanent_sink_stops_when_source_crashes_test() {
  let assert Ok(crasher) = crashing_source() |> source.start()
  let assert Ok(collector) = quiet_sink() |> sink.start()
  let _source_monitor = watch(crasher)
  let sink_monitor = watch(collector)

  let assert Ok(_) =
    sluice.subscription(to: crasher.data)
    |> small_demand
    |> sluice.subscribe(consumer: collector.data)

  let assert process.Abnormal(_) = await_down(sink_monitor)
}

pub fn transient_sink_stops_when_source_crashes_test() {
  let assert Ok(crasher) = crashing_source() |> source.start()
  let assert Ok(collector) = quiet_sink() |> sink.start()
  let _source_monitor = watch(crasher)
  let sink_monitor = watch(collector)

  let assert Ok(_) =
    sluice.subscription(to: crasher.data)
    |> small_demand
    |> sluice.cancel_mode(sluice.Transient)
    |> sluice.subscribe(consumer: collector.data)

  let assert process.Abnormal(_) = await_down(sink_monitor)
}

pub fn transient_sink_survives_normal_stop_test() {
  let batch_probe = process.new_subject()
  // This source emits four events at the first ask. It stops with the
  // normal reason at the second ask.
  let assert Ok(finite) =
    source.new(init: False, on_demand: fn(exhausted, _demand) {
      case exhausted {
        True -> source.stop()
        False -> source.emit(count_up(0, 4), True)
      }
    })
    |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()
  let source_monitor = watch(finite)
  let _sink_monitor = watch(collector)

  let assert Ok(_) =
    sluice.subscription(to: finite.data)
    |> small_demand
    |> sluice.cancel_mode(sluice.Transient)
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(#(_events, step)) = process.receive(batch_probe, 1000)
  process.send(step, Nil)
  let assert process.Normal = await_down(source_monitor)

  assert_sink_still_works(collector, batch_probe)
  shutdown(collector)
}

pub fn transient_sink_survives_shutdown_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) =
    counter_source(process.new_subject()) |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()
  let source_monitor = watch(counter)
  let _sink_monitor = watch(collector)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.cancel_mode(sluice.Transient)
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(#(_events, step)) = process.receive(batch_probe, 1000)
  process.send(step, Nil)

  // A supervisor shutdown is not a failure. The Transient sink
  // continues.
  process.send_abnormal_exit(counter.pid, atom.create("shutdown"))
  let assert process.Abnormal(_) = await_down(source_monitor)
  drain_in_flight(batch_probe)

  assert_sink_still_works(collector, batch_probe)
  shutdown(collector)
}

pub fn temporary_sink_survives_crash_test() {
  let batch_probe = process.new_subject()
  let assert Ok(crasher) = crashing_source() |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()
  let source_monitor = watch(crasher)
  let _sink_monitor = watch(collector)

  let assert Ok(_) =
    sluice.subscription(to: crasher.data)
    |> small_demand
    |> sluice.cancel_mode(sluice.Temporary)
    |> sluice.subscribe(consumer: collector.data)

  let assert process.Abnormal(_) = await_down(source_monitor)

  assert_sink_still_works(collector, batch_probe)
  shutdown(collector)
}

/// Move a lockstep sink through the batches that move to it, until no
/// more batches come. This occurs, for example, after a cancellation.
fn drain_in_flight(batch_probe: Subject(#(List(Int), Subject(Nil)))) -> Nil {
  case process.receive(batch_probe, 200) {
    Error(Nil) -> Nil
    Ok(#(_events, step)) -> {
      process.send(step, Nil)
      drain_in_flight(batch_probe)
    }
  }
}

pub fn manual_cancel_permanent_stops_sink_test() {
  let demand_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(counter) = counter_source(demand_probe) |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()
  process.unlink(counter.pid)
  let sink_monitor = watch(collector)

  let assert Ok(subscription) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(#(_events, step)) = process.receive(batch_probe, 1000)
  sluice.cancel(subscription)
  process.send(step, Nil)
  drain_in_flight(batch_probe)

  // A cancellation of a Permanent subscription stops the consumer
  // stage. The stop reason is normal, because there was no failure.
  let assert process.Normal = await_down(sink_monitor)
  assert process.is_alive(counter.pid)

  shutdown(counter)
}

pub fn manual_cancel_temporary_keeps_sink_and_stops_demand_test() {
  let demand_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(counter) = counter_source(demand_probe) |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()
  process.unlink(counter.pid)
  let _sink_monitor = watch(collector)

  let assert Ok(subscription) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.cancel_mode(sluice.Temporary)
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(#(_events, step)) = process.receive(batch_probe, 1000)
  sluice.cancel(subscription)
  process.send(step, Nil)
  drain_in_flight(batch_probe)

  // The producer stays alive. It receives no more demand.
  while_quiet(demand_probe)
  assert process.is_alive(counter.pid)
  assert process.is_alive(collector.pid)

  // The old subscription is fully removed. A new subscription to the
  // same source is successful (no AlreadySubscribed). Events come again.
  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.cancel_mode(sluice.Temporary)
    |> sluice.subscribe(consumer: collector.data)
  let assert Ok(#(_events, step)) = process.receive(batch_probe, 1000)
  process.send(step, Nil)

  shutdown(collector)
  shutdown(counter)
}

fn while_quiet(demand_probe: Subject(Int)) -> Nil {
  // First remove the messages that are already in the queue. Then make
  // sure that no more messages come.
  case process.receive(demand_probe, 0) {
    Ok(_) -> while_quiet(demand_probe)
    Error(Nil) -> {
      assert process.receive(demand_probe, 150) == Error(Nil)
    }
  }
}

pub fn aborting_sink_exits_abnormally_test() {
  let assert Ok(counter) =
    counter_source(process.new_subject()) |> source.start()
  let assert Ok(aborter) =
    sink.new(init: Nil, on_events: fn(_state, _events, _subscription) {
      sink.stop_abnormal("cannot handle it")
    })
    |> sink.start()
  process.unlink(counter.pid)
  let sink_monitor = watch(aborter)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.subscribe(consumer: aborter.data)

  let assert process.Abnormal(_) = await_down(sink_monitor)
  shutdown(counter)
}

pub fn sibling_survives_other_consumers_death_test() {
  let batch_probe_first = process.new_subject()
  let batch_probe_second = process.new_subject()
  let assert Ok(counter) =
    counter_source(process.new_subject()) |> source.start()
  let assert Ok(doomed) = lockstep_sink(batch_probe_first) |> sink.start()
  let assert Ok(survivor) = lockstep_sink(batch_probe_second) |> sink.start()
  process.unlink(counter.pid)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.subscribe(consumer: doomed.data)
  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.subscribe(consumer: survivor.data)

  let assert Ok(#(_events, _step)) = process.receive(batch_probe_first, 1000)
  let assert Ok(#(_events, step)) = process.receive(batch_probe_second, 1000)
  shutdown(doomed)

  // The other sink continues to receive events.
  process.send(step, Nil)
  let assert Ok(#(_events, step)) = process.receive(batch_probe_second, 1000)
  process.send(step, Nil)

  shutdown(survivor)
  shutdown(counter)
}

pub fn subscribe_timeout_is_configurable_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) =
    counter_source(process.new_subject()) |> source.start()
  let assert Ok(other) = counter_source(process.new_subject()) |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.subscribe(consumer: collector.data)

  // The sink stays in its first batch. It can not answer a second
  // subscribe request, and the short timeout ends the wait.
  let assert Ok(#(_events, _step)) = process.receive(batch_probe, 1000)
  assert sluice.subscription(to: other.data)
    |> sluice.subscribe_timeout(milliseconds: 50)
    |> sluice.subscribe(consumer: collector.data)
    == Error(sluice.SubscribeTimeout)

  shutdown(collector)
  shutdown(other)
  shutdown(counter)
}

pub fn start_timeout_is_configurable_test() {
  let assert Error(actor.InitTimeout) =
    source.new_with_emitter(init: fn(_emitter) {
      process.sleep(500)
      Ok(Nil)
    })
    |> source.start_timeout(milliseconds: 50)
    |> source.start()
}
