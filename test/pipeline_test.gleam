//// Full tests for Source -> Sink pipelines. The tests use probe subjects
//// and lockstep sinks. They do not use sleeps.

import gleam/erlang/process.{type Subject}
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

/// A source that counts up without an end. It sends each demand that it
/// receives to the probe.
fn counter_source(demand_probe: Subject(Int)) {
  source.new(init: 0, on_demand: fn(counter, demand) {
    process.send(demand_probe, demand)
    source.emit(count_up(counter, demand), counter + demand)
  })
}

/// A sink that sends each batch to the probe, together with a subject.
/// The test must send a reply on that subject before the sink continues.
fn lockstep_sink(batch_probe: Subject(#(List(Int), Subject(Nil)))) {
  sink.new(init: Nil, on_events: fn(_state, events, _subscription) {
    let step = process.new_subject()
    process.send(batch_probe, #(events, step))
    let assert Ok(Nil) = process.receive(step, 10_000)
    sink.continue(Nil)
  })
}

/// Move a lockstep sink forward until the quantity of collected events is
/// `target`, or until no more events come.
fn collect_events(
  batch_probe: Subject(#(List(Int), Subject(Nil))),
  target: Int,
  received: List(Int),
) -> List(Int) {
  case list.length(received) >= target {
    True -> received
    False ->
      case process.receive(batch_probe, 1000) {
        Error(Nil) -> received
        Ok(#(events, step)) -> {
          process.send(step, Nil)
          collect_events(batch_probe, target, list.append(received, events))
        }
      }
  }
}

fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

pub fn events_flow_in_order_test() {
  let demand_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(counter) = counter_source(demand_probe) |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  // The first ask is equal to max_demand.
  let assert Ok(first_demand) = process.receive(demand_probe, 1000)
  assert first_demand == 6

  // Collect the first three batches. Together they are the sequence from
  // zero, in parts of a maximum of max_demand - min_demand events.
  let received =
    int.range(from: 0, to: 3, with: [], run: fn(received, _index) {
      let assert Ok(#(events, step)) = process.receive(batch_probe, 1000)
      assert list.length(events) <= 4
      process.send(step, Nil)
      list.append(received, events)
    })
  assert received == count_up(0, list.length(received))

  shutdown(collector)
  shutdown(counter)
}

pub fn slow_sink_throttles_source_test() {
  let demand_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(counter) = counter_source(demand_probe) |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  // The first ask comes. The sink receives the first batch.
  let assert Ok(6) = process.receive(demand_probe, 1000)
  let assert Ok(#(_events, step)) = process.receive(batch_probe, 1000)

  // The sink stays in its batch. Thus no more demand goes to the
  // source. This is backpressure.
  assert process.receive(demand_probe, 100) == Error(Nil)

  // A step of the sink starts the demand again.
  process.send(step, Nil)
  let assert Ok(_) = process.receive(demand_probe, 1000)

  shutdown(collector)
  shutdown(counter)
}

pub fn overproducing_source_is_capped_by_buffer_and_demand_test() {
  let batch_probe = process.new_subject()
  // This source emits five times the quantity of the demand.
  let assert Ok(flooder) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      source.emit(count_up(counter, demand * 5), counter + demand * 5)
    })
    |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: flooder.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  // Each delivered batch obeys max_demand although the source makes too
  // many events. The extra events stay in the source buffer. Thus the
  // events come in sequence, without gaps.
  let received =
    int.range(from: 0, to: 5, with: [], run: fn(received, _index) {
      let assert Ok(#(events, step)) = process.receive(batch_probe, 1000)
      assert list.length(events) <= 6
      process.send(step, Nil)
      list.append(received, events)
    })
  assert received == count_up(0, list.length(received))

  shutdown(collector)
  shutdown(flooder)
}

pub fn emitter_pushes_flow_downstream_test() {
  let emitter_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(pushed) =
    source.new_with_emitter(init: fn(emitter) {
      process.send(emitter_probe, emitter)
      Ok(Nil)
    })
    |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()
  let assert Ok(emitter) = process.receive(emitter_probe, 1000)

  let assert Ok(_) =
    sluice.subscription(to: pushed.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  // No events come before a push.
  assert process.receive(batch_probe, 100) == Error(Nil)

  source.push(emitter, [1, 2, 3])
  let assert Ok(#([1, 2, 3], step)) = process.receive(batch_probe, 1000)
  process.send(step, Nil)

  // The buffer keeps the pushes that are more than the open demand.
  // They are not lost.
  source.push(emitter, count_up(4, 10))
  assert collect_events(batch_probe, 10, []) == count_up(4, 10)

  shutdown(collector)
  shutdown(pushed)
}

/// Push the events 1..10 into a source buffer that has a limit of 3,
/// while there is no subscriber. Then subscribe. Return the events that
/// the overflow policy kept.
fn overflow_survivors(keep: sluice.Keep) -> List(Int) {
  let emitter_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let discard_probe = process.new_subject()
  let assert Ok(pushed) =
    source.new_with_emitter(init: fn(emitter) {
      process.send(emitter_probe, emitter)
      Ok(Nil)
    })
    |> source.buffer_capacity(events: 3)
    |> source.buffer_keep(keep)
    |> source.on_discard(fn(state, count) {
      process.send(discard_probe, count)
      state
    })
    |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()
  let assert Ok(emitter) = process.receive(emitter_probe, 1000)

  source.push(emitter, count_up(1, 10))
  // Ten events went into a buffer with a limit of three: the source
  // reports seven discarded events through the callback.
  let assert Ok(7) = process.receive(discard_probe, 1000)
  let assert Ok(_) =
    sluice.subscription(to: pushed.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(#(survivors, step)) = process.receive(batch_probe, 1000)
  process.send(step, Nil)
  // The buffer kept only these events. No other events come.
  assert process.receive(batch_probe, 100) == Error(Nil)

  shutdown(collector)
  shutdown(pushed)
  survivors
}

pub fn source_overflow_keep_first_test() {
  assert overflow_survivors(sluice.KeepFirst) == [1, 2, 3]
}

pub fn source_overflow_keep_last_test() {
  assert overflow_survivors(sluice.KeepLast) == [8, 9, 10]
}

// A source can supply demand and accept pushes at the same time. A
// different process can stop it through its emitter.
pub fn hybrid_source_pulls_pushes_and_finishes_test() {
  let emitter_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(hybrid) =
    source.new_with_emitter(init: fn(emitter) {
      process.send(emitter_probe, emitter)
      Ok(0)
    })
    |> source.on_demand(fn(counter, demand) {
      source.emit(count_up(counter, demand), counter + demand)
    })
    |> source.buffer_unbounded()
    |> source.start()
  let assert Ok(collector) = lockstep_sink(batch_probe) |> sink.start()
  let assert Ok(emitter) = process.receive(emitter_probe, 1000)
  process.unlink(hybrid.pid)
  let source_monitor = process.monitor(hybrid.pid)

  let assert Ok(_) =
    sluice.subscription(to: hybrid.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  // The on_demand handler supplies the first demand.
  let assert Ok(#(first_batch, step)) = process.receive(batch_probe, 1000)
  assert list.first(first_batch) == Ok(0)

  // Each process can stop the source through the emitter.
  source.finish(emitter)
  process.send(step, Nil)
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(source_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(down_selector, 2000)

  shutdown(collector)
}

pub fn finite_source_stops_and_permanent_sink_follows_test() {
  let batch_probe = process.new_subject()
  // This source emits 0..9 one time. Then it stops with the normal
  // reason.
  let assert Ok(finite) =
    source.new(init: 0, on_demand: fn(emitted, demand) {
      case emitted >= 10 {
        True -> source.stop()
        False -> {
          let count = int.min(demand, 10 - emitted)
          source.emit(count_up(emitted, count), emitted + count)
        }
      }
    })
    |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(_state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(Nil)
    })
    |> sink.start()
  let sink_monitor = process.monitor(collector.pid)
  process.unlink(collector.pid)
  process.unlink(finite.pid)

  let assert Ok(_) =
    sluice.subscription(to: finite.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(first) = process.receive(batch_probe, 1000)
  assert list.first(first) == Ok(0)

  // When the source stops, the Permanent subscription stops the sink
  // with the normal reason.
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(sink_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(down_selector, 2000)
}

pub fn subscribe_validation_test() {
  let assert Ok(counter) =
    counter_source(process.new_subject()) |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.start()

  // The check refuses demand configurations that are not correct,
  // before messages move.
  assert sluice.subscription(to: counter.data)
    |> sluice.min_demand(10)
    |> sluice.max_demand(10)
    |> sluice.subscribe(consumer: collector.data)
    == Error(sluice.InvalidDemand(10, 10))

  // The first subscription is successful. The check refuses a second
  // subscription to the same producer.
  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.subscribe(consumer: collector.data)
  assert sluice.subscription(to: counter.data)
    |> sluice.subscribe(consumer: collector.data)
    == Error(sluice.AlreadySubscribed)

  shutdown(collector)
  shutdown(counter)
}

pub fn subscribe_to_dead_stages_test() {
  let assert Ok(counter) =
    counter_source(process.new_subject()) |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.start()

  shutdown(counter)
  assert sluice.subscription(to: counter.data)
    |> sluice.subscribe(consumer: collector.data)
    == Error(sluice.ProducerNotAlive)

  shutdown(collector)
  let assert Ok(replacement) =
    counter_source(process.new_subject()) |> source.start()
  assert sluice.subscription(to: replacement.data)
    |> sluice.subscribe(consumer: collector.data)
    == Error(sluice.ConsumerNotAlive)

  shutdown(replacement)
}
