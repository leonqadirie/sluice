//// Tests for Gate stages: the change of events, the typed chain that the
//// compiler examines, and the demand connection between the two faces of
//// a gate.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import sluice
import sluice/gate
import sluice/sink
import sluice/source
import support

// The primary type-safety property: events from an Int source go through
// an Int -> String gate into a String sink. The compilation of this test
// is the important result. The content check shows that the gate changes
// the events in sequence.
pub fn heterogeneous_typed_chain_test() {
  let batch_probe: Subject(#(List(String), Subject(Nil))) =
    process.new_subject()
  let assert Ok(counter) =
    support.counter_source_with_probe(process.new_subject()) |> source.start()
  let assert Ok(stringifier) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(list.map(events, int.to_string), state)
    })
    |> gate.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(_state, events, _subscription) {
      let step = process.new_subject()
      process.send(batch_probe, #(events, step))
      let assert Ok(Nil) = process.receive(step, 10_000)
      sink.continue(Nil)
    })
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: gate.inlet(stringifier.data))
  let assert Ok(_) =
    sluice.subscription(to: gate.outlet(stringifier.data))
    |> support.small_demand
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(#(first_batch, step)) = process.receive(batch_probe, 1000)
  assert list.first(first_batch) == Ok("0")
  assert first_batch
    == list.map(support.count_up(0, list.length(first_batch)), int.to_string)
  process.send(step, Nil)

  support.shutdown(collector)
  support.shutdown(stringifier)
  support.shutdown(counter)
}

// A gate can be a filter. It then emits fewer events than it receives.
// The pipeline does not stop. Demand continues although the gate discards
// many events.
pub fn filtering_gate_keeps_pipeline_moving_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) =
    support.counter_source_with_probe(process.new_subject()) |> source.start()
  let assert Ok(evens_only) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(list.filter(events, fn(event) { event % 2 == 0 }), state)
    })
    |> gate.buffer_capacity(events: 1000)
    |> gate.buffer_keep(sluice.KeepLast)
    |> gate.start()
  let assert Ok(collector) =
    sink.new(init: 0, on_events: fn(total, events, _subscription) {
      let total = total + list.length(events)
      process.send(batch_probe, #(events, total))
      case total >= 10 {
        True -> sink.stop()
        False -> sink.continue(total)
      }
    })
    |> sink.start()
  process.unlink(collector.pid)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: gate.inlet(evens_only.data))
  let assert Ok(_) =
    sluice.subscription(to: gate.outlet(evens_only.data))
    |> support.small_demand
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok(#(first_batch, _total)) = process.receive(batch_probe, 1000)
  assert list.all(first_batch, fn(event) { event % 2 == 0 })
  assert list.first(first_batch) == Ok(0)

  support.shutdown(evens_only)
  support.shutdown(counter)
}

// The demand connection: the sink does not continue, and the gate buffer
// contains events. In this condition, the gate does not ask the source
// for more events.
pub fn gate_holds_upstream_asks_while_downstream_idle_test() {
  let demand_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(counter) =
    support.counter_source_with_probe(demand_probe) |> source.start()
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(list.map(events, fn(event) { event * 2 }), state)
    })
    |> gate.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(_state, events, _subscription) {
      let step = process.new_subject()
      process.send(batch_probe, #(events, step))
      let assert Ok(Nil) = process.receive(step, 10_000)
      sink.continue(Nil)
    })
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: gate.inlet(doubler.data))
  let assert Ok(_) =
    sluice.subscription(to: gate.outlet(doubler.data))
    |> support.small_demand
    |> sluice.subscribe(consumer: collector.data)

  // Receive the first source demand while the sink stays on its first
  // batch.
  let assert Ok(_) = process.receive(demand_probe, 1000)
  let assert Ok(#(_first_batch, step)) = process.receive(batch_probe, 1000)
  let assert Ok(_) = process.receive(demand_probe, 1000)

  // The gate buffer now contains events that no subsequent stage asks
  // for. The source must not receive more asks.
  assert process.receive(demand_probe, 150) == Error(Nil)

  // A step of the sink starts the demand again, up to the source.
  process.send(step, Nil)
  let assert Ok(#(_second_batch, _step)) = process.receive(batch_probe, 1000)
  let assert Ok(_) = process.receive(demand_probe, 1000)

  support.shutdown(collector)
  support.shutdown(doubler)
  support.shutdown(counter)
}

// A gate that is complete stops with the normal reason. A gate that has
// a failure stops with a failure reason. Monitors show the two results.
pub fn gate_stop_decisions_propagate_test() {
  let assert Ok(counter) =
    support.counter_source_with_probe(process.new_subject()) |> source.start()
  let assert Ok(closer) =
    gate.new(init: Nil, on_events: fn(_state, _events, _subscription) {
      gate.stop()
    })
    |> gate.start()
  process.unlink(counter.pid)
  process.unlink(closer.pid)
  let closer_monitor = process.monitor(closer.pid)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: gate.inlet(closer.data))
  let closer_selector =
    process.new_selector()
    |> process.select_specific_monitor(closer_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(closer_selector, 2000)

  let assert Ok(aborter) =
    gate.new(init: Nil, on_events: fn(_state, _events, _subscription) {
      gate.stop_abnormal("boom")
    })
    |> gate.start()
  process.unlink(aborter.pid)
  let aborter_monitor = process.monitor(aborter.pid)
  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: gate.inlet(aborter.data))
  let aborter_selector =
    process.new_selector()
    |> process.select_specific_monitor(aborter_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Abnormal(_))) =
    process.selector_receive(aborter_selector, 2000)

  support.shutdown(counter)
}

pub fn gate_cannot_subscribe_to_itself_test() {
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(events, state)
    })
    |> gate.start()

  assert sluice.subscription(to: gate.outlet(doubler.data))
    |> sluice.subscribe(consumer: gate.inlet(doubler.data))
    == Error(sluice.SelfSubscription)

  support.shutdown(doubler)
}

// A gate that makes more events than the demand after it, with a buffer
// limit of zero, must report the discarded events.
pub fn gate_reports_discarded_events_test() {
  let batch_probe = process.new_subject()
  let discard_probe = process.new_subject()
  let assert Ok(finite) =
    source.new(init: False, on_demand: fn(exhausted, _demand) {
      case exhausted {
        True -> source.emit([], True)
        False -> source.emit(support.count_up(1, 6), True)
      }
    })
    |> source.start()
  let assert Ok(expander) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(
        list.flat_map(events, fn(event) { [event, event, event] }),
        state,
      )
    })
    |> gate.buffer_capacity(events: 0)
    |> gate.start_timeout(milliseconds: 2000)
    |> gate.on_discard(fn(state, count) {
      process.send(discard_probe, count)
      state
    })
    |> gate.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(_state, events, _subscription) {
      let step = process.new_subject()
      process.send(batch_probe, #(events, step))
      let assert Ok(Nil) = process.receive(step, 10_000)
      sink.continue(Nil)
    })
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: gate.outlet(expander.data))
    |> support.small_demand
    |> sluice.subscribe(consumer: collector.data)
  let assert Ok(_) =
    sluice.subscription(to: finite.data)
    |> support.small_demand
    |> sluice.subscribe(consumer: gate.inlet(expander.data))

  // The gate changes 4 events into 12. The sink asked for 6. The buffer
  // limit is 0. Thus the gate discards 6 events and reports them.
  let assert Ok(first_discard) = process.receive(discard_probe, 1000)
  assert first_discard > 0

  support.shutdown(collector)
  support.shutdown(expander)
  support.shutdown(finite)
}
