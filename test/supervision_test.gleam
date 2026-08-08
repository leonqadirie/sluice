//// Tests for stages that have names, for declared subscriptions, and for
//// pipelines under a supervisor.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor.{type Started}
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision
import sluice
import sluice/gate
import sluice/internal/platform
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

fn lockstep_sink(batch_probe: Subject(#(List(Int), Subject(Nil)))) {
  sink.new(init: Nil, on_events: fn(_state, events, _subscription) {
    let step = process.new_subject()
    process.send(batch_probe, #(events, step))
    let assert Ok(Nil) = process.receive(step, 10_000)
    sink.continue(Nil)
  })
}

fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

fn small_demand(options: sluice.SubscriptionOptions(event)) {
  options |> sluice.min_demand(2) |> sluice.max_demand(6)
}

pub fn named_gate_pipeline_flows_test() {
  let batch_probe = process.new_subject()
  let counter_name = source.new_name("supervised_counter")
  let doubler_name = gate.new_name("supervised_doubler")

  let assert Ok(counter) =
    counter_source() |> source.named(counter_name) |> source.start()
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(list.map(events, fn(event) { event * 2 }), state)
    })
    |> gate.named(doubler_name)
    |> gate.start()
  assert gate.whereis(doubler_name) == Ok(doubler.pid)
  let assert Ok(collector) =
    lockstep_sink(batch_probe)
    |> sink.subscribe(
      sluice.subscription(to: gate.outlet_of(doubler_name)) |> small_demand,
    )
    |> sink.start()
  let assert Ok(_) =
    sluice.subscription(to: source.outlet_of(counter_name))
    |> small_demand
    |> sluice.subscribe(consumer: gate.inlet_of(doubler_name))

  // Registered names alone connected the full chain.
  let assert Ok(#(first_batch, step)) = process.receive(batch_probe, 1000)
  assert list.first(first_batch) == Ok(0)
  assert list.all(first_batch, fn(event) { event % 2 == 0 })
  process.send(step, Nil)

  shutdown(collector)
  shutdown(doubler)
  shutdown(counter)
}

pub fn named_sink_reachable_by_name_test() {
  let batch_probe = process.new_subject()
  let collector_name = sink.new_name("named_collector")
  let assert Ok(collector) =
    lockstep_sink(batch_probe) |> sink.named(collector_name) |> sink.start()
  let assert Ok(counter) = counter_source() |> source.start()
  assert sink.whereis(collector_name) == Ok(collector.pid)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> small_demand
    |> sluice.subscribe(consumer: sink.inlet_of(collector_name))

  let assert Ok(#(_events, step)) = process.receive(batch_probe, 1000)
  process.send(step, Nil)

  shutdown(collector)
  shutdown(counter)
}

pub fn duplicate_name_fails_to_start_test() {
  let name = source.new_name("unique_counter")
  let assert Ok(counter) =
    counter_source() |> source.named(name) |> source.start()

  let assert Error(actor.InitFailed(_)) =
    counter_source() |> source.named(name) |> source.start()

  shutdown(counter)
}

pub fn declared_subscription_to_missing_producer_fails_start_test() {
  let ghost_name = source.new_name("ghost")
  let assert Error(actor.InitFailed(_)) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.subscribe(sluice.subscription(to: source.outlet_of(ghost_name)))
    |> sink.start()
}

pub fn supervised_pipeline_restarts_and_reconnects_test() {
  let batch_probe = process.new_subject()
  let counter_name = source.new_name("restarting_counter")
  let doubler_name = gate.new_name("restarting_doubler")

  let assert Ok(supervised) =
    supervisor.new(supervisor.RestForOne)
    |> supervisor.add(
      supervision.worker(fn() {
        counter_source() |> source.named(counter_name) |> source.start()
      }),
    )
    |> supervisor.add(
      supervision.worker(fn() {
        gate.new(init: Nil, on_events: fn(state, events, _subscription) {
          gate.emit(list.map(events, fn(event) { event * 2 }), state)
        })
        |> gate.named(doubler_name)
        |> gate.subscribe(
          sluice.subscription(to: source.outlet_of(counter_name))
          |> small_demand,
        )
        |> gate.start()
      }),
    )
    |> supervisor.add(
      supervision.worker(fn() {
        lockstep_sink(batch_probe)
        |> sink.subscribe(
          sluice.subscription(to: gate.outlet_of(doubler_name))
          |> small_demand,
        )
        |> sink.start()
      }),
    )
    |> supervisor.start()

  // Events move through the supervised pipeline.
  let assert Ok(#(first_batch, _step)) = process.receive(batch_probe, 1000)
  assert list.first(first_batch) == Ok(0)

  // Kill the source. RestForOne restarts the source and the subsequent
  // stages. The declared subscriptions make the connections again through
  // the names. The new counter starts at zero. This shows that these are
  // new processes.
  let assert Ok(source_pid) = source.whereis(counter_name)
  process.kill(source_pid)
  let assert Ok(#(fresh_batch, _step)) = process.receive(batch_probe, 2000)
  assert list.first(fresh_batch) == Ok(0)

  shutdown(supervised)
}

// A send to a name that has no process must not stop the sender.
pub fn send_to_unregistered_name_is_safe_test() {
  let name = process.new_name("no_process_here")
  platform.send_named(name, "lost")

  // A subscription to an outlet with an unregistered name fails with a
  // clear error, without a panic.
  let unregistered = source.new_name("also_no_process")
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.start_timeout(milliseconds: 2000)
    |> sink.start()
  assert sluice.subscription(to: source.outlet_of(unregistered))
    |> sluice.subscribe(consumer: collector.data)
    == Error(sluice.ProducerNotAlive)

  shutdown(collector)
}
