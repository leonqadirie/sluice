//// The shared helpers of the test modules: event sequences, common
//// stages, subscription presets, and event collection.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor.{type Started}
import sluice
import sluice/sink
import sluice/source

/// The list of `count` integers that starts at `from`.
pub fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}

/// Disconnect a stage from the test process and stop it. Thus the stop
/// does not stop the test.
pub fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

/// A demand configuration with small batches. The tests can then count
/// the batches and the asks.
pub fn small_demand(
  options: sluice.SubscriptionOptions(event),
) -> sluice.SubscriptionOptions(event) {
  options |> sluice.min_demand(2) |> sluice.max_demand(6)
}

/// A source that counts up without an end.
pub fn counter_source() -> source.Builder(Int, Int) {
  source.new(init: 0, on_demand: fn(counter, demand) {
    source.emit(count_up(counter, demand), counter + demand)
  })
}

/// A source that counts up without an end. It sends each demand that it
/// receives to the probe.
pub fn counter_source_with_probe(
  demand_probe: Subject(Int),
) -> source.Builder(Int, Int) {
  source.new(init: 0, on_demand: fn(counter, demand) {
    process.send(demand_probe, demand)
    source.emit(count_up(counter, demand), counter + demand)
  })
}

/// A sink that sends each batch to the probe, together with a subject.
/// The test must send a reply on that subject before the sink continues.
pub fn lockstep_sink(
  batch_probe: Subject(#(List(Int), Subject(Nil))),
) -> sink.Builder(Nil, Int) {
  sink.new(init: Nil, on_events: fn(_state, events, _subscription) {
    let step = process.new_subject()
    process.send(batch_probe, #(events, step))
    let assert Ok(Nil) = process.receive(step, 10_000)
    sink.continue(Nil)
  })
}

/// A sink that ignores its events.
pub fn quiet_sink() -> sink.Builder(Nil, event) {
  sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
    sink.continue(state)
  })
}

/// A sink that sends each batch to the probe.
pub fn collector(batch_probe: Subject(List(Int))) -> sink.Builder(Nil, Int) {
  sink.new(init: Nil, on_events: fn(state, events, _subscription) {
    process.send(batch_probe, events)
    sink.continue(state)
  })
}

/// Receive batches from a `collector` probe until the quantity of
/// collected events is `target`, or until no more events come.
pub fn collect_events(
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

/// Move a lockstep sink forward until the quantity of collected events
/// is `target`, or until no more events come.
pub fn collect_lockstep_events(
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
          collect_lockstep_events(
            batch_probe,
            target,
            list.append(received, events),
          )
        }
      }
  }
}
