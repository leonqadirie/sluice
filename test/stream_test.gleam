//// Tests for sources from yielders and for pipeline folds.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/yielder
import sluice
import sluice/sink
import sluice/source
import support

// A yielder source delivers the full sequence and then stops with the
// normal reason.
pub fn yielder_source_delivers_and_stops_test() {
  let batch_probe = process.new_subject()
  let assert Ok(numbers) =
    source.from_yielder(yielder.from_list([1, 2, 3, 4, 5]))
    |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
    })
    |> sink.on_cancelled(fn(state, _end) { sink.continue(state) })
    |> sink.start()
  process.unlink(numbers.pid)
  let source_monitor = process.monitor(numbers.pid)

  let assert Ok(_) =
    sluice.subscription(to: numbers.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  // The consumer divides the delivered events into parts of
  // max_demand - min_demand.
  let assert Ok([1, 2, 3, 4]) = process.receive(batch_probe, 1000)
  let assert Ok([5]) = process.receive(batch_probe, 1000)
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(source_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(down_selector, 2000)

  support.shutdown(collector)
}

// A source can emit its last events and stop in one step.
pub fn emit_final_delivers_and_stops_test() {
  let batch_probe = process.new_subject()
  let assert Ok(last_words) =
    source.new(init: Nil, on_demand: fn(_state, _demand) {
      source.emit_final([1, 2, 3])
    })
    |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
    })
    |> sink.on_cancelled(fn(state, _end) { sink.continue(state) })
    |> sink.start()
  process.unlink(last_words.pid)
  let source_monitor = process.monitor(last_words.pid)

  let assert Ok(_) =
    sluice.subscription(to: last_words.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  let assert Ok([1, 2, 3]) = process.receive(batch_probe, 1000)
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(source_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(down_selector, 2000)

  support.shutdown(collector)
}

// A source whose events end in the middle of a demand cannot wait for
// the next ask: after a batch that does not fill the demand, the
// consumer does not ask again. The source must end with `emit_final` in
// the same call. The full sequence arrives, and the Permanent sink
// follows the stop of the source.
pub fn emit_final_ends_a_partial_final_batch_test() {
  let batch_probe = process.new_subject()
  let total = 8
  let assert Ok(finite) =
    source.new(init: 0, on_demand: fn(emitted, demand) {
      let remaining = total - emitted
      case remaining <= demand {
        True -> source.emit_final(support.count_up(emitted, remaining))
        False ->
          source.emit(support.count_up(emitted, demand), emitted + demand)
      }
    })
    |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
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

  // The first demand of 6 is filled completely. The final two events do
  // not fill the second demand, and they arrive nevertheless.
  assert collect_batches(batch_probe, total, []) == support.count_up(0, total)

  // The source stopped, and the Permanent sink follows.
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(sink_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(down_selector, 2000)
}

fn collect_batches(
  batch_probe: process.Subject(List(Int)),
  quantity: Int,
  collected: List(Int),
) -> List(Int) {
  case list.length(collected) >= quantity {
    True -> collected
    False -> {
      let assert Ok(events) = process.receive(batch_probe, 1000)
      collect_batches(batch_probe, quantity, list.append(collected, events))
    }
  }
}

pub fn fold_returns_the_final_value_test() {
  let assert Ok(numbers) =
    source.from_yielder(yielder.from_list(support.count_up(1, 100)))
    |> source.start()
  process.unlink(numbers.pid)

  assert sink.fold(from: numbers.data, initial: 0, with: int.add, within: 5000)
    == Ok(5050)
}

pub fn fold_reports_a_timeout_test() {
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      source.emit(support.count_up(counter, demand), counter + demand)
    })
    |> source.start()

  let result =
    sink.fold(
      from: counter.data,
      initial: 0,
      with: fn(accumulated, _event) { accumulated },
      within: 100,
    )
  assert result == Error(sink.FoldTimeout)

  support.shutdown(counter)
}

pub fn fold_reports_a_producer_failure_test() {
  let assert Ok(crasher) =
    source.new(init: Nil, on_demand: fn(_state, _demand) {
      source.stop_abnormal("boom")
    })
    |> source.start()
  process.unlink(crasher.pid)

  let result =
    sink.fold(from: crasher.data, initial: 0, with: int.add, within: 5000)
  let assert Error(sink.FoldProducerFailed(_)) = result
}
