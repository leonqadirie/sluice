//// Tests for sources from yielders and for pipeline folds.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/otp/actor.{type Started}
import gleam/yielder
import sluice
import sluice/sink
import sluice/source

fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}

fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

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

  shutdown(collector)
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

  shutdown(collector)
}

pub fn fold_returns_the_final_value_test() {
  let assert Ok(numbers) =
    source.from_yielder(yielder.from_list(count_up(1, 100)))
    |> source.start()
  process.unlink(numbers.pid)

  assert sink.fold(from: numbers.data, initial: 0, with: int.add, within: 5000)
    == Ok(5050)
}

pub fn fold_reports_a_timeout_test() {
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      source.emit(count_up(counter, demand), counter + demand)
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

  shutdown(counter)
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
