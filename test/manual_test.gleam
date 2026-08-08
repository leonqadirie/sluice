//// Tests for the manual demand mode: no events without an ask, exact
//// quantities per ask, and self-paced asks from inside a callback.

import gleam/erlang/process
import sluice
import sluice/sink
import sluice/source
import support

pub fn manual_subscription_moves_only_on_ask_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
    })
    |> sink.start()

  let assert Ok(subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: collector.data)

  // Without an ask, no events come.
  assert process.receive(batch_probe, 150) == Error(Nil)

  // Each ask supplies exactly the asked quantity, in sequence.
  sluice.ask(subscription:, count: 4)
  let assert Ok([0, 1, 2, 3]) = process.receive(batch_probe, 1000)
  assert process.receive(batch_probe, 100) == Error(Nil)

  sluice.ask(subscription:, count: 2)
  let assert Ok([4, 5]) = process.receive(batch_probe, 1000)
  assert process.receive(batch_probe, 100) == Error(Nil)

  support.shutdown(collector)
  support.shutdown(counter)
}

// A sink can control its own speed: it asks for the subsequent batch from
// inside the callback.
pub fn manual_ask_from_inside_callback_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, subscription) {
      process.send(batch_probe, events)
      sluice.ask(subscription:, count: 2)
      sink.continue(state)
    })
    |> sink.start()

  let assert Ok(subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: collector.data)

  // One external ask starts the flow. The callback continues it.
  sluice.ask(subscription:, count: 2)
  let assert Ok([0, 1]) = process.receive(batch_probe, 1000)
  let assert Ok([2, 3]) = process.receive(batch_probe, 1000)
  let assert Ok([4, 5]) = process.receive(batch_probe, 1000)

  support.shutdown(collector)
  support.shutdown(counter)
}

pub fn ask_with_zero_or_negative_count_is_ignored_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
    })
    |> sink.start()

  let assert Ok(subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: collector.data)

  sluice.ask(subscription:, count: 0)
  sluice.ask(subscription:, count: -3)
  assert process.receive(batch_probe, 150) == Error(Nil)

  support.shutdown(collector)
  support.shutdown(counter)
}
