//// Tests for the accumulation of demand on a source.

import gleam/erlang/process
import sluice
import sluice/sink
import sluice/source
import support

// A source in the accumulation mode holds the asks. The forward call
// releases them, and the source receives the collected demand in one
// piece.
pub fn accumulated_demand_moves_on_forward_test() {
  let demand_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      process.send(demand_probe, demand)
      source.emit(support.count_up(counter, demand), counter + demand)
    })
    |> source.accumulate_demand()
    |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
    })
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  // The subscription stands, but no demand reaches the handler and no
  // events move.
  assert process.receive(demand_probe, 150) == Error(Nil)
  assert process.receive(batch_probe, 100) == Error(Nil)

  // The forward call releases the held ask, and the events move.
  sluice.forward_demand(outlet: counter.data)
  let assert Ok(6) = process.receive(demand_probe, 1000)
  let assert Ok([0, 1, 2, 3]) = process.receive(batch_probe, 1000)

  support.shutdown(collector)
  support.shutdown(counter)
}

// A second forward call changes nothing: the source is already in the
// forwarding mode.
pub fn forward_is_idempotent_test() {
  let demand_probe = process.new_subject()
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      process.send(demand_probe, demand)
      source.emit(support.count_up(counter, demand), counter + demand)
    })
    |> source.accumulate_demand()
    |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(6)
    |> sluice.subscribe(consumer: collector.data)

  sluice.forward_demand(outlet: counter.data)
  let assert Ok(6) = process.receive(demand_probe, 1000)
  sluice.forward_demand(outlet: counter.data)

  support.shutdown(collector)
  support.shutdown(counter)
}
