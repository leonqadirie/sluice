//// The basic pipeline: a source, a gate, and a sink.
////
//// The source counts upward without an end. The gate keeps the even
//// numbers and doubles them. The sink prints each number, slowly.
////
//// The source makes numbers only when the sink asks for more. Watch
//// the output: a new demand line appears only after the sink completes
//// a batch. The slow sink sets the speed of the full pipeline.
////
//// Run this example from the `example` directory:
////
////     gleam run --module counter_pipeline

import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import sluice
import sluice/gate
import sluice/sink
import sluice/source

/// The sink stops the example after this quantity of printed numbers.
const printed_target = 15

pub fn main() -> Nil {
  // The source holds the next number as its state. It receives each
  // demand and makes exactly that quantity of numbers. The print shows
  // when demand arrives: only when the sink has capacity again.
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(next_number, demand) {
      io.println(
        "[source] received a demand for " <> int.to_string(demand) <> " numbers",
      )
      source.emit(count_up(next_number, demand), next_number + demand)
    })
    |> source.start()

  // The gate keeps only the even numbers and doubles them. A gate can
  // change events, and it can also remove events: the quantity that
  // goes out does not need to equal the quantity that comes in.
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, numbers, _subscription) {
      let doubled =
        numbers
        |> list.filter(fn(number) { number % 2 == 0 })
        |> list.map(fn(number) { number * 2 })
      gate.emit(doubled, state)
    })
    |> gate.start()

  // The sink prints each number and then waits. The wait simulates
  // slow work. The sink counts the numbers, and it stops itself at the
  // target. The `done` subject tells the main process that the example
  // is complete.
  let done = process.new_subject()
  let assert Ok(printer) =
    sink.new(init: 0, on_events: fn(printed, numbers, _subscription) {
      list.each(numbers, fn(number) {
        io.println("[sink]   " <> int.to_string(number))
      })
      process.sleep(300)
      let printed = printed + list.length(numbers)
      case printed >= printed_target {
        True -> {
          process.send(done, Nil)
          sink.stop()
        }
        False -> sink.continue(printed)
      }
    })
    |> sink.start()

  // Connect the stages: counter -> doubler -> printer. The demand
  // values here are small, so the batches are small and the flow is
  // easy to observe. The defaults are 750 and 1000.
  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(5)
    |> sluice.subscribe(consumer: gate.inlet(doubler.data))
  let assert Ok(_) =
    sluice.subscription(to: gate.outlet(doubler.data))
    |> sluice.min_demand(2)
    |> sluice.max_demand(5)
    |> sluice.subscribe(consumer: printer.data)

  let assert Ok(Nil) = process.receive(done, 30_000)
  io.println("The sink reached its target and stopped. The example ends.")
}

/// The list of `count` integers that starts at `from`.
fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}
