//// A source that receives pushed events, and a buffer that overflows.
////
//// Some events do not wait for demand: they come from a socket, from a
//// timer, or from a different process. `source.new_with_emitter` gives
//// an `Emitter` to the initialiser, and each process can push events
//// through it. Pushed events go out immediately, up to the open
//// demand. The buffer keeps the remaining events.
////
//// Here the pusher is much faster than the sink. The buffer holds a
//// maximum of 10 readings, and the default `KeepLast` policy discards
//// the oldest readings first. The `on_discard` hook reports each
//// discard. Watch the output: the sink receives the first readings and
//// the last readings, with a gap between them.
////
//// Run this example from the `example` directory:
////
////     gleam run --module push_and_buffer

import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/string
import sluice
import sluice/sink
import sluice/source

pub fn main() -> Nil {
  // The pusher process sends 50 readings in five fast bursts. Then it
  // waits, so the sink can drain the buffer, and stops the source with
  // `finish`. A stop through `finish` does not drain the buffer, so
  // only finish when the flow is complete.
  let assert Ok(readings) =
    source.new_with_emitter(init: fn(emitter) {
      process.spawn(fn() {
        list.each(count_up(0, 5), fn(burst) {
          process.sleep(100)
          let first_reading = burst * 10 + 1
          source.push(emitter, count_up(first_reading, 10))
        })
        process.sleep(3000)
        source.finish(emitter)
      })
      Ok(Nil)
    })
    |> source.buffer_capacity(events: 10)
    |> source.on_discard(fn(state, count) {
      io.println(
        "[source] the buffer discarded " <> int.to_string(count) <> " readings",
      )
      state
    })
    |> source.start()

  // The sink is slow: it processes a small batch and then waits. When
  // the source stops, the subscription of the sink ends, and the
  // `on_cancelled` hook reports the end to the main process.
  let done = process.new_subject()
  let assert Ok(printer) =
    sink.new(init: Nil, on_events: fn(state, batch, _subscription) {
      let numbers = list.map(batch, int.to_string) |> string.join(", ")
      io.println("[sink]   received " <> numbers)
      process.sleep(200)
      sink.continue(state)
    })
    |> sink.on_cancelled(fn(_state, _end) {
      process.send(done, Nil)
      sink.stop()
    })
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: readings.data)
    |> sluice.min_demand(1)
    |> sluice.max_demand(5)
    |> sluice.subscribe(consumer: printer.data)

  let assert Ok(Nil) = process.receive(done, 30_000)
  io.println("The source finished. The example ends.")
}

/// The list of `count` integers that starts at `from`.
fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}
