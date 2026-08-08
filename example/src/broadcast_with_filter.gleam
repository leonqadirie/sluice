//// One flow of events to many sinks, with a filter.
////
//// The default dispatcher gives each event to exactly one subscriber.
//// The broadcast dispatcher gives each event to every subscriber
//// instead. A subscriber can set a filter with `sluice.selector`; it
//// then receives only the events that the filter keeps.
////
//// The `audit` sink receives every number. The `evens` sink receives
//// only the even numbers. The source accumulates its demand until both
//// sinks are connected. Thus no sink misses the first events.
////
//// Run this example from the `example` directory:
////
////     gleam run --module broadcast_with_filter

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/io
import gleam/list
import sluice
import sluice/dispatcher
import sluice/sink
import sluice/source

pub fn main() -> Nil {
  let done = process.new_subject()

  // The source counts upward. `accumulate_demand` holds the incoming
  // asks: the source makes no events until `forward_demand` releases
  // the asks.
  let assert Ok(numbers) =
    source.new(init: 0, on_demand: fn(next_number, demand) {
      source.emit(count_up(next_number, demand), next_number + demand)
    })
    |> source.dispatcher(dispatcher.broadcast())
    |> source.accumulate_demand()
    |> source.start()

  // The audit sink receives every number.
  let assert Ok(audit) =
    counting_sink(label: "audit", target: 20, done:) |> sink.start()
  let assert Ok(_) =
    sluice.subscription(to: numbers.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(5)
    |> sluice.subscribe(consumer: audit.data)

  // The evens sink sets a filter on its subscription. The broadcast
  // dispatcher then sends it only the numbers that the filter keeps.
  let assert Ok(evens) =
    counting_sink(label: "evens", target: 10, done:) |> sink.start()
  let assert Ok(_) =
    sluice.subscription(to: numbers.data)
    |> sluice.min_demand(2)
    |> sluice.max_demand(5)
    |> sluice.selector(keep: fn(number) { number % 2 == 0 })
    |> sluice.subscribe(consumer: evens.data)

  // Both sinks are connected. Release the held demand: the events
  // start to move.
  sluice.forward_demand(outlet: numbers.data)

  let assert Ok(first) = process.receive(done, 30_000)
  let assert Ok(second) = process.receive(done, 30_000)
  io.println(
    "The "
    <> first
    <> " sink and the "
    <> second
    <> " sink reached their targets. The example ends.",
  )
}

/// A sink that prints each number with a label. It counts the numbers,
/// and it stops itself at the target.
fn counting_sink(
  label label: String,
  target target: Int,
  done done: Subject(String),
) -> sink.Builder(Int, Int) {
  sink.new(init: 0, on_events: fn(count, numbers, _subscription) {
    list.each(numbers, fn(number) {
      io.println("[" <> label <> "] " <> int.to_string(number))
    })
    process.sleep(100)
    let count = count + list.length(numbers)
    case count >= target {
      True -> {
        process.send(done, label)
        sink.stop()
      }
      False -> sink.continue(count)
    }
  })
}

/// The list of `count` integers that starts at `from`.
fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}
