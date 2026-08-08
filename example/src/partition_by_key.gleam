//// Events divided across sinks by a key.
////
//// The partition dispatcher gives each event a partition through the
//// `by` function. Each partition has a maximum of one subscriber, and
//// each event goes only to the subscriber of its partition. Events for
//// a partition without open demand wait in the queue of that
//// partition. Thus a slow partition does not stop the other
//// partitions.
////
//// Here orders with an even number go to partition 0, and the other
//// orders go to partition 1. The sink of partition 1 is much slower.
//// Watch the output: partition 0 completes early, while partition 1
//// continues at its own speed.
////
//// Run this example from the `example` directory:
////
////     gleam run --module partition_by_key

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

  // The `by` function selects a partition from 0 to `count` - 1 for
  // each order.
  let assert Ok(orders) =
    source.new(init: 0, on_demand: fn(next_order, demand) {
      source.emit(count_up(next_order, demand), next_order + demand)
    })
    |> source.dispatcher(
      dispatcher.partition(count: 2, by: fn(order) { order % 2 }),
    )
    |> source.start()

  // One sink for each partition. Each subscription selects its
  // partition with `sluice.partition`. The sink of partition 1 waits
  // three times as long for each batch.
  let assert Ok(fast_worker) =
    partition_sink(label: "partition 0", wait: 100, target: 8, done:)
    |> sink.start()
  let assert Ok(_) =
    sluice.subscription(to: orders.data)
    |> sluice.min_demand(1)
    |> sluice.max_demand(4)
    |> sluice.partition(index: 0)
    |> sluice.subscribe(consumer: fast_worker.data)

  let assert Ok(slow_worker) =
    partition_sink(label: "partition 1", wait: 300, target: 8, done:)
    |> sink.start()
  let assert Ok(_) =
    sluice.subscription(to: orders.data)
    |> sluice.min_demand(1)
    |> sluice.max_demand(4)
    |> sluice.partition(index: 1)
    |> sluice.subscribe(consumer: slow_worker.data)

  let assert Ok(first) = process.receive(done, 30_000)
  let assert Ok(second) = process.receive(done, 30_000)
  io.println(
    "The sink of "
    <> first
    <> " and the sink of "
    <> second
    <> " reached their targets. The example ends.",
  )
}

/// A sink that prints each order with a label and waits after each
/// batch. It counts the orders, and it stops itself at the target.
fn partition_sink(
  label label: String,
  wait wait: Int,
  target target: Int,
  done done: Subject(String),
) -> sink.Builder(Int, Int) {
  sink.new(init: 0, on_events: fn(count, orders, _subscription) {
    list.each(orders, fn(order) {
      io.println("[" <> label <> "] order " <> int.to_string(order))
    })
    process.sleep(wait)
    let count = count + list.length(orders)
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
