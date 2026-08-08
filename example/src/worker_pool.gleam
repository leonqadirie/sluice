//// Events processed in parallel worker processes.
////
//// `pool.sink` makes a sink that runs each event in a separate worker
//// process, with a limit on the quantity of parallel workers. The
//// demand follows the workers: when a worker completes, the pool asks
//// for one more event. Thus the backpressure of the pipeline follows
//// the true work speed.
////
//// Twelve orders arrive, and a maximum of four couriers work at the
//// same time. Each order takes a different time. Watch the output: the
//// completion sequence differs from the start sequence, and a slow
//// order does not block the other couriers.
////
//// Run this example from the `example` directory:
////
////     gleam run --module worker_pool

import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/yielder
import sluice
import sluice/pool
import sluice/sink
import sluice/source

pub fn main() -> Nil {
  // Twelve orders from a yielder. The source stops with the normal
  // reason at the end of the yielder.
  let assert Ok(orders) =
    source.from_yielder(yielder.range(1, 12))
    |> source.start()

  // A maximum of four couriers run at the same time. Each courier
  // handles one order and then asks for a replacement.
  let assert Ok(deliveries) =
    pool.sink(concurrency: 4, run: fn(order) {
      io.println("[courier] delivers order " <> int.to_string(order))
      process.sleep(100 * { order % 3 + 1 })
      io.println("[courier] completed order " <> int.to_string(order))
    })
    |> sink.start()

  // The pool controls its own asks: subscribe it with the `Manual`
  // demand mode.
  let assert Ok(_) =
    sluice.subscription(to: orders.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: deliveries.data)

  // When the source stops, the subscription of the pool ends, and the
  // pool stops also. A monitor reports that stop to the main process.
  let monitor = process.monitor(deliveries.pid)
  let assert Ok(_down) =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
    |> process.selector_receive(10_000)

  // The couriers are separate processes without a link to the pool.
  // The last couriers can still work here: give them a moment.
  process.sleep(500)
  io.println("All orders were delivered. The example ends.")
}
