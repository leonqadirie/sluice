//// A supervised child process for each event.
////
//// `consumer_supervisor` starts one linked OTP child for each event,
//// like a supervisor with one child specification. `max_demand` limits
//// the quantity of children that run at the same time. A slot
//// becomes free only when its child terminates, so the demand follows
//// the true work speed. `pool.sink` is similar but lighter: it gives no
//// restart policy and no child inspection.
////
//// Run this example from the `example` directory:
////
////     gleam run --module event_workers

import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/yielder
import sluice
import sluice/consumer_supervisor
import sluice/source

pub fn main() -> Nil {
  let completed = process.new_subject()

  // Eight orders from a yielder.
  let assert Ok(orders) =
    source.from_yielder(yielder.range(1, 8))
    |> source.start()

  // The start function receives one order and starts one linked child
  // for it, as a normal OTP child start function does. The start must
  // return quickly; the child does the long work after it starts.
  let assert Ok(shipments) =
    consumer_supervisor.new(fn(order) {
      let pid =
        process.spawn(fn() {
          io.println("[child] packs order " <> int.to_string(order))
          process.sleep(200)
          io.println("[child] shipped order " <> int.to_string(order))
          process.send(completed, order)
        })
      Ok(actor.Started(pid:, data: Nil))
    })
    |> consumer_supervisor.start()

  // `max_demand` is the limit of parallel children: here a maximum of
  // three. The `Temporary` cancel mode lets the supervisor continue
  // when the source stops. Thus the children that still run can
  // complete their work.
  let assert Ok(_) =
    sluice.subscription(to: orders.data)
    |> sluice.min_demand(1)
    |> sluice.max_demand(3)
    |> sluice.cancel_mode(sluice.Temporary)
    |> sluice.subscribe(consumer: consumer_supervisor.inlet(shipments.data))

  // The supervisor can report its children.
  process.sleep(100)
  let assert Ok(consumer_supervisor.ChildrenCount(active:, ..)) =
    consumer_supervisor.count_children(shipments.data)
  io.println("[main]  " <> int.to_string(active) <> " children are active")

  // Wait for the completion message of each order.
  list.each(count_up(1, 8), fn(_) {
    let assert Ok(_order) = process.receive(completed, 5000)
  })
  io.println("All orders were shipped. The example ends.")
}

/// The list of `count` integers that starts at `from`.
fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}
