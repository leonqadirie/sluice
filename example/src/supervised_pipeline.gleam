//// A pipeline that a supervisor restarts.
////
//// Stages start as standard OTP actors, so a `static_supervisor` can
//// hold them as children. The stages have names, and the sink declares
//// its subscription on the builder. Thus the connection continues
//// through restarts: after a restart, the new sink finds the new
//// source through the name.
////
//// The example kills the source while the pipeline runs. `RestForOne`
//// restarts the source and then the subsequent children. Watch the
//// output: after the kill, the count starts at zero again, because
//// these are new processes with new state. The OTP logger also writes
//// a supervisor report for the kill; this is expected.
////
//// Run this example from the `example` directory:
////
////     gleam run --module supervised_pipeline

import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision
import sluice
import sluice/sink
import sluice/source

pub fn main() -> Nil {
  let counter_name = source.new_name(prefix: "counter")

  let assert Ok(_supervised) =
    supervisor.new(supervisor.RestForOne)
    |> supervisor.add(
      supervision.worker(fn() {
        source.new(init: 0, on_demand: fn(next_number, demand) {
          source.emit(count_up(next_number, demand), next_number + demand)
        })
        |> source.named(counter_name)
        |> source.start()
      }),
    )
    |> supervisor.add(
      supervision.worker(fn() {
        sink.new(init: Nil, on_events: fn(state, numbers, _subscription) {
          list.each(numbers, fn(number) {
            io.println("[sink] " <> int.to_string(number))
          })
          process.sleep(250)
          sink.continue(state)
        })
        |> sink.subscribe(
          sluice.subscription(to: source.outlet_of(counter_name))
          |> sluice.min_demand(2)
          |> sluice.max_demand(5),
        )
        |> sink.start()
      }),
    )
    |> supervisor.start()

  // Let the pipeline run for a moment.
  process.sleep(1500)

  // Kill the source. The sink stops also, because its subscription is
  // `Permanent`. The supervisor restarts the source and then the sink,
  // in that sequence, and the declared subscription connects the new
  // processes through the name.
  io.println("-- kill the source --")
  let assert Ok(source_pid) = source.whereis(counter_name)
  process.kill(source_pid)

  process.sleep(1500)
  io.println("The supervisor restarted the pipeline. The example ends.")
}

/// The list of `count` integers that starts at `from`.
fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}
