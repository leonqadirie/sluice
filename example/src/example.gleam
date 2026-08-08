//// The index of the examples. Each example is a module with a `main`
//// function. Run an example with `gleam run --module <name>`.

import gleam/io
import gleam/list

pub fn main() -> Nil {
  io.println("Run an example with: gleam run --module <name>")
  io.println("")
  list.each(
    [
      #("counter_pipeline", "The basic pipeline: source, gate, and sink."),
      #("push_and_buffer", "Pushed events, and a buffer that overflows."),
      #("broadcast_with_filter", "One event flow to many sinks, with a filter."),
      #("partition_by_key", "Events divided across sinks by a key."),
      #("worker_pool", "Events processed in parallel worker processes."),
      #("event_workers", "A supervised child process for each event."),
      #("supervised_pipeline", "A pipeline that a supervisor restarts."),
    ],
    fn(entry) {
      let #(name, description) = entry
      io.println("  " <> name <> " - " <> description)
    },
  )
}
