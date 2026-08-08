//// A pooled sink. It runs each event in a separate worker process, with a
//// limit on the quantity of parallel workers. Demand follows the workers:
//// each worker asks for one more event through the subscription when it
//// completes. Thus the backpressure of the pipeline follows the true work
//// speed.
////
//// The limit applies for each subscription of the pool. A failure in a
//// worker does not stop the pool. The pool writes a warning to the log
//// and continues.
////
//// Use `pool.sink` in the place of `sink.new`, and set the demand mode of
//// the subscription to `Manual`. The pool makes its own asks. Do not
//// change the subscription hooks of a pool sink.

import gleam/erlang/process
import gleam/int
import gleam/list
import sluice.{type Subscription}
import sluice/internal/platform
import sluice/sink

/// The internal state of a pool sink.
pub opaque type State {
  State(concurrency: Int)
}

/// Define a pooled sink: a maximum of `concurrency` workers run at the
/// same time, and each worker runs `run` for one event. The result is a
/// normal sink builder: give it a name or start it directly, and
/// subscribe it with the `Manual` demand mode.
///
/// * `concurrency`: The maximum quantity of parallel workers for each
///   subscription.
/// * `run`: The function that one worker runs for one event.
pub fn sink(
  concurrency concurrency: Int,
  run run: fn(event) -> Nil,
) -> sink.Builder(State, event) {
  let concurrency = int.max(concurrency, 1)
  sink.new(
    init: State(concurrency:),
    on_events: fn(state, events, subscription) {
      list.each(events, fn(event) { start_worker(event, subscription, run) })
      sink.continue(state)
    },
  )
  |> sink.on_subscribed(fn(state, subscription) {
    // Fill the pool: one ask for each worker place.
    sluice.ask(subscription:, count: state.concurrency)
    sink.continue(state)
  })
}

fn start_worker(
  event: event,
  subscription: Subscription,
  run: fn(event) -> Nil,
) -> process.Pid {
  process.spawn_unlinked(fn() {
    case platform.safely(fn() { run(event) }) {
      True -> Nil
      False -> platform.log_warning("sluice: a pool worker failed")
    }
    // The worker itself asks for its replacement event. An ask is a plain
    // message send, so any process can make it.
    sluice.ask(subscription:, count: 1)
  })
}
