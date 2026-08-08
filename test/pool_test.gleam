//// Tests for the pooled sink: the worker limit, demand that follows the
//// workers, and a worker failure that does not stop the pool.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor.{type Started}
import sluice
import sluice/pool
import sluice/sink
import sluice/source

fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}

fn counter_source() {
  source.new(init: 0, on_demand: fn(counter, demand) {
    source.emit(count_up(counter, demand), counter + demand)
  })
}

fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

// Each worker offers its event to the test and waits for a release. Thus
// the test controls the completion of each worker.
fn blocking_work(offer_probe: Subject(#(Int, Subject(Nil)))) -> fn(Int) -> Nil {
  fn(event) {
    let release = process.new_subject()
    process.send(offer_probe, #(event, release))
    let assert Ok(Nil) = process.receive(release, 10_000)
    Nil
  }
}

pub fn pool_limits_parallel_workers_test() {
  let offer_probe = process.new_subject()
  let assert Ok(counter) = counter_source() |> source.start()
  let assert Ok(workers) =
    pool.sink(concurrency: 2, run: blocking_work(offer_probe))
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: workers.data)

  // The pool fills its two places.
  let assert Ok(#(first_event, first_release)) =
    process.receive(offer_probe, 1000)
  let assert Ok(#(second_event, _second_release)) =
    process.receive(offer_probe, 1000)
  assert list.sort([first_event, second_event], by: int.compare) == [0, 1]

  // Both workers run: no third worker starts.
  assert process.receive(offer_probe, 150) == Error(Nil)

  // A completion opens one place, and exactly one new worker starts.
  process.send(first_release, Nil)
  let assert Ok(#(2, _third_release)) = process.receive(offer_probe, 1000)
  assert process.receive(offer_probe, 150) == Error(Nil)

  shutdown(workers)
  shutdown(counter)
}

pub fn pool_survives_a_worker_failure_test() {
  let offer_probe = process.new_subject()
  let assert Ok(counter) = counter_source() |> source.start()
  let assert Ok(workers) =
    pool.sink(concurrency: 1, run: fn(event) {
      case event {
        0 -> panic as "the first worker fails"
        _ -> process.send(offer_probe, #(event, process.new_subject()))
      }
    })
    |> sink.start()

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: workers.data)

  // The first worker fails. The pool reports the completion anyway, asks
  // for one more event, and the second worker runs.
  let assert Ok(#(1, _release)) = process.receive(offer_probe, 1000)

  shutdown(workers)
  shutdown(counter)
}
