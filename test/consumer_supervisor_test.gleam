//// Integration tests for per-event child supervision and demand accounting.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/list
import gleam/otp/actor
import sluice
import sluice/consumer_supervisor
import sluice/source
import support

type WorkerAction {
  Complete
  Crash
}

type WorkerOffer {
  WorkerOffer(event: Int, pid: Pid, action: Subject(WorkerAction))
}

fn controlled_worker(
  probe: Subject(WorkerOffer),
) -> fn(Int) -> actor.StartResult(Nil) {
  fn(event) {
    let pid =
      process.spawn(fn() {
        let action = process.new_subject()
        process.send(probe, WorkerOffer(event, process.self(), action))
        case process.receive_forever(action) {
          Complete -> Nil
          Crash -> process.kill(process.self())
        }
      })
    Ok(actor.Started(pid:, data: Nil))
  }
}

fn subscribe(
  producer: actor.Started(sluice.Outlet(Int)),
  supervisor: consumer_supervisor.Supervisor(Int, Nil),
  min min: Int,
  max max: Int,
) {
  let assert Ok(_) =
    sluice.subscription(to: producer.data)
    |> sluice.min_demand(min)
    |> sluice.max_demand(max)
    |> sluice.subscribe(consumer: consumer_supervisor.inlet(supervisor))
  Nil
}

pub fn demand_follows_final_child_termination_test() {
  let probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(controlled_worker(probe))
    |> consumer_supervisor.shutdown_timeout(100)
    |> consumer_supervisor.start_timeout(1000)
    |> consumer_supervisor.start()
  subscribe(counter, supervisor.data, min: 0, max: 2)

  let assert Ok(WorkerOffer(first, _first_pid, first_action)) =
    process.receive(probe, 1000)
  let assert Ok(WorkerOffer(second, _second_pid, second_action)) =
    process.receive(probe, 1000)
  assert list.sort([first, second], by: int.compare) == [0, 1]
  assert process.receive(probe, 100) == Error(Nil)

  // With min_demand 0, one completion is not enough to replenish the batch.
  process.send(first_action, Complete)
  assert process.receive(probe, 100) == Error(Nil)

  // The second completion frees the second slot. One ask then refills
  // the two slots together.
  process.send(second_action, Complete)
  let assert Ok(WorkerOffer(third, _, _)) = process.receive(probe, 1000)
  let assert Ok(WorkerOffer(fourth, _, _)) = process.receive(probe, 1000)
  assert list.sort([third, fourth], by: int.compare) == [2, 3]

  support.shutdown(supervisor)
  support.shutdown(counter)
}

pub fn transient_child_restarts_same_event_without_releasing_demand_test() {
  let probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(controlled_worker(probe))
    |> consumer_supervisor.restart(consumer_supervisor.Transient)
    |> consumer_supervisor.start()
  subscribe(counter, supervisor.data, min: 0, max: 1)

  let assert Ok(WorkerOffer(0, first_pid, first_action)) =
    process.receive(probe, 1000)
  process.send(first_action, Crash)

  let assert Ok(WorkerOffer(0, second_pid, second_action)) =
    process.receive(probe, 1000)
  assert first_pid != second_pid
  assert process.receive(probe, 100) == Error(Nil)

  process.send(second_action, Complete)
  let assert Ok(WorkerOffer(1, _, _)) = process.receive(probe, 1000)

  support.shutdown(supervisor)
  support.shutdown(counter)
}

pub fn temporary_child_failure_frees_its_slot_test() {
  let probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(controlled_worker(probe))
    |> consumer_supervisor.start()
  subscribe(counter, supervisor.data, min: 0, max: 1)

  let assert Ok(WorkerOffer(0, _, action)) = process.receive(probe, 1000)
  process.send(action, Crash)
  let assert Ok(WorkerOffer(1, _, _)) = process.receive(probe, 1000)

  support.shutdown(supervisor)
  support.shutdown(counter)
}

pub fn children_are_visible_and_can_be_terminated_test() {
  let probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(controlled_worker(probe))
    |> consumer_supervisor.start()
  subscribe(counter, supervisor.data, min: 0, max: 1)

  let assert Ok(WorkerOffer(0, pid, _action)) = process.receive(probe, 1000)
  let assert Ok(consumer_supervisor.ChildrenCount(1, 0, 1)) =
    consumer_supervisor.count_children(supervisor.data)
  let assert Ok([
    consumer_supervisor.Child(_, consumer_supervisor.Running(visible_pid)),
  ]) = consumer_supervisor.which_children(supervisor.data)
  assert visible_pid == pid

  assert consumer_supervisor.terminate_child(supervisor.data, pid) == Ok(Nil)
  let assert Ok(WorkerOffer(1, _, _)) = process.receive(probe, 1000)

  support.shutdown(supervisor)
  support.shutdown(counter)
}

pub fn manual_demand_is_rejected_test() {
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(fn(_event) {
      Ok(actor.Started(pid: process.spawn(fn() { Nil }), data: Nil))
    })
    |> consumer_supervisor.start()

  assert sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: consumer_supervisor.inlet(supervisor.data))
    == Error(sluice.UnsupportedDemandMode)

  support.shutdown(supervisor)
  support.shutdown(counter)
}

pub fn restart_intensity_stops_the_supervisor_test() {
  let probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(controlled_worker(probe))
    |> consumer_supervisor.restart(consumer_supervisor.Transient)
    |> consumer_supervisor.restart_tolerance(max_restarts: 0, within_seconds: 5)
    |> consumer_supervisor.start()
  subscribe(counter, supervisor.data, min: 0, max: 1)

  process.unlink(supervisor.pid)
  let monitor = process.monitor(supervisor.pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(down) { down })
  let assert Ok(WorkerOffer(0, _, action)) = process.receive(probe, 1000)
  process.send(action, Crash)
  let assert Ok(process.ProcessDown(_, _, _)) =
    process.selector_receive(selector, 1000)

  support.shutdown(counter)
}

pub fn immediate_completion_has_no_monitoring_gap_test() {
  let demand_probe = process.new_subject()
  let assert Ok(counter) =
    support.counter_source_with_probe(demand_probe) |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(fn(_event) {
      Ok(actor.Started(pid: process.spawn(fn() { Nil }), data: Nil))
    })
    |> consumer_supervisor.start()
  subscribe(counter, supervisor.data, min: 0, max: 1)

  // The child can finish before its start function returns. Because the
  // supervisor owns the link before it handles the exit, the slot is still
  // freed and the next ask arrives.
  let assert Ok(1) = process.receive(demand_probe, 1000)
  let assert Ok(1) = process.receive(demand_probe, 1000)

  support.shutdown(supervisor)
  support.shutdown(counter)
}

pub fn producer_failure_stops_supervisor_and_its_running_children_test() {
  let probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(controlled_worker(probe))
    |> consumer_supervisor.start()
  subscribe(counter, supervisor.data, min: 0, max: 1)

  let assert Ok(WorkerOffer(0, child, _action)) = process.receive(probe, 1000)
  let child_monitor = process.monitor(child)
  let child_selector =
    process.new_selector()
    |> process.select_specific_monitor(child_monitor, fn(down) { down })
  process.unlink(supervisor.pid)
  support.shutdown(counter)

  let assert Ok(process.ProcessDown(_, _, _)) =
    process.selector_receive(child_selector, 1000)
}

pub fn named_supervisor_establishes_declared_subscription_test() {
  let probe = process.new_subject()
  let name = consumer_supervisor.new_name("deliveries")
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(controlled_worker(probe))
    |> consumer_supervisor.subscribe(
      sluice.subscription(to: counter.data)
      |> sluice.min_demand(0)
      |> sluice.max_demand(1),
    )
    |> consumer_supervisor.named(name)
    |> consumer_supervisor.start()

  let assert Ok(WorkerOffer(0, _, _)) = process.receive(probe, 1000)
  assert consumer_supervisor.whereis(name) == Ok(supervisor.pid)
  let _named_inlet = consumer_supervisor.inlet_of(name)
  let by_name = consumer_supervisor.supervisor_of(name)
  assert consumer_supervisor.count_children(by_name)
    == Ok(consumer_supervisor.ChildrenCount(1, 0, 1))

  support.shutdown(supervisor)
  support.shutdown(counter)
}

pub fn child_start_panic_is_a_failed_event_not_a_supervisor_crash_test() {
  let probe = process.new_subject()
  let assert Ok(counter) = support.counter_source() |> source.start()
  let assert Ok(supervisor) =
    consumer_supervisor.new(fn(event) {
      case event {
        0 -> panic as "start failed"
        _ -> controlled_worker(probe)(event)
      }
    })
    |> consumer_supervisor.start()
  subscribe(counter, supervisor.data, min: 0, max: 1)

  // Event zero is discarded after its start callback panics. Its slot is
  // freed and the still-live supervisor starts event one.
  let assert Ok(WorkerOffer(1, _, _)) = process.receive(probe, 1000)
  assert process.is_alive(supervisor.pid)

  support.shutdown(supervisor)
  support.shutdown(counter)
}
