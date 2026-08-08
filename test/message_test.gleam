//// Tests for the private message channels of the stages: injection of
//// events into a source, reconfiguration of a gate, timers, and a stop
//// command to a sink.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/otp/actor.{type Started}
import sluice
import sluice/gate
import sluice/sink
import sluice/source

fn count_up(from: Int, count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}

fn shutdown(started: Started(data)) -> Nil {
  process.unlink(started.pid)
  process.kill(started.pid)
}

fn small_demand(options: sluice.SubscriptionOptions(event)) {
  options |> sluice.min_demand(2) |> sluice.max_demand(6)
}

type SourceCommand {
  Inject(Int)
  Finish
}

// A silent source emits events only when a command arrives on its message
// channel. A timer message from `send_after` also works: the channel is a
// normal subject.
pub fn source_message_channel_test() {
  let channel_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(silent) =
    source.new(init: Nil, on_demand: fn(state, _demand) {
      source.emit([], state)
    })
    |> source.on_message(
      initialise: fn(subject) {
        process.send(channel_probe, subject)
        let _timer = process.send_after(subject, 50, Inject(99))
        Nil
      },
      handler: fn(state, command) {
        case command {
          Inject(event) -> source.emit([event], state)
          Finish -> source.stop()
        }
      },
    )
    |> source.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
    })
    |> sink.start()
  let assert Ok(channel) = process.receive(channel_probe, 1000)
  process.unlink(silent.pid)
  let source_monitor = process.monitor(silent.pid)
  process.unlink(collector.pid)

  let assert Ok(_) =
    sluice.subscription(to: silent.data)
    |> small_demand
    |> sluice.subscribe(consumer: collector.data)

  // The timer message arrives first and becomes an event.
  let assert Ok([99]) = process.receive(batch_probe, 1000)

  // Direct sends inject events in sequence.
  process.send(channel, Inject(1))
  let assert Ok([1]) = process.receive(batch_probe, 1000)
  process.send(channel, Inject(2))
  let assert Ok([2]) = process.receive(batch_probe, 1000)

  // A command can stop the source. The handler has the full stage
  // vocabulary.
  process.send(channel, Finish)
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(source_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(down_selector, 2000)
}

// A gate changes its configuration through its message channel. The
// manual demand mode makes the sequence deterministic: the factor changes
// between two asks.
pub fn gate_reconfiguration_test() {
  let channel_probe = process.new_subject()
  let batch_probe = process.new_subject()
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      source.emit(count_up(counter, demand), counter + demand)
    })
    |> source.start()
  let assert Ok(multiplier) =
    gate.new(init: 2, on_events: fn(factor, events, _subscription) {
      gate.emit(list.map(events, fn(event) { event * factor }), factor)
    })
    |> gate.on_message(
      initialise: fn(subject) { process.send(channel_probe, subject) },
      handler: fn(_factor, new_factor) { gate.emit([], new_factor) },
    )
    |> gate.start()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      process.send(batch_probe, events)
      sink.continue(state)
    })
    |> sink.start()
  let assert Ok(channel) = process.receive(channel_probe, 1000)

  // Both subscriptions use the manual mode. Thus events pass the gate
  // only between the asks, and the factor change has a clear position in
  // the sequence.
  let assert Ok(upstream) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: gate.inlet(multiplier.data))
  let assert Ok(downstream) =
    sluice.subscription(to: gate.outlet(multiplier.data))
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.subscribe(consumer: collector.data)

  sluice.ask(subscription: upstream, count: 2)
  sluice.ask(subscription: downstream, count: 2)
  let assert Ok([0, 2]) = process.receive(batch_probe, 1000)

  // The new factor applies to all subsequent events.
  process.send(channel, 10)
  sluice.ask(subscription: upstream, count: 2)
  sluice.ask(subscription: downstream, count: 2)
  let assert Ok([20, 30]) = process.receive(batch_probe, 1000)

  shutdown(collector)
  shutdown(multiplier)
  shutdown(counter)
}

type SinkCommand {
  Note(String)
  Halt
}

pub fn sink_message_channel_test() {
  let channel_probe = process.new_subject()
  let note_probe = process.new_subject()
  let assert Ok(collector) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.on_message(
      initialise: fn(subject) { process.send(channel_probe, subject) },
      handler: fn(state, command) {
        case command {
          Note(text) -> {
            process.send(note_probe, text)
            sink.continue(state)
          }
          Halt -> sink.stop()
        }
      },
    )
    |> sink.start()
  let assert Ok(channel) = process.receive(channel_probe, 1000)
  process.unlink(collector.pid)
  let sink_monitor = process.monitor(collector.pid)

  process.send(channel, Note("hello"))
  let assert Ok("hello") = process.receive(note_probe, 1000)

  process.send(channel, Halt)
  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(sink_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Normal)) =
    process.selector_receive(down_selector, 2000)
}
