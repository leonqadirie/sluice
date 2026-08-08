//// The shared runtime for the producer face of a stage (Source and
//// Gate). It records the subscribers, counts the demand, dispatches the
//// events, and operates the buffer.
////
//// The core sends no messages itself. Each operation returns the
//// messages for the subscribers as `Outbound` values, in the sequence of
//// the sends. The stage sends them with `send_all`. Thus the decision
//// logic is a pure function over its inputs, and tests can examine the
//// outbound messages without processes.

import gleam/dict.{type Dict}
import gleam/erlang/process.{
  type Down, type Monitor, type Selector, type Subject,
}
import gleam/int
import gleam/list
import sluice/internal/buffer.{type Buffer}
import sluice/internal/dispatcher.{type Dispatcher}
import sluice/internal/protocol.{type ConsumerMessage, type SubscribeMetadata}

pub type Core(event) {
  Core(
    dispatcher: Dispatcher(event),
    buffer: Buffer(event),
    subscribers: Dict(Subject(ConsumerMessage(event)), Monitor),
    mode: AskMode(event),
  )
}

/// A source can accumulate the incoming asks, for example until the full
/// pipeline stands. The `forward` function replays them.
pub type AskMode(event) {
  Forwarding
  Accumulating(held: List(#(Subject(ConsumerMessage(event)), Int)))
}

/// A message that the stage must send to a subscriber.
pub type Outbound(event) {
  Outbound(to: Subject(ConsumerMessage(event)), message: ConsumerMessage(event))
}

/// The outbound messages of one operation, as chunks with the newest
/// chunk first. The internal loops only prepend chunks. `assemble` makes
/// the send sequence one time, at the end. Thus the collection stays
/// linear in the quantity of messages.
type Chunks(event) =
  List(List(Outbound(event)))

/// The result of `emit`: the next core, the messages for the subscribers,
/// the quantity of events that the overflow policy discarded, and the
/// demand that stays unfilled.
pub type EmitResult(event) {
  EmitResult(
    core: Core(event),
    outbound: List(Outbound(event)),
    discarded: Int,
    unfilled: Int,
  )
}

/// The result of `on_cancel`: the next core, the selector without the
/// monitor of the subscriber, the messages for the subscribers, and the
/// demand that the removal frees for the other subscribers.
pub type CancelResult(event, message) {
  CancelResult(
    core: Core(event),
    selector: Selector(message),
    outbound: List(Outbound(event)),
    freed: Int,
  )
}

pub fn new(
  dispatcher dispatcher: Dispatcher(event),
  buffer buffer: Buffer(event),
) -> Core(event) {
  Core(dispatcher:, buffer:, subscribers: dict.new(), mode: Forwarding)
}

/// Start in the accumulation mode: the asks wait until `forward` runs.
pub fn accumulate(core: Core(event)) -> Core(event) {
  Core(..core, mode: Accumulating(held: []))
}

pub fn has_demand(core: Core(event)) -> Bool {
  core.dispatcher.total_demand() > 0
}

pub fn buffer_is_empty(core: Core(event)) -> Bool {
  buffer.is_empty(core.buffer)
}

pub fn subscriber_count(core: Core(event)) -> Int {
  dict.size(core.subscribers)
}

/// Send the outbound messages of a core operation to their subscribers,
/// in sequence. This is the only function of this module that sends.
pub fn send_all(outbound: List(Outbound(event))) -> Nil {
  list.each(outbound, fn(entry) { process.send(entry.to, entry.message) })
}

/// Add a new subscriber: give it to the dispatcher, monitor its process,
/// and select its exit message. When the dispatcher refuses the
/// subscription, the outbound messages contain the refusal.
pub fn on_subscribe(
  core: Core(event),
  selector: Selector(message),
  from: Subject(ConsumerMessage(event)),
  metadata: SubscribeMetadata(event),
  tag_down: fn(Subject(ConsumerMessage(event)), Down) -> message,
) -> #(Core(event), Selector(message), List(Outbound(event))) {
  case process.subject_owner(from) {
    // The subscriber process is not alive. A monitor message can not
    // come.
    Error(Nil) -> #(core, selector, [])
    Ok(pid) ->
      case core.dispatcher.subscribe(from, metadata) {
        Error(reason) -> {
          let refusal =
            Outbound(
              to: from,
              message: protocol.Cancelled(protocol.Abnormal(
                "subscription refused: " <> reason,
              )),
            )
          #(core, selector, [refusal])
        }
        Ok(next_dispatcher) -> {
          let monitor = process.monitor(pid)
          let selector =
            process.select_specific_monitor(selector, monitor, fn(down) {
              tag_down(from, down)
            })
          let core =
            Core(
              ..core,
              dispatcher: next_dispatcher,
              subscribers: dict.insert(core.subscribers, from, monitor),
            )
          #(core, selector, [])
        }
      }
  }
}

/// Process an ask from a subscriber. First, the buffer supplies the
/// recorded demand: the outbound messages carry those events. The
/// returned integer is the part of this ask that stays open. The stage
/// can make new events for it.
///
/// The buffer supply does not examine the delta from the dispatcher. The
/// dispatcher can decrease the delta to zero when it absorbs the ask into
/// demand from a cancelled subscriber. The buffer must supply the recorded
/// demand also in that condition. If not, a new subscriber to a producer
/// with a full buffer and no new events waits without an end.
pub fn on_ask(
  core core: Core(event),
  from from: Subject(ConsumerMessage(event)),
  demand demand: Int,
) -> #(Core(event), List(Outbound(event)), Int) {
  case core.mode {
    Accumulating(held) -> {
      let mode = Accumulating(held: [#(from, demand), ..held])
      #(Core(..core, mode:), [], 0)
    }
    Forwarding -> {
      let #(core, chunks, unfilled) = forward_ask(core:, from:, demand:)
      #(core, assemble(chunks), unfilled)
    }
  }
}

/// End the accumulation: replay the held asks in their sequence. Return
/// the demand that the stage can make new events for.
pub fn forward(
  core: Core(event),
) -> #(Core(event), List(Outbound(event)), Int) {
  case core.mode {
    Forwarding -> #(core, [], 0)
    Accumulating(held) -> {
      let core = Core(..core, mode: Forwarding)
      let #(core, chunks, unfilled) =
        list.fold(list.reverse(held), #(core, [], 0), fn(accumulated, ask) {
          let #(core, chunks, unfilled) = accumulated
          let #(from, demand) = ask
          let #(core, replayed, added) = forward_ask(core:, from:, demand:)
          #(core, list.append(replayed, chunks), unfilled + added)
        })
      #(core, assemble(chunks), unfilled)
    }
  }
}

fn forward_ask(
  core core: Core(event),
  from from: Subject(ConsumerMessage(event)),
  demand demand: Int,
) -> #(Core(event), Chunks(event), Int) {
  let #(new_demand, deliveries, next_dispatcher) =
    core.dispatcher.ask(demand, from)
  let core = Core(..core, dispatcher: next_dispatcher)
  let #(core, chunks, debt) =
    drain(core:, chunks: [from_deliveries(deliveries)], debt: new_demand)
  #(core, chunks, int.max(debt, 0))
}

/// Dispatch new events to the subscribers. Keep the events that are more
/// than the recorded demand in the buffer. The result carries the
/// outbound messages, the quantity of events that the overflow policy
/// discarded, and the replacement demand that filters released and that
/// the buffer could not supply. The stage can make new events for the
/// replacement demand.
pub fn emit(
  core core: Core(event),
  events events: List(event),
) -> EmitResult(event) {
  case events {
    [] -> EmitResult(core:, outbound: [], discarded: 0, unfilled: 0)
    _ -> {
      let #(core, chunks, debt) = drain(core:, chunks: [], debt: 0)
      case buffer.is_empty(core.buffer) {
        True -> {
          let result = core.dispatcher.dispatch(events, list.length(events))
          let #(core, chunks, extra) = settle(core:, result:, chunks:)
          let #(new_buffer, discarded) =
            buffer.store(core.buffer, result.leftover)
          let core = Core(..core, buffer: new_buffer)
          let #(core, chunks, debt) = drain(core:, chunks:, debt: debt + extra)
          EmitResult(
            core:,
            outbound: assemble(chunks),
            discarded:,
            unfilled: int.max(debt, 0),
          )
        }
        // The buffer contains older events that no demand supplied. The
        // new events go behind them. Thus the sequence of the events does
        // not change.
        False -> {
          let #(new_buffer, discarded) = buffer.store(core.buffer, events)
          let #(core, chunks, debt) =
            drain(core: Core(..core, buffer: new_buffer), chunks:, debt:)
          EmitResult(
            core:,
            outbound: assemble(chunks),
            discarded:,
            unfilled: int.max(debt, 0),
          )
        }
      }
    }
  }
}

// Apply a dispatch result: collect the deliveries, and replay the
// replacement asks through the normal ask path. Return the new demand
// that the replacement asks release.
fn settle(
  core core: Core(event),
  result result: dispatcher.DispatchResult(event),
  chunks chunks: Chunks(event),
) -> #(Core(event), Chunks(event), Int) {
  let chunks = [from_deliveries(result.deliveries), ..chunks]
  let core = Core(..core, dispatcher: result.next)
  list.fold(result.reask, #(core, chunks, 0), fn(accumulated, replacement) {
    let #(core, chunks, released) = accumulated
    let #(from, demand) = replacement
    let #(delta, deliveries, next_dispatcher) =
      core.dispatcher.ask(demand, from)
    #(
      Core(..core, dispatcher: next_dispatcher),
      [from_deliveries(deliveries), ..chunks],
      released + delta,
    )
  })
}

// Supply the recorded demand from the buffer, until the demand or the
// buffer is empty. The `debt` counts demand that entered through asks and
// was not supplied: each event from the buffer decreases it, and each
// replacement ask increases it. Events that the dispatcher does not
// deliver go to the front again. Thus the sequence stays.
fn drain(
  core core: Core(event),
  chunks chunks: Chunks(event),
  debt debt: Int,
) -> #(Core(event), Chunks(event), Int) {
  let want = int.min(core.dispatcher.total_demand(), buffer.size(core.buffer))
  case want > 0 {
    False -> #(core, chunks, debt)
    True -> {
      let #(buffered, remaining_buffer) = buffer.take(core.buffer, want)
      let result = core.dispatcher.dispatch(buffered, list.length(buffered))
      let #(core, chunks, extra) =
        settle(core: Core(..core, buffer: remaining_buffer), result:, chunks:)
      let core =
        Core(..core, buffer: buffer.store_front(core.buffer, result.leftover))
      let supplied = list.length(buffered) - list.length(result.leftover)
      drain(core:, chunks:, debt: debt + extra - supplied)
    }
  }
}

/// Remove a subscriber. Use `acknowledge: True` when the subscriber asked
/// for the cancellation: the outbound messages then contain the
/// acknowledgment. Use `acknowledge: False` when its process stopped. The
/// `freed` value is demand that the removal releases for the other
/// subscribers (a broadcast dispatcher moves at the speed of its slowest
/// subscriber, so a removal can release demand). The stage can make new
/// events for it.
pub fn on_cancel(
  core core: Core(event),
  selector selector: Selector(message),
  from from: Subject(ConsumerMessage(event)),
  acknowledge acknowledge: Bool,
) -> CancelResult(event, message) {
  case dict.get(core.subscribers, from) {
    Error(Nil) ->
      // The subscription is not known. Return the acknowledgment anyway.
      // Thus a consumer that waits for it can continue.
      CancelResult(
        core:,
        selector:,
        outbound: acknowledgement(from, acknowledge),
        freed: 0,
      )
    Ok(monitor) -> {
      process.demonitor_process(monitor)
      let selector = process.deselect_specific_monitor(selector, monitor)
      let #(released, next_dispatcher) = core.dispatcher.cancel(from)
      let core =
        Core(
          ..core,
          dispatcher: next_dispatcher,
          subscribers: dict.delete(core.subscribers, from),
        )
      let core = case core.mode {
        Forwarding -> core
        Accumulating(held) ->
          Core(
            ..core,
            mode: Accumulating(
              held: list.filter(held, fn(ask) { ask.0 != from }),
            ),
          )
      }
      let #(core, chunks, debt) =
        drain(
          core:,
          chunks: [acknowledgement(from, acknowledge)],
          debt: released,
        )
      CancelResult(
        core:,
        selector:,
        outbound: assemble(chunks),
        freed: int.max(debt, 0),
      )
    }
  }
}

fn acknowledgement(
  from: Subject(ConsumerMessage(event)),
  acknowledge: Bool,
) -> List(Outbound(event)) {
  case acknowledge {
    False -> []
    True -> [Outbound(to: from, message: protocol.Cancelled(protocol.Normal))]
  }
}

fn from_deliveries(
  deliveries: List(dispatcher.Delivery(event)),
) -> List(Outbound(event)) {
  list.map(deliveries, fn(delivery) {
    Outbound(to: delivery.to, message: protocol.NewEvents(delivery.events))
  })
}

// The chunks stand with the newest chunk first. One reversal and one
// flatten make the send sequence.
fn assemble(chunks: Chunks(event)) -> List(Outbound(event)) {
  chunks |> list.reverse |> list.flatten
}
