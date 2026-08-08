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
  )
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
/// and the quantity of events that the overflow policy discarded.
pub type EmitResult(event) {
  EmitResult(core: Core(event), outbound: List(Outbound(event)), discarded: Int)
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
  Core(dispatcher:, buffer:, subscribers: dict.new())
}

pub fn has_demand(core: Core(event)) -> Bool {
  core.dispatcher.total_demand() > 0
}

pub fn buffer_is_empty(core: Core(event)) -> Bool {
  buffer.is_empty(core.buffer)
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
  let #(new_demand, deliveries, next_dispatcher) =
    core.dispatcher.ask(demand, from)
  let core = Core(..core, dispatcher: next_dispatcher)
  let buffered_before = buffer.size(core.buffer)
  let #(core, chunks) = drain(core, [from_deliveries(deliveries)])
  let taken = buffered_before - buffer.size(core.buffer)
  #(core, assemble(chunks), int.max(new_demand - taken, 0))
}

/// Dispatch new events to the subscribers. Keep the events that are more
/// than the recorded demand in the buffer. The result carries the
/// outbound messages and the quantity of events that the overflow policy
/// discarded.
pub fn emit(
  core core: Core(event),
  events events: List(event),
) -> EmitResult(event) {
  case events {
    [] -> EmitResult(core:, outbound: [], discarded: 0)
    _ -> {
      let #(core, chunks) = drain(core, [])
      case buffer.is_empty(core.buffer) {
        True -> {
          let result = core.dispatcher.dispatch(events, list.length(events))
          let chunks = [from_deliveries(result.deliveries), ..chunks]
          let #(new_buffer, discarded) =
            buffer.store(core.buffer, result.leftover)
          EmitResult(
            core: Core(..core, dispatcher: result.next, buffer: new_buffer),
            outbound: assemble(chunks),
            discarded:,
          )
        }
        // The buffer contains older events that no demand supplied. The
        // new events go behind them. Thus the sequence of the events does
        // not change.
        False -> {
          let #(new_buffer, discarded) = buffer.store(core.buffer, events)
          EmitResult(
            core: Core(..core, buffer: new_buffer),
            outbound: assemble(chunks),
            discarded:,
          )
        }
      }
    }
  }
}

// Supply the recorded demand from the buffer. Events that the dispatcher
// does not deliver go to the front again. Thus the sequence stays.
fn drain(
  core: Core(event),
  chunks: Chunks(event),
) -> #(Core(event), Chunks(event)) {
  let want = int.min(core.dispatcher.total_demand(), buffer.size(core.buffer))
  case want > 0 {
    False -> #(core, chunks)
    True -> {
      let #(buffered, remaining_buffer) = buffer.take(core.buffer, want)
      let result = core.dispatcher.dispatch(buffered, list.length(buffered))
      #(
        Core(
          ..core,
          dispatcher: result.next,
          buffer: buffer.store_front(remaining_buffer, result.leftover),
        ),
        [from_deliveries(result.deliveries), ..chunks],
      )
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
      let #(freed, next_dispatcher) = core.dispatcher.cancel(from)
      let core =
        Core(
          ..core,
          dispatcher: next_dispatcher,
          subscribers: dict.delete(core.subscribers, from),
        )
      let buffered_before = buffer.size(core.buffer)
      let #(core, chunks) = drain(core, [acknowledgement(from, acknowledge)])
      let taken = buffered_before - buffer.size(core.buffer)
      CancelResult(
        core:,
        selector:,
        outbound: assemble(chunks),
        freed: int.max(freed - taken, 0),
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
