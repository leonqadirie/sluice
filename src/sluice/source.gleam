//// A `Source` is the start of a pipeline. It makes events when the stages
//// after it ask for them. Different processes can also push events into it
//// through an `Emitter`.

import gleam/bool
import gleam/erlang/process.{type Down, type Selector, type Subject}
import gleam/int
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import sluice.{type Keep, type Outlet}
import sluice/internal/buffer
import sluice/internal/dispatcher
import sluice/internal/platform
import sluice/internal/producer_core
import sluice/internal/protocol.{type ConsumerMessage, type ProducerMessage}

/// The response of a source to demand. Make it with `emit`, `stop`, or
/// `stop_abnormal`.
pub opaque type Produce(state, event) {
  Emit(events: List(event), state: state)
  Stop
  StopAbnormal(reason: String)
}

/// Emit events for the demand. The quantity of events can be less than the
/// demand, or zero. The demand that stays open is then supplied by
/// subsequent pushes through an `Emitter`. The quantity can also be more
/// than the demand. The buffer keeps the extra events.
pub fn emit(
  events events: List(event),
  state state: state,
) -> Produce(state, event) {
  Emit(events, state)
}

/// Stop the source with the normal reason. The subscribers apply their
/// cancel modes. Use this to stop a pipeline that has an end.
pub fn stop() -> Produce(state, event) {
  Stop
}

/// Stop the source with a failure reason.
pub fn stop_abnormal(reason: String) -> Produce(state, event) {
  StopAbnormal(reason)
}

/// A handle for a source that operates. Each process can use it to push
/// events into the source.
pub opaque type Emitter(event) {
  Emitter(subject: Subject(EmitterMessage(event)))
}

type EmitterMessage(event) {
  Pushed(events: List(event))
  Finished
}

/// Push events into the source. The source sends them to the consumers
/// immediately, up to the open demand. The buffer keeps the remaining
/// events.
pub fn push(
  through emitter: Emitter(event),
  events events: List(event),
) -> Nil {
  process.send(emitter.subject, Pushed(events))
}

/// Stop the source with the normal reason, from a different process.
pub fn finish(emitter: Emitter(event)) -> Nil {
  process.send(emitter.subject, Finished)
}

/// A permanent name for a source. Subscribers can find the source through
/// the name after a restart. Make the name with `new_name`. Attach it with
/// `named`. Point subscriptions to it with `outlet_of`.
pub opaque type Name(event) {
  Name(name: process.Name(ProducerMessage(event)))
}

pub fn new_name(prefix prefix: String) -> Name(event) {
  Name(process.new_name(prefix))
}

/// The process that has this name now, if a process has it.
pub fn whereis(name: Name(event)) -> Result(process.Pid, Nil) {
  process.named(name.name)
}

/// The outlet of the source that has this name. The outlet stays correct
/// through restarts of the source. Thus use it to connect stages under a
/// supervisor.
pub fn outlet_of(name: Name(event)) -> Outlet(event) {
  // While a stage restarts, its name can be free for a short time.
  // Messages that you send in this time are lost. This is the same as
  // messages to a dead pid. The monitors of the other side find the loss.
  sluice.make_outlet(
    protocol.ProducerHandle(
      send: fn(message) { platform.send_named(name.name, message) },
      owner: fn() { process.named(name.name) },
    ),
  )
}

pub opaque type Builder(state, event) {
  Builder(
    initialise: fn(Emitter(event)) -> Result(state, String),
    on_demand: fn(state, Int) -> Produce(state, event),
    capacity: buffer.Capacity,
    keep: Keep,
    name: Option(Name(event)),
    start_timeout: Int,
    on_discard: fn(state, Int) -> state,
    messages: Option(fn() -> Selector(Message(state, event))),
  )
}

// The default response to discarded events is a log warning.
fn default_on_discard(state: state, count: Int) -> state {
  platform.log_warning(
    "sluice: a source discarded "
    <> int.to_string(count)
    <> " events because its buffer was full",
  )
  state
}

/// Define a source that makes events on demand.
pub fn new(
  init state: state,
  on_demand on_demand: fn(state, Int) -> Produce(state, event),
) -> Builder(state, event) {
  Builder(
    initialise: fn(_emitter) { Ok(state) },
    on_demand:,
    capacity: buffer.Bounded(10_000),
    keep: sluice.KeepLast,
    name: None,
    start_timeout: 5000,
    on_discard: default_on_discard,
    messages: None,
  )
}

/// Define a source that receives its events from an external location. The
/// initialiser receives an `Emitter`. Each process can `push` events
/// through the `Emitter`. A source that only receives pushes can keep the
/// default `on_demand`. If the source can also make events on request, set
/// a handler with `on_demand`.
pub fn new_with_emitter(
  init initialise: fn(Emitter(event)) -> Result(state, String),
) -> Builder(state, event) {
  Builder(
    initialise:,
    on_demand: fn(state, _demand) { Emit([], state) },
    capacity: buffer.Bounded(10_000),
    keep: sluice.KeepLast,
    name: None,
    start_timeout: 5000,
    on_discard: default_on_discard,
    messages: None,
  )
}

/// Set the demand handler of a source that `new_with_emitter` made.
pub fn on_demand(
  builder: Builder(state, event),
  on_demand: fn(state, Int) -> Produce(state, event),
) -> Builder(state, event) {
  Builder(..builder, on_demand:)
}

/// Set the maximum quantity of events that the buffer keeps while there is
/// no demand. The default is 10,000.
pub fn buffer_capacity(
  builder builder: Builder(state, event),
  events capacity: Int,
) -> Builder(state, event) {
  Builder(..builder, capacity: buffer.Bounded(int.max(capacity, 0)))
}

/// Remove the buffer limit.
pub fn buffer_unbounded(
  builder: Builder(state, event),
) -> Builder(state, event) {
  Builder(..builder, capacity: buffer.Unbounded)
}

/// Select the events that stay when the buffer is full. The default is
/// `KeepLast`.
pub fn buffer_keep(
  builder builder: Builder(state, event),
  keep keep: Keep,
) -> Builder(state, event) {
  Builder(..builder, keep:)
}

/// Give the source a private message channel with a type of your choice.
/// At the start, `initialise` receives the subject of the channel: send it
/// to other processes, or start a timer with `process.send_after`. The
/// handler receives each message together with the state, and it can emit
/// events, like the demand handler. Use this for timers, for configuration
/// changes, and for queries.
pub fn on_message(
  builder: Builder(state, event),
  initialise initialise: fn(Subject(user_message)) -> Nil,
  handler handler: fn(state, user_message) -> Produce(state, event),
) -> Builder(state, event) {
  Builder(
    ..builder,
    messages: Some(fn() {
      let subject = process.new_subject()
      initialise(subject)
      process.new_selector()
      |> process.select_map(subject, fn(user_message) {
        FromUser(fn(state) { handler(state, user_message) })
      })
    }),
  )
}

/// Set the response of the source to discarded events. The callback
/// receives the state and the quantity of discarded events. The default
/// response writes a warning to the log.
pub fn on_discard(
  builder: Builder(state, event),
  on_discard: fn(state, Int) -> state,
) -> Builder(state, event) {
  Builder(..builder, on_discard:)
}

/// The maximum time for the start of the source. The default is 5000
/// milliseconds.
pub fn start_timeout(
  builder builder: Builder(state, event),
  milliseconds milliseconds: Int,
) -> Builder(state, event) {
  Builder(..builder, start_timeout: int.max(milliseconds, 1))
}

/// Attach a permanent name to the source at its start.
pub fn named(
  builder builder: Builder(state, event),
  name name: Name(event),
) -> Builder(state, event) {
  Builder(..builder, name: Some(name))
}

type Message(state, event) {
  FromDownstream(message: ProducerMessage(event))
  SubscriberDown(from: Subject(ConsumerMessage(event)), down: Down)
  FromEmitter(message: EmitterMessage(event))
  FromUser(apply: fn(state) -> Produce(state, event))
}

type State(state, event) {
  State(
    user_state: state,
    core: producer_core.Core(event),
    selector: Selector(Message(state, event)),
    on_demand: fn(state, Int) -> Produce(state, event),
    on_discard: fn(state, Int) -> state,
  )
}

/// Start the source. The returned data is its `Outlet`. You can subscribe
/// to it.
pub fn start(
  builder: Builder(state, event),
) -> Result(actor.Started(Outlet(event)), actor.StartError) {
  actor.new_with_initialiser(builder.start_timeout, fn(_default) {
    initialise(builder)
  })
  |> actor.on_message(handle_message)
  |> actor.start()
}

// nolint: stringly_typed_error -- the actor initialiser contract requires String errors
fn initialise(
  builder: Builder(state, event),
) -> Result(
  actor.Initialised(State(state, event), Message(state, event), Outlet(event)),
  String,
) {
  let emitter_subject = process.new_subject()
  let base_selector =
    process.new_selector()
    |> process.select_map(emitter_subject, FromEmitter)
  let faces = case builder.name {
    None -> {
      let subject = process.new_subject()
      let outlet =
        sluice.make_outlet(
          protocol.ProducerHandle(
            send: fn(message) { process.send(subject, message) },
            owner: fn() { process.subject_owner(subject) },
          ),
        )
      Ok(#(outlet, process.select_map(base_selector, subject, FromDownstream)))
    }
    Some(name) ->
      case process.register(process.self(), name.name) {
        Error(Nil) -> Error("name is already registered")
        Ok(Nil) -> {
          let subject = process.named_subject(name.name)
          Ok(#(
            outlet_of(name),
            process.select_map(base_selector, subject, FromDownstream),
          ))
        }
      }
  }
  case faces {
    Error(reason) -> Error(reason)
    Ok(#(outlet, selector)) -> {
      let selector = merge_user_messages(selector, builder.messages)
      case builder.initialise(Emitter(emitter_subject)) {
        Error(reason) -> Error(reason)
        Ok(user_state) -> {
          let core =
            producer_core.new(
              dispatcher.demand(),
              buffer.new(builder.capacity, builder.keep),
            )
          actor.initialised(State(
            user_state:,
            core:,
            selector:,
            on_demand: builder.on_demand,
            on_discard: builder.on_discard,
          ))
          |> actor.selecting(selector)
          |> actor.returning(outlet)
          |> Ok
        }
      }
    }
  }
}

fn merge_user_messages(
  selector: Selector(Message(state, event)),
  messages: Option(fn() -> Selector(Message(state, event))),
) -> Selector(Message(state, event)) {
  case messages {
    None -> selector
    Some(install) -> process.merge_selector(selector, install())
  }
}

fn handle_message(
  state: State(state, event),
  message: Message(state, event),
) -> actor.Next(State(state, event), Message(state, event)) {
  case message {
    FromDownstream(protocol.Subscribe(from, metadata)) -> {
      let #(core, selector, outbound) =
        producer_core.on_subscribe(
          state.core,
          state.selector,
          from,
          metadata,
          SubscriberDown,
        )
      producer_core.send_all(outbound)
      continue_with(State(..state, core:, selector:))
    }
    FromDownstream(protocol.Ask(from, demand)) -> {
      let #(core, outbound, unfilled) =
        producer_core.on_ask(state.core, from, demand)
      producer_core.send_all(outbound)
      let state = State(..state, core:)
      case unfilled > 0 {
        False -> actor.continue(state)
        True -> produce(state, unfilled)
      }
    }
    FromDownstream(protocol.Cancel(from)) -> {
      let producer_core.CancelResult(core:, selector:, outbound:) =
        producer_core.on_cancel(
          state.core,
          state.selector,
          from,
          acknowledge: True,
        )
      producer_core.send_all(outbound)
      continue_with(State(..state, core:, selector:))
    }
    SubscriberDown(from, _down) -> {
      let producer_core.CancelResult(core:, selector:, outbound:) =
        producer_core.on_cancel(
          state.core,
          state.selector,
          from,
          acknowledge: False,
        )
      producer_core.send_all(outbound)
      continue_with(State(..state, core:, selector:))
    }
    FromEmitter(Pushed(events)) -> {
      let producer_core.EmitResult(core:, outbound:, discarded:) =
        producer_core.emit(state.core, events)
      producer_core.send_all(outbound)
      let user_state =
        report_discards(state:, user_state: state.user_state, discarded:)
      actor.continue(State(..state, core:, user_state:))
    }
    FromEmitter(Finished) -> actor.stop()
    FromUser(apply) -> apply_produce(state, apply(state.user_state))
  }
}

fn produce(
  state: State(state, event),
  demand: Int,
) -> actor.Next(State(state, event), Message(state, event)) {
  apply_produce(state, state.on_demand(state.user_state, demand))
}

fn apply_produce(
  state: State(state, event),
  produced: Produce(state, event),
) -> actor.Next(State(state, event), Message(state, event)) {
  case produced {
    Emit(events, user_state) -> {
      let producer_core.EmitResult(core:, outbound:, discarded:) =
        producer_core.emit(state.core, events)
      producer_core.send_all(outbound)
      let user_state = report_discards(state:, user_state:, discarded:)
      actor.continue(State(..state, core:, user_state:))
    }
    Stop -> actor.stop()
    StopAbnormal(reason) -> actor.stop_abnormal(reason)
  }
}

fn continue_with(
  state: State(state, event),
) -> actor.Next(State(state, event), Message(state, event)) {
  actor.continue(state) |> actor.with_selector(state.selector)
}

fn report_discards(
  state state: State(state, event),
  user_state user_state: state,
  discarded discarded: Int,
) -> state {
  use <- bool.guard(when: discarded <= 0, return: user_state)
  state.on_discard(user_state, discarded)
}
