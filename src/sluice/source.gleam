//// A `Source` is the start of a pipeline. It makes events when the stages
//// after it ask for them. Different processes can also push events into it
//// through an `Emitter`.

import gleam/bool
import gleam/erlang/process.{type Down, type Selector, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/yielder.{type Yielder}
import sluice.{type Keep, type Outlet}
import sluice/dispatcher.{type Dispatcher}
import sluice/internal/buffer
import sluice/internal/platform
import sluice/internal/producer_core
import sluice/internal/protocol.{type ConsumerMessage, type ProducerMessage}

/// The response of a source to demand. Make it with `emit`, `stop`, or
/// `stop_abnormal`.
pub opaque type Produce(state, event) {
  Emit(events: List(event), state: state)
  EmitFinal(events: List(event))
  Stop
  StopAbnormal(reason: String)
}

/// Emit events for the demand. The quantity of events can be more than
/// the demand: the buffer keeps the extra events. The quantity can also
/// be less than the demand, or zero. The demand that stays open is then
/// supplied by later events, from a push through an `Emitter` or from a
/// message handler that emits.
///
/// Emit less than the demand only when such later events can come:
/// without them, the consumers wait for the open demand and do not ask
/// again. A source that ends must end with `emit_final`.
pub fn emit(
  events events: List(event),
  state state: state,
) -> Produce(state, event) {
  Emit(events, state)
}

/// Emit the last events, and then stop the source with the normal
/// reason. Use this when the source ends in the middle of a demand: the
/// events go out first, and then the subscribers apply their cancel
/// modes.
pub fn emit_final(events: List(event)) -> Produce(state, event) {
  EmitFinal(events)
}

/// Stop the source with the normal reason. The subscribers apply their
/// cancel modes.
///
/// The demand handler runs again only when a consumer asks again, and a
/// consumer asks again only after it received its full demand. Thus
/// `stop` can end the source only after emits that filled the demand
/// completely. When the events end in the middle of a demand, use
/// `emit_final` in that same call.
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
    dispatcher: Dispatcher(event),
    on_subscribers: fn(state, sluice.SubscriberChange) -> state,
    accumulate: Bool,
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

fn default_builder(
  initialise initialise: fn(Emitter(event)) -> Result(state, String),
  on_demand on_demand: fn(state, Int) -> Produce(state, event),
) -> Builder(state, event) {
  Builder(
    initialise:,
    on_demand:,
    capacity: buffer.Bounded(10_000),
    keep: sluice.KeepLast,
    name: None,
    start_timeout: 5000,
    on_discard: default_on_discard,
    messages: None,
    dispatcher: dispatcher.demand(),
    on_subscribers: fn(state, _change) { state },
    accumulate: False,
  )
}

/// Define a source that makes events on demand.
pub fn new(
  init state: state,
  on_demand on_demand: fn(state, Int) -> Produce(state, event),
) -> Builder(state, event) {
  default_builder(initialise: fn(_emitter) { Ok(state) }, on_demand:)
}

/// Define a source that receives its events from an external location. The
/// initialiser receives an `Emitter`. Each process can `push` events
/// through the `Emitter`. A source that only receives pushes can keep the
/// default `on_demand`. If the source can also make events on request, set
/// a handler with `on_demand`.
pub fn new_with_emitter(
  init initialise: fn(Emitter(event)) -> Result(state, String),
) -> Builder(state, event) {
  default_builder(initialise:, on_demand: fn(state, _demand) { Emit([], state) })
}

/// Define a source that takes its events from a yielder. The source
/// steps the yielder as far as the demand asks. It stops with the normal
/// reason at the end of the yielder.
pub fn from_yielder(
  yielder yielder: Yielder(event),
) -> Builder(Yielder(event), event) {
  new(init: yielder, on_demand: fn(remaining, demand) {
    step_yielder(remaining:, count: demand, taken: [])
  })
}

fn step_yielder(
  remaining remaining: Yielder(event),
  count count: Int,
  taken taken: List(event),
) -> Produce(Yielder(event), event) {
  use <- bool.guard(
    when: count <= 0,
    return: Emit(list.reverse(taken), remaining),
  )
  case yielder.step(remaining) {
    yielder.Done ->
      case taken {
        [] -> Stop
        _ -> EmitFinal(list.reverse(taken))
      }
    yielder.Next(event, remaining) ->
      step_yielder(remaining:, count: count - 1, taken: [event, ..taken])
  }
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

/// Start the source in the accumulation mode: incoming asks wait, and
/// the source makes no events. A call of `sluice.forward_demand` on the
/// outlet releases the held asks. Use this to connect a full pipeline
/// before the events start to move.
pub fn accumulate_demand(
  builder: Builder(state, event),
) -> Builder(state, event) {
  Builder(..builder, accumulate: True)
}

/// Set a hook that runs when a subscriber arrives or leaves. Use it, for
/// example, to start work at the first subscriber and to stop work at the
/// last one.
pub fn on_subscribers(
  builder: Builder(state, event),
  on_subscribers: fn(state, sluice.SubscriberChange) -> state,
) -> Builder(state, event) {
  Builder(..builder, on_subscribers:)
}

/// Set the dispatcher of the source. The default is the demand
/// dispatcher.
pub fn dispatcher(
  builder builder: Builder(state, event),
  dispatcher dispatcher: Dispatcher(event),
) -> Builder(state, event) {
  Builder(..builder, dispatcher:)
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
  DeferredDemand(demand: Int)
}

type State(state, event) {
  State(
    user_state: state,
    core: producer_core.Core(event),
    selector: Selector(Message(state, event)),
    deferred_demand: Subject(Int),
    on_demand: fn(state, Int) -> Produce(state, event),
    on_discard: fn(state, Int) -> state,
    on_subscribers: fn(state, sluice.SubscriberChange) -> state,
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
  let deferred_demand = process.new_subject()
  let base_selector =
    process.new_selector()
    |> process.select_map(emitter_subject, FromEmitter)
    |> process.select_map(deferred_demand, DeferredDemand)
  use #(outlet, selector) <- result.try(select_faces(builder, base_selector))
  let selector = merge_user_messages(selector, builder.messages)
  use user_state <- result.try(builder.initialise(Emitter(emitter_subject)))
  let core = build_core(builder)
  actor.initialised(State(
    user_state:,
    core:,
    selector:,
    deferred_demand:,
    on_demand: builder.on_demand,
    on_discard: builder.on_discard,
    on_subscribers: builder.on_subscribers,
  ))
  |> actor.selecting(selector)
  |> actor.returning(outlet)
  |> Ok
}

// The producer face of the source: a plain subject, or the registered
// name.
// nolint: stringly_typed_error -- feeds the actor initialiser, which requires String errors
fn select_faces(
  builder: Builder(state, event),
  base_selector: Selector(Message(state, event)),
) -> Result(#(Outlet(event), Selector(Message(state, event))), String) {
  case builder.name {
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
}

fn build_core(builder: Builder(state, event)) -> producer_core.Core(event) {
  let core =
    producer_core.new(
      dispatcher.build(builder.dispatcher),
      buffer.new(builder.capacity, builder.keep),
    )
  case builder.accumulate {
    True -> producer_core.accumulate(core)
    False -> core
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
      let before = producer_core.subscriber_count(state.core)
      let #(core, selector, outbound) =
        producer_core.on_subscribe(
          state.core,
          state.selector,
          from,
          metadata,
          SubscriberDown,
        )
      producer_core.send_all(outbound)
      let count = producer_core.subscriber_count(core)
      let user_state = case count > before {
        True ->
          state.on_subscribers(
            state.user_state,
            sluice.SubscriberArrived(count),
          )
        False -> state.user_state
      }
      continue_with(State(..state, core:, selector:, user_state:))
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
    FromDownstream(protocol.ForwardDemand) -> {
      let #(core, outbound, unfilled) = producer_core.forward(state.core)
      producer_core.send_all(outbound)
      let state = State(..state, core:)
      case unfilled > 0 {
        False -> actor.continue(state)
        True -> produce(state, unfilled)
      }
    }
    FromDownstream(protocol.Cancel(from)) ->
      subscriber_gone(state:, from:, acknowledge: True)
    SubscriberDown(from, _down) ->
      subscriber_gone(state:, from:, acknowledge: False)
    FromEmitter(Pushed(events)) -> {
      let producer_core.EmitResult(core:, outbound:, discarded:, unfilled:) =
        producer_core.emit(state.core, events)
      producer_core.send_all(outbound)
      let user_state =
        report_discards(state:, user_state: state.user_state, discarded:)
      let state = State(..state, core:, user_state:)
      defer_demand(state, unfilled)
    }
    FromEmitter(Finished) -> actor.stop()
    FromUser(apply) -> apply_produce(state, apply(state.user_state))
    DeferredDemand(demand) -> produce(state, demand)
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
      let producer_core.EmitResult(core:, outbound:, discarded:, unfilled:) =
        producer_core.emit(state.core, events)
      producer_core.send_all(outbound)
      let user_state = report_discards(state:, user_state:, discarded:)
      let state = State(..state, core:, user_state:)
      // Filters on broadcast subscriptions can release replacement
      // demand. Defer it through the actor mailbox so a selector that
      // rejects every event cannot monopolise this process.
      defer_demand(state, unfilled)
    }
    EmitFinal(events) -> {
      let producer_core.EmitResult(outbound:, ..) =
        producer_core.emit(state.core, events)
      producer_core.send_all(outbound)
      actor.stop()
    }
    Stop -> actor.stop()
    StopAbnormal(reason) -> actor.stop_abnormal(reason)
  }
}

fn defer_demand(
  state: State(state, event),
  demand: Int,
) -> actor.Next(State(state, event), Message(state, event)) {
  case demand > 0 {
    False -> actor.continue(state)
    True -> {
      process.send(state.deferred_demand, demand)
      actor.continue(state)
    }
  }
}

// A removal can release demand (a broadcast dispatcher moves at the speed
// of its slowest subscriber). Make new events for the released demand.
fn subscriber_gone(
  state state: State(state, event),
  from from: Subject(ConsumerMessage(event)),
  acknowledge acknowledge: Bool,
) -> actor.Next(State(state, event), Message(state, event)) {
  let before = producer_core.subscriber_count(state.core)
  let producer_core.CancelResult(core:, selector:, outbound:, freed:) =
    producer_core.on_cancel(state.core, state.selector, from, acknowledge:)
  producer_core.send_all(outbound)
  let count = producer_core.subscriber_count(core)
  let user_state = case count < before {
    True -> state.on_subscribers(state.user_state, sluice.SubscriberLeft(count))
    False -> state.user_state
  }
  let state = State(..state, core:, selector:, user_state:)
  case freed > 0 {
    False -> continue_with(state)
    True -> produce(state, freed) |> actor.with_selector(selector)
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
