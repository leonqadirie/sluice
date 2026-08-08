//// A `Sink` is the end of a pipeline. It subscribes to the stages before
//// it and uses their events. Its speed controls the demand. Thus its speed
//// controls the event flow through the full pipeline.

import gleam/erlang/process.{type Down, type Selector, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/string
import sluice.{type Inlet, type Subscription, type SubscriptionOptions}
import sluice/internal/consumer_core
import sluice/internal/exit_reason
import sluice/internal/platform
import sluice/internal/protocol.{type ConsumerMessage}

/// The response of a sink after it processes a batch of events. Make it
/// with `continue`, `stop`, or `stop_abnormal`.
pub opaque type Next(state) {
  Continue(state: state)
  Stop
  StopAbnormal(reason: String)
}

pub fn continue(state: state) -> Next(state) {
  Continue(state)
}

/// Stop the sink with the normal reason. The stages before it see this
/// through their monitors. They release the open demand of the sink.
pub fn stop() -> Next(state) {
  Stop
}

pub fn stop_abnormal(reason: String) -> Next(state) {
  StopAbnormal(reason)
}

/// A permanent name for a sink. Make the name with `new_name`. Attach it
/// with `named`. Find the sink that operates with `inlet_of`.
pub opaque type Name(event) {
  Name(name: process.Name(sluice.ConsumerControl(event)))
}

pub fn new_name(prefix prefix: String) -> Name(event) {
  Name(process.new_name(prefix))
}

/// The process that has this name now, if a process has it.
pub fn whereis(name: Name(event)) -> Result(process.Pid, Nil) {
  process.named(name.name)
}

/// The inlet of the sink that has this name. The inlet stays correct
/// through restarts.
pub fn inlet_of(name: Name(event)) -> Inlet(event) {
  sluice.make_inlet(
    fn(control) { platform.send_named(name.name, control) },
    fn() { process.named(name.name) },
  )
}

pub opaque type Builder(state, event) {
  Builder(
    state: state,
    on_events: fn(state, List(event), Subscription) -> Next(state),
    name: Option(Name(event)),
    subscriptions: List(SubscriptionOptions(event)),
    start_timeout: Int,
    messages: Option(fn() -> Selector(Message(state, event))),
  )
}

/// Define a sink. The handler receives each batch of events and the
/// subscription that supplied the batch.
pub fn new(
  init state: state,
  on_events on_events: fn(state, List(event), Subscription) -> Next(state),
) -> Builder(state, event) {
  Builder(
    state:,
    on_events:,
    name: None,
    subscriptions: [],
    start_timeout: 5000,
    messages: None,
  )
}

/// Declare a subscription. The sink makes the connection during its start.
/// If the connection is not possible, the start fails. Under a supervisor,
/// the restart sequence then does the retry. Use this together with
/// `outlet_of` names for connections that continue through restarts.
pub fn subscribe(
  builder builder: Builder(state, event),
  options options: SubscriptionOptions(event),
) -> Builder(state, event) {
  Builder(..builder, subscriptions: [options, ..builder.subscriptions])
}

/// Give the sink a private message channel with a type of your choice.
/// At the start, `initialise` receives the subject of the channel: send it
/// to other processes, or start a timer with `process.send_after`. The
/// handler receives each message together with the state. Use this for
/// timers, for configuration changes, and for queries.
pub fn on_message(
  builder: Builder(state, event),
  initialise initialise: fn(Subject(user_message)) -> Nil,
  handler handler: fn(state, user_message) -> Next(state),
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

/// The maximum time for the start of the sink, which includes its
/// declared subscriptions. The default is 5000 milliseconds.
pub fn start_timeout(
  builder builder: Builder(state, event),
  milliseconds milliseconds: Int,
) -> Builder(state, event) {
  Builder(..builder, start_timeout: int.max(milliseconds, 1))
}

/// Attach a permanent name to the sink at its start.
pub fn named(
  builder builder: Builder(state, event),
  name name: Name(event),
) -> Builder(state, event) {
  Builder(..builder, name: Some(name))
}

type Message(state, event) {
  Control(control: sluice.ConsumerControl(event))
  FromUpstream(
    subject: Subject(ConsumerMessage(event)),
    message: ConsumerMessage(event),
  )
  ProducerDown(subject: Subject(ConsumerMessage(event)), down: Down)
  FromUser(apply: fn(state) -> Next(state))
}

type State(state, event) {
  State(
    user_state: state,
    core: consumer_core.Core(event),
    selector: Selector(Message(state, event)),
    control_subject: Subject(sluice.ConsumerControl(event)),
    on_events: fn(state, List(event), Subscription) -> Next(state),
  )
}

/// Start the sink. The returned data is its `Inlet`. You can subscribe it
/// to outlets.
pub fn start(
  builder: Builder(state, event),
) -> Result(actor.Started(Inlet(event)), actor.StartError) {
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
  actor.Initialised(State(state, event), Message(state, event), Inlet(event)),
  String,
) {
  let faces = case builder.name {
    None -> {
      let control_subject = process.new_subject()
      let inlet =
        sluice.make_inlet(
          fn(control) { process.send(control_subject, control) },
          fn() { process.subject_owner(control_subject) },
        )
      Ok(#(control_subject, inlet))
    }
    Some(name) ->
      case process.register(process.self(), name.name) {
        Error(Nil) -> Error("name is already registered")
        Ok(Nil) -> Ok(#(process.named_subject(name.name), inlet_of(name)))
      }
  }
  case faces {
    Error(reason) -> Error(reason)
    Ok(#(control_subject, inlet)) -> {
      let selector =
        process.new_selector() |> process.select_map(control_subject, Control)
      let selector = case builder.messages {
        None -> selector
        Some(install) -> process.merge_selector(selector, install())
      }
      let state =
        State(
          user_state: builder.state,
          core: consumer_core.new(),
          selector:,
          control_subject:,
          on_events: builder.on_events,
        )
      case establish_declared(state, list.reverse(builder.subscriptions)) {
        Error(reason) -> Error(reason)
        Ok(state) ->
          actor.initialised(state)
          |> actor.selecting(state.selector)
          |> actor.returning(inlet)
          |> Ok
      }
    }
  }
}

// nolint: stringly_typed_error -- feeds the actor initialiser, which requires String errors
fn establish_declared(
  state: State(state, event),
  subscriptions: List(SubscriptionOptions(event)),
) -> Result(State(state, event), String) {
  case subscriptions {
    [] -> Ok(state)
    [options, ..remaining] -> {
      let #(producer, min_demand, max_demand, cancel, metadata, mode) =
        sluice.options_fields(options)
      case
        consumer_core.add_subscription(
          state.core,
          state.selector,
          producer,
          min_demand:,
          max_demand:,
          cancel:,
          mode:,
          metadata:,
          tag_message: FromUpstream,
          tag_down: ProducerDown,
          make_cancel: cancel_closure(state.control_subject),
          make_ask: ask_closure(state.control_subject),
        )
      {
        Error(error) -> Error("could not subscribe: " <> string.inspect(error))
        Ok(#(core, selector, _handle)) ->
          establish_declared(State(..state, core:, selector:), remaining)
      }
    }
  }
}

fn cancel_closure(
  control_subject: Subject(sluice.ConsumerControl(event)),
) -> fn(Subject(ConsumerMessage(event))) -> fn() -> Nil {
  fn(subject) {
    fn() { process.send(control_subject, sluice.CancelSubscription(subject)) }
  }
}

fn ask_closure(
  control_subject: Subject(sluice.ConsumerControl(event)),
) -> fn(Subject(ConsumerMessage(event))) -> fn(Int) -> Nil {
  fn(subject) {
    fn(demand) {
      process.send(control_subject, sluice.RequestDemand(subject, demand))
    }
  }
}

fn handle_message(
  state: State(state, event),
  message: Message(state, event),
) -> actor.Next(State(state, event), Message(state, event)) {
  case message {
    Control(sluice.SubscribeTo(
      producer,
      min_demand,
      max_demand,
      cancel,
      metadata,
      mode,
      reply,
    )) -> {
      case
        consumer_core.add_subscription(
          state.core,
          state.selector,
          producer,
          min_demand:,
          max_demand:,
          cancel:,
          mode:,
          metadata:,
          tag_message: FromUpstream,
          tag_down: ProducerDown,
          make_cancel: cancel_closure(state.control_subject),
          make_ask: ask_closure(state.control_subject),
        )
      {
        Error(error) -> {
          process.send(reply, Error(error))
          actor.continue(state)
        }
        Ok(#(core, selector, handle)) -> {
          process.send(reply, Ok(handle))
          continue_with(State(..state, core:, selector:))
        }
      }
    }
    Control(sluice.CancelSubscription(subject)) -> {
      let core = consumer_core.request_cancel(state.core, subject)
      actor.continue(State(..state, core:))
    }
    Control(sluice.RequestDemand(subject, demand)) -> {
      let core =
        consumer_core.request_demand(core: state.core, subject:, demand:)
      actor.continue(State(..state, core:))
    }
    FromUpstream(subject, protocol.NewEvents(events)) -> {
      let #(core, user_state, outcome) =
        consumer_core.deliver(
          state.core,
          subject,
          events,
          state.user_state,
          handle_batch(state.on_events),
          fn(_state) { True },
        )
      let state = State(..state, core:, user_state:)
      case outcome {
        consumer_core.KeepRunning -> actor.continue(state)
        consumer_core.StopNormal -> actor.stop()
        consumer_core.StopAbnormal(reason) -> actor.stop_abnormal(reason)
      }
    }
    FromUpstream(subject, protocol.Cancelled(reason)) ->
      subscription_closed(state: state, subject: subject, reason: reason)
    ProducerDown(subject, down) ->
      case down {
        process.ProcessDown(_monitor, _pid, reason) ->
          subscription_closed(
            state: state,
            subject: subject,
            reason: exit_reason.classify(reason),
          )
        process.PortDown(..) -> actor.continue(state)
      }
    FromUser(apply) ->
      case apply(state.user_state) {
        Continue(user_state) -> actor.continue(State(..state, user_state:))
        Stop -> actor.stop()
        StopAbnormal(reason) -> actor.stop_abnormal(reason)
      }
  }
}

fn handle_batch(
  on_events: fn(state, List(event), Subscription) -> Next(state),
) -> fn(state, List(event), Subscription) -> consumer_core.BatchResult(state) {
  fn(user_state, batch, subscription) {
    case on_events(user_state, batch, subscription) {
      Continue(new_state) -> consumer_core.BatchContinue(new_state)
      Stop -> consumer_core.BatchStop(user_state)
      StopAbnormal(reason) ->
        consumer_core.BatchStopAbnormal(user_state, reason)
    }
  }
}

fn subscription_closed(
  state state: State(state, event),
  subject subject: Subject(ConsumerMessage(event)),
  reason reason: protocol.CancelReason,
) -> actor.Next(State(state, event), Message(state, event)) {
  let #(core, selector, outcome) =
    consumer_core.closed(state.core, state.selector, subject, reason)
  let state = State(..state, core:, selector:)
  case outcome {
    consumer_core.KeepRunning -> continue_with(state)
    consumer_core.StopNormal -> actor.stop()
    consumer_core.StopAbnormal(reason) -> actor.stop_abnormal(reason)
  }
}

fn continue_with(
  state: State(state, event),
) -> actor.Next(State(state, event), Message(state, event)) {
  actor.continue(state) |> actor.with_selector(state.selector)
}
