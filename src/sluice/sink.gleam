//// A `Sink` is the end of a pipeline. It subscribes to the stages before
//// it and uses their events. Its speed controls the demand. Thus its speed
//// controls the event flow through the full pipeline.

import gleam/erlang/process.{type Down, type Selector, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
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

/// Continue with the new state.
///
/// * `state`: The new state of the sink.
pub fn continue(state: state) -> Next(state) {
  Continue(state)
}

/// Stop the sink with the normal reason. The stages before it see this
/// through their monitors. They release the open demand of the sink.
pub fn stop() -> Next(state) {
  Stop
}

/// Stop the sink with a failure reason.
///
/// * `reason`: The description of the failure.
pub fn stop_abnormal(reason: String) -> Next(state) {
  StopAbnormal(reason)
}

/// The result of a `fold` that did not complete.
pub type FoldError {
  /// The internal sink did not start.
  FoldDidNotStart
  /// The subscription to the outlet was not possible.
  FoldDidNotSubscribe(reason: sluice.SubscribeError)
  /// The producer failed before the end of the flow.
  FoldProducerFailed(reason: String)
  /// The producer did not stop in the given time.
  FoldTimeout
}

/// Run the full flow of an outlet through a fold, and return the final
/// value when the producer stops. Use it together with `from_yielder` or
/// an other source that has an end. The wait has a limit of `within`
/// milliseconds.
///
/// * `outlet`: The outlet that supplies the events.
/// * `initial`: The first value of the accumulator.
/// * `combine`: The function that folds one event into the accumulator.
/// * `timeout`: The maximum wait time in milliseconds.
pub fn fold(
  from outlet: sluice.Outlet(event),
  initial initial: accumulated,
  with combine: fn(accumulated, event) -> accumulated,
  within timeout: Int,
) -> Result(accumulated, FoldError) {
  let reply = process.new_subject()
  let builder =
    new(init: initial, on_events: fn(accumulated, events, _subscription) {
      Continue(list.fold(events, accumulated, combine))
    })
    |> on_cancelled(fn(accumulated, end) {
      let result = case end {
        sluice.ProducerStopped -> Ok(accumulated)
        sluice.ProducerFailed(reason) -> Error(FoldProducerFailed(reason))
      }
      process.send(reply, result)
      Stop
    })
  case start(builder) {
    Error(_) -> Error(FoldDidNotStart)
    Ok(folder) -> wait_for_fold(folder:, outlet:, reply:, timeout:)
  }
}

fn wait_for_fold(
  folder folder: actor.Started(Inlet(event)),
  outlet outlet: sluice.Outlet(event),
  reply reply: Subject(Result(accumulated, FoldError)),
  timeout timeout: Int,
) -> Result(accumulated, FoldError) {
  let subscribed =
    sluice.subscription(to: outlet)
    |> sluice.subscribe(consumer: folder.data)
  case subscribed {
    Error(error) -> {
      stop_folder(folder)
      Error(FoldDidNotSubscribe(error))
    }
    Ok(_) ->
      case process.receive(reply, timeout) {
        Ok(result) -> result
        Error(Nil) -> {
          stop_folder(folder)
          Error(FoldTimeout)
        }
      }
  }
}

fn stop_folder(folder: actor.Started(Inlet(event))) -> Nil {
  process.unlink(folder.pid)
  process.kill(folder.pid)
}

/// A permanent name for a sink. Make the name with `new_name`. Attach it
/// with `named`. Find the sink that operates with `inlet_of`.
pub opaque type Name(event) {
  Name(name: process.Name(sluice.ConsumerControl(event)))
}

/// Make a permanent name. Create names during application start, not
/// inside a dynamic loop.
///
/// * `prefix`: The readable prefix of the name.
pub fn new_name(prefix prefix: String) -> Name(event) {
  Name(process.new_name(prefix))
}

/// The process that has this name now, if a process has it.
///
/// * `name`: The name of the sink.
pub fn whereis(name: Name(event)) -> Result(process.Pid, Nil) {
  process.named(name.name)
}

/// The inlet of the sink that has this name. The inlet stays correct
/// through restarts.
///
/// * `name`: The name of the sink.
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
    on_subscribed: fn(state, Subscription) -> Next(state),
    on_cancelled: Option(fn(state, sluice.SubscriptionEnd) -> Next(state)),
  )
}

/// Define a sink. The handler receives each batch of events and the
/// subscription that supplied the batch.
///
/// * `state`: The first state of the sink.
/// * `on_events`: The batch handler. It receives the state, one batch of
///   events, and the subscription that supplied the batch.
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
    on_subscribed: fn(state, _subscription) { Continue(state) },
    on_cancelled: None,
  )
}

/// Declare a subscription. The sink makes the connection during its
/// start. The start fails for the checks that the sink can make itself:
/// a dead producer, a duplicate, a subscription to itself, and demand
/// values that are not correct. Under a supervisor, the restart sequence
/// then does the retry. A refusal from the dispatcher of the producer,
/// for example a missing partition, comes later as an abnormal end of
/// the subscription. Use this together with `outlet_of` names for
/// connections that continue through restarts.
///
/// * `builder`: The builder to change.
/// * `options`: The subscription options, from `sluice.subscription`.
pub fn subscribe(
  builder builder: Builder(state, event),
  options options: SubscriptionOptions(event),
) -> Builder(state, event) {
  Builder(..builder, subscriptions: [options, ..builder.subscriptions])
}

/// Set a hook that runs when the sink establishes a subscription. The
/// hook receives the new `Subscription`, so a sink with manual demand can
/// make its first ask here.
///
/// * `builder`: The builder to change.
/// * `on_subscribed`: The hook. It receives the state and the new
///   `Subscription`.
pub fn on_subscribed(
  builder: Builder(state, event),
  on_subscribed: fn(state, Subscription) -> Next(state),
) -> Builder(state, event) {
  Builder(..builder, on_subscribed:)
}

/// Set a hook that runs when a subscription of the sink ends. When this
/// hook is set, it decides what the sink does, and the cancel mode of the
/// subscription does not apply.
///
/// * `builder`: The builder to change.
/// * `on_cancelled`: The hook. It receives the state and the
///   `SubscriptionEnd` that gives the cause.
pub fn on_cancelled(
  builder: Builder(state, event),
  on_cancelled: fn(state, sluice.SubscriptionEnd) -> Next(state),
) -> Builder(state, event) {
  Builder(..builder, on_cancelled: Some(on_cancelled))
}

/// Give the sink a private message channel with a type of your choice.
/// At the start, `initialise` receives the subject of the channel: send it
/// to other processes, or start a timer with `process.send_after`. The
/// handler receives each message together with the state. Use this for
/// timers, for configuration changes, and for queries.
///
/// * `builder`: The builder to change.
/// * `initialise`: The function that receives the subject of the channel
///   at the start of the sink.
/// * `handler`: The message handler. It receives the state and one
///   message, and it returns a `Next`.
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
///
/// * `builder`: The builder to change.
/// * `milliseconds`: The maximum start time in milliseconds.
pub fn start_timeout(
  builder builder: Builder(state, event),
  milliseconds milliseconds: Int,
) -> Builder(state, event) {
  Builder(..builder, start_timeout: int.max(milliseconds, 1))
}

/// Attach a permanent name to the sink at its start.
///
/// * `builder`: The builder to change.
/// * `name`: The name, from `new_name`.
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
    on_subscribed: fn(state, Subscription) -> Next(state),
    on_cancelled: Option(fn(state, sluice.SubscriptionEnd) -> Next(state)),
  )
}

/// Start the sink. The returned data is its `Inlet`. You can subscribe it
/// to outlets.
///
/// * `builder`: The configuration of the sink.
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
  use #(control_subject, inlet) <- result.try(select_faces(builder))
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
      on_subscribed: builder.on_subscribed,
      on_cancelled: builder.on_cancelled,
    )
  use state <- result.try(establish_declared(
    state,
    list.reverse(builder.subscriptions),
  ))
  actor.initialised(state)
  |> actor.selecting(state.selector)
  |> actor.returning(inlet)
  |> Ok
}

// The control face of the sink: a plain subject, or the registered name.
// nolint: stringly_typed_error -- feeds the actor initialiser, which requires String errors
fn select_faces(
  builder: Builder(state, event),
) -> Result(#(Subject(sluice.ConsumerControl(event)), Inlet(event)), String) {
  case builder.name {
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
}

// nolint: stringly_typed_error -- feeds the actor initialiser, which requires String errors
fn establish_declared(
  state: State(state, event),
  subscriptions: List(SubscriptionOptions(event)),
) -> Result(State(state, event), String) {
  case subscriptions {
    [] -> Ok(state)
    [options, ..remaining] -> {
      let sluice.OptionsFields(
        producer:,
        min_demand:,
        max_demand:,
        cancel:,
        metadata:,
        mode:,
      ) = sluice.options_fields(options)
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
          reply: option.None,
          tag_message: FromUpstream,
          tag_down: ProducerDown,
          make_cancel: cancel_closure(state.control_subject),
          make_ask: ask_closure(state.control_subject),
        )
      {
        Error(error) -> Error("could not subscribe: " <> string.inspect(error))
        Ok(#(core, selector, _subject, handle)) -> {
          let state = State(..state, core:, selector:)
          declared_subscribed(state:, handle:, remaining:)
        }
      }
    }
  }
}

// nolint: stringly_typed_error -- feeds the actor initialiser, which requires String errors
fn declared_subscribed(
  state state: State(state, event),
  handle handle: Subscription,
  remaining remaining: List(SubscriptionOptions(event)),
) -> Result(State(state, event), String) {
  case state.on_subscribed(state.user_state, handle) {
    Continue(user_state) ->
      establish_declared(State(..state, user_state:), remaining)
    Stop -> Error("the sink stopped in on_subscribed")
    StopAbnormal(reason) -> Error(reason)
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
          reply: option.Some(reply),
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
        Ok(#(core, selector, _subject, handle)) -> {
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
    Control(sluice.ConfirmSubscribe(reply, confirmed, deadline)) ->
      confirm_subscription(state:, reply:, confirmed:, deadline:)
    Control(sluice.AbandonSubscribe(reply)) -> {
      let #(core, selector) =
        consumer_core.abandon(state.core, state.selector, reply)
      continue_with(State(..state, core:, selector:))
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
    FromUser(apply) -> apply_next(state, apply(state.user_state))
  }
}

fn confirm_subscription(
  state state: State(state, event),
  reply reply: Subject(Result(Subscription, sluice.SubscribeError)),
  confirmed confirmed: Subject(Nil),
  deadline deadline: Int,
) -> actor.Next(State(state, event), Message(state, event)) {
  let #(core, selector, handle) = case
    platform.monotonic_milliseconds() < deadline
  {
    True -> {
      let #(core, confirmed) = consumer_core.confirm(core: state.core, reply:)
      let handle = option.map(confirmed, fn(pair) { pair.1 })
      #(core, state.selector, handle)
    }
    False -> {
      let #(core, selector) =
        consumer_core.abandon(state.core, state.selector, reply)
      #(core, selector, None)
    }
  }
  let state = State(..state, core:, selector:)
  process.send(confirmed, Nil)
  case handle {
    None -> continue_with(state)
    Some(handle) ->
      apply_next(state, state.on_subscribed(state.user_state, handle))
  }
}

fn apply_next(
  state: State(state, event),
  next: Next(state),
) -> actor.Next(State(state, event), Message(state, event)) {
  case next {
    Continue(user_state) -> continue_with(State(..state, user_state:))
    Stop -> actor.stop()
    StopAbnormal(reason) -> actor.stop_abnormal(reason)
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
  case state.on_cancelled {
    Some(on_cancelled) ->
      apply_next(
        state,
        on_cancelled(state.user_state, subscription_end(reason)),
      )
    None ->
      case outcome {
        consumer_core.KeepRunning -> continue_with(state)
        consumer_core.StopNormal -> actor.stop()
        consumer_core.StopAbnormal(reason) -> actor.stop_abnormal(reason)
      }
  }
}

fn subscription_end(reason: protocol.CancelReason) -> sluice.SubscriptionEnd {
  case reason {
    protocol.Normal -> sluice.ProducerStopped
    protocol.Shutdown -> sluice.ProducerStopped
    protocol.Abnormal(reason) -> sluice.ProducerFailed(reason)
  }
}

fn continue_with(
  state: State(state, event),
) -> actor.Next(State(state, event), Message(state, event)) {
  actor.continue(state) |> actor.with_selector(state.selector)
}
