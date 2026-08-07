//// A `Gate` is in the middle of a pipeline. It uses events from the stages
//// before it. It changes the events. It makes the results for the stages
//// after it. Its two faces are connected: the gate asks for more events
//// only while the stages after it ask for events. Thus backpressure goes
//// through the gate.

import gleam/erlang/process.{type Down, type Selector, type Subject}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/string
import sluice.{
  type Inlet, type Keep, type Outlet, type Subscription,
  type SubscriptionOptions,
}
import sluice/internal/buffer
import sluice/internal/consumer_core
import sluice/internal/dispatcher
import sluice/internal/exit_reason
import sluice/internal/platform
import sluice/internal/producer_core
import sluice/internal/protocol.{type ConsumerMessage, type ProducerMessage}

/// The response of a gate to a batch of events. Make it with `emit`,
/// `stop`, or `stop_abnormal`.
pub opaque type Transform(state, out) {
  Emit(events: List(out), state: state)
  Stop
  StopAbnormal(reason: String)
}

/// Emit the changed events. The quantity of output events is free. A
/// filter makes fewer events. An expander makes more events. The buffer
/// keeps the events that are more than the demand after the gate.
pub fn emit(
  events events: List(out),
  state state: state,
) -> Transform(state, out) {
  Emit(events, state)
}

/// Stop the gate with the normal reason.
pub fn stop() -> Transform(state, out) {
  Stop
}

/// Stop the gate with a failure reason.
pub fn stop_abnormal(reason: String) -> Transform(state, out) {
  StopAbnormal(reason)
}

/// The two faces of a gate that operates. Subscribe its `inlet` to the
/// outlets before it. Subscribe the subsequent inlets to its `outlet`.
pub opaque type Gate(in, out) {
  Gate(inlet: Inlet(in), outlet: Outlet(out))
}

pub fn inlet(gate: Gate(in, out)) -> Inlet(in) {
  gate.inlet
}

pub fn outlet(gate: Gate(in, out)) -> Outlet(out) {
  gate.outlet
}

/// The messages that come through the registered name of a gate, for its
/// two faces.
type NamedMessage(in, out) {
  NamedControl(control: sluice.ConsumerControl(in))
  NamedProducer(message: ProducerMessage(out))
}

/// A permanent name for a gate. It gives the two faces addresses that stay
/// correct through restarts: `inlet_of` and `outlet_of`.
pub opaque type Name(in, out) {
  Name(name: process.Name(NamedMessage(in, out)))
}

pub fn new_name(prefix prefix: String) -> Name(in, out) {
  Name(process.new_name(prefix))
}

/// The process that has this name now, if a process has it.
pub fn whereis(name: Name(in, out)) -> Result(process.Pid, Nil) {
  process.named(name.name)
}

/// The input face of the gate that has this name. It stays correct through
/// restarts.
pub fn inlet_of(name: Name(in, out)) -> Inlet(in) {
  sluice.make_inlet(
    fn(control) { send_named(name, NamedControl(control)) },
    fn() { process.named(name.name) },
  )
}

/// The output face of the gate that has this name. It stays correct
/// through restarts.
pub fn outlet_of(name: Name(in, out)) -> Outlet(out) {
  sluice.make_outlet(
    protocol.ProducerHandle(
      send: fn(message) { send_named(name, NamedProducer(message)) },
      owner: fn() { process.named(name.name) },
    ),
  )
}

// While a stage restarts, its name can be free for a short time.
// Messages that you send in this time are lost. This is the same as
// messages to a dead pid. The monitor of the other side finds the loss.
fn send_named(name: Name(in, out), message: NamedMessage(in, out)) -> Nil {
  platform.send_named(name.name, message)
}

pub opaque type Builder(state, in, out) {
  Builder(
    state: state,
    on_events: fn(state, List(in), Subscription) -> Transform(state, out),
    capacity: buffer.Capacity,
    keep: Keep,
    name: Option(Name(in, out)),
    subscriptions: List(SubscriptionOptions(in)),
    start_timeout: Int,
    on_discard: fn(state, Int) -> state,
  )
}

/// Define a gate. The handler receives each batch and the subscription
/// that supplied the batch. The handler returns the changed events.
pub fn new(
  init state: state,
  on_events on_events: fn(state, List(in), Subscription) ->
    Transform(state, out),
) -> Builder(state, in, out) {
  Builder(
    state:,
    on_events:,
    capacity: buffer.Unbounded,
    keep: sluice.KeepLast,
    name: None,
    subscriptions: [],
    start_timeout: 5000,
    on_discard: default_on_discard,
  )
}

// The default response to discarded events is a log warning.
fn default_on_discard(state: state, count: Int) -> state {
  platform.log_warning(
    "sluice: a gate discarded "
    <> int.to_string(count)
    <> " events because its buffer was full",
  )
  state
}

/// Set the response of the gate to discarded events. The callback receives
/// the state and the quantity of discarded events. The default response
/// writes a warning to the log.
pub fn on_discard(
  builder: Builder(state, in, out),
  on_discard: fn(state, Int) -> state,
) -> Builder(state, in, out) {
  Builder(..builder, on_discard:)
}

/// The maximum time for the start of the gate, which includes its declared
/// subscriptions. The default is 5000 milliseconds.
pub fn start_timeout(
  builder builder: Builder(state, in, out),
  milliseconds milliseconds: Int,
) -> Builder(state, in, out) {
  Builder(..builder, start_timeout: int.max(milliseconds, 1))
}

/// Declare a subscription. The gate makes the connection during its start.
/// If the connection is not possible, the start fails.
pub fn subscribe(
  builder builder: Builder(state, in, out),
  options options: SubscriptionOptions(in),
) -> Builder(state, in, out) {
  Builder(..builder, subscriptions: [options, ..builder.subscriptions])
}

/// Set a limit for the output buffer of the gate. The default is no limit.
/// The demand connection between the two faces keeps the buffer small. The
/// maximum content is approximately the sum of the `max_demand` values of
/// the subscriptions before the gate. Thus the gate discards nothing.
pub fn buffer_capacity(
  builder builder: Builder(state, in, out),
  events capacity: Int,
) -> Builder(state, in, out) {
  Builder(..builder, capacity: buffer.Bounded(capacity))
}

/// Select the events that stay when a buffer with a limit is full.
pub fn buffer_keep(
  builder builder: Builder(state, in, out),
  keep keep: Keep,
) -> Builder(state, in, out) {
  Builder(..builder, keep:)
}

/// Attach a permanent name to the gate at its start.
pub fn named(
  builder builder: Builder(state, in, out),
  name name: Name(in, out),
) -> Builder(state, in, out) {
  Builder(..builder, name: Some(name))
}

type Message(in, out) {
  Control(control: sluice.ConsumerControl(in))
  FromUpstream(
    subject: Subject(ConsumerMessage(in)),
    message: ConsumerMessage(in),
  )
  UpstreamDown(subject: Subject(ConsumerMessage(in)), down: Down)
  FromDownstream(message: ProducerMessage(out))
  SubscriberDown(from: Subject(ConsumerMessage(out)), down: Down)
}

type State(state, in, out) {
  State(
    user_state: state,
    consumer: consumer_core.Core(in),
    producer: producer_core.Core(out),
    selector: Selector(Message(in, out)),
    control_subject: Subject(sluice.ConsumerControl(in)),
    on_events: fn(state, List(in), Subscription) -> Transform(state, out),
    on_discard: fn(state, Int) -> state,
  )
}

/// Start the gate. The returned data is the pair of faces. Connect them
/// with `inlet` and `outlet`.
pub fn start(
  builder: Builder(state, in, out),
) -> Result(actor.Started(Gate(in, out)), actor.StartError) {
  actor.new_with_initialiser(builder.start_timeout, fn(_default) {
    initialise(builder)
  })
  |> actor.on_message(handle_message)
  |> actor.start()
}

// nolint: stringly_typed_error -- the actor initialiser contract requires String errors
fn initialise(
  builder: Builder(state, in, out),
) -> Result(
  actor.Initialised(State(state, in, out), Message(in, out), Gate(in, out)),
  String,
) {
  let control_subject = process.new_subject()
  let producer_subject = process.new_subject()
  let selector =
    process.new_selector()
    |> process.select_map(control_subject, Control)
    |> process.select_map(producer_subject, FromDownstream)
  let selector = case builder.name {
    None -> Ok(selector)
    Some(name) ->
      case process.register(process.self(), name.name) {
        Error(Nil) -> Error("name is already registered")
        Ok(Nil) ->
          Ok(
            process.select_map(
              selector,
              process.named_subject(name.name),
              fn(named_message) {
                case named_message {
                  NamedControl(control) -> Control(control)
                  NamedProducer(message) -> FromDownstream(message)
                }
              },
            ),
          )
      }
  }
  case selector {
    Error(reason) -> Error(reason)
    Ok(selector) -> {
      let gate =
        Gate(
          inlet: sluice.make_inlet(
            fn(control) { process.send(control_subject, control) },
            fn() { process.subject_owner(control_subject) },
          ),
          outlet: sluice.make_outlet(
            protocol.ProducerHandle(
              send: fn(message) { process.send(producer_subject, message) },
              owner: fn() { process.subject_owner(producer_subject) },
            ),
          ),
        )
      let state =
        State(
          user_state: builder.state,
          consumer: consumer_core.new(),
          producer: producer_core.new(
            dispatcher.demand(),
            buffer.new(builder.capacity, builder.keep),
          ),
          selector:,
          control_subject:,
          on_events: builder.on_events,
          on_discard: builder.on_discard,
        )
      case establish_declared(state, list.reverse(builder.subscriptions)) {
        Error(reason) -> Error(reason)
        Ok(state) ->
          actor.initialised(state)
          |> actor.selecting(state.selector)
          |> actor.returning(gate)
          |> Ok
      }
    }
  }
}

// nolint: stringly_typed_error -- feeds the actor initialiser, which requires String errors
fn establish_declared(
  state: State(state, in, out),
  subscriptions: List(SubscriptionOptions(in)),
) -> Result(State(state, in, out), String) {
  case subscriptions {
    [] -> Ok(state)
    [options, ..remaining] -> {
      let #(producer, min_demand, max_demand, cancel, metadata) =
        sluice.options_fields(options)
      case
        consumer_core.add_subscription(
          state.consumer,
          state.selector,
          producer,
          min_demand:,
          max_demand:,
          cancel:,
          metadata:,
          tag_message: FromUpstream,
          tag_down: UpstreamDown,
          make_cancel: cancel_closure(state.control_subject),
        )
      {
        Error(error) -> Error("could not subscribe: " <> string.inspect(error))
        Ok(#(consumer, selector, _handle)) ->
          establish_declared(State(..state, consumer:, selector:), remaining)
      }
    }
  }
}

fn cancel_closure(
  control_subject: Subject(sluice.ConsumerControl(in)),
) -> fn(Subject(ConsumerMessage(in))) -> fn() -> Nil {
  fn(subject) {
    fn() { process.send(control_subject, sluice.CancelSubscription(subject)) }
  }
}

fn handle_message(
  state: State(state, in, out),
  message: Message(in, out),
) -> actor.Next(State(state, in, out), Message(in, out)) {
  case message {
    Control(sluice.SubscribeTo(
      producer,
      min_demand,
      max_demand,
      cancel,
      metadata,
      reply,
    )) ->
      case
        consumer_core.add_subscription(
          state.consumer,
          state.selector,
          producer,
          min_demand:,
          max_demand:,
          cancel:,
          metadata:,
          tag_message: FromUpstream,
          tag_down: UpstreamDown,
          make_cancel: cancel_closure(state.control_subject),
        )
      {
        Error(error) -> {
          process.send(reply, Error(error))
          actor.continue(state)
        }
        Ok(#(consumer, selector, handle)) -> {
          process.send(reply, Ok(handle))
          continue_with(State(..state, consumer:, selector:))
        }
      }
    Control(sluice.CancelSubscription(subject)) -> {
      let consumer = consumer_core.request_cancel(state.consumer, subject)
      actor.continue(State(..state, consumer:))
    }
    FromUpstream(subject, protocol.NewEvents(events)) ->
      transform(state: state, subject: subject, events: events)
    FromUpstream(subject, protocol.Cancelled(reason)) ->
      upstream_closed(state: state, subject: subject, reason: reason)
    UpstreamDown(subject, down) ->
      case down {
        process.ProcessDown(_monitor, _pid, reason) ->
          upstream_closed(
            state: state,
            subject: subject,
            reason: exit_reason.classify(reason),
          )
        process.PortDown(..) -> actor.continue(state)
      }
    FromDownstream(protocol.Subscribe(from, metadata)) -> {
      let #(producer, selector, outbound) =
        producer_core.on_subscribe(
          state.producer,
          state.selector,
          from,
          metadata,
          SubscriberDown,
        )
      producer_core.send_all(outbound)
      continue_with(State(..state, producer:, selector:))
    }
    FromDownstream(protocol.Ask(from, demand)) -> {
      // A gate does not make events on demand. It is sufficient to record
      // the demand and to send events from the buffer. New demand after
      // the gate also releases the asks that the gate kept.
      let #(producer, outbound, _unfilled) =
        producer_core.on_ask(state.producer, from, demand)
      producer_core.send_all(outbound)
      let appetite =
        producer_core.has_demand(producer)
        || producer_core.buffer_is_empty(producer)
      let consumer = case appetite {
        True -> consumer_core.flush_held_asks(state.consumer)
        False -> state.consumer
      }
      actor.continue(State(..state, producer:, consumer:))
    }
    FromDownstream(protocol.Cancel(from)) -> {
      let producer_core.CancelResult(core: producer, selector:, outbound:) =
        producer_core.on_cancel(
          state.producer,
          state.selector,
          from,
          acknowledge: True,
        )
      producer_core.send_all(outbound)
      continue_with(State(..state, producer:, selector:))
    }
    SubscriberDown(from, _down) -> {
      let producer_core.CancelResult(core: producer, selector:, outbound:) =
        producer_core.on_cancel(
          state.producer,
          state.selector,
          from,
          acknowledge: False,
        )
      producer_core.send_all(outbound)
      continue_with(State(..state, producer:, selector:))
    }
  }
}

fn transform(
  state state: State(state, in, out),
  subject subject: Subject(ConsumerMessage(in)),
  events events: List(in),
) -> actor.Next(State(state, in, out), Message(in, out)) {
  let on_events = state.on_events
  let on_discard = state.on_discard
  let #(consumer, #(user_state, producer), outcome) =
    consumer_core.deliver(
      state.consumer,
      subject,
      events,
      #(state.user_state, state.producer),
      fn(folded, batch, subscription) {
        let #(user_state, producer) = folded
        case on_events(user_state, batch, subscription) {
          Emit(produced, user_state) -> {
            let producer_core.EmitResult(core: producer, outbound:, discarded:) =
              producer_core.emit(producer, produced)
            producer_core.send_all(outbound)
            let user_state = case discarded > 0 {
              True -> on_discard(user_state, discarded)
              False -> user_state
            }
            consumer_core.BatchContinue(#(user_state, producer))
          }
          Stop -> consumer_core.BatchStop(folded)
          StopAbnormal(reason) ->
            consumer_core.BatchStopAbnormal(folded, reason)
        }
      },
      // Ask the stages before the gate again only in one of two
      // conditions: the stages after the gate ask for events, or the
      // buffer is empty. If not, keep the ask.
      fn(folded) {
        let #(_user_state, producer) = folded
        producer_core.has_demand(producer)
        || producer_core.buffer_is_empty(producer)
      },
    )
  let state = State(..state, consumer:, producer:, user_state:)
  case outcome {
    consumer_core.KeepRunning -> actor.continue(state)
    consumer_core.StopNormal -> actor.stop()
    consumer_core.StopAbnormal(reason) -> actor.stop_abnormal(reason)
  }
}

fn upstream_closed(
  state state: State(state, in, out),
  subject subject: Subject(ConsumerMessage(in)),
  reason reason: protocol.CancelReason,
) -> actor.Next(State(state, in, out), Message(in, out)) {
  let #(consumer, selector, outcome) =
    consumer_core.closed(state.consumer, state.selector, subject, reason)
  let state = State(..state, consumer:, selector:)
  case outcome {
    consumer_core.KeepRunning -> continue_with(state)
    consumer_core.StopNormal -> actor.stop()
    consumer_core.StopAbnormal(reason) -> actor.stop_abnormal(reason)
  }
}

fn continue_with(
  state: State(state, in, out),
) -> actor.Next(State(state, in, out), Message(in, out)) {
  actor.continue(state) |> actor.with_selector(state.selector)
}
