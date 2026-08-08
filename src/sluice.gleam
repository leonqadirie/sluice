//// Stage pipelines with demand control and backpressure.
////
//// A pipeline has three types of stages. A `Source` makes events. A `Gate`
//// changes events. A `Sink` uses events. Use `subscribe` to connect the
//// stages. Events move only when the consumer side asks for them. Thus a
//// slow consumer decreases the speed of all stages before it.
////
//// The compiler examines each connection. An `Outlet(event)` can connect
//// only to an `Inlet` that has the same event type.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/int
import gleam/option.{Some}
import sluice/internal/platform
import sluice/internal/protocol.{
  type ConsumerMessage, type ProducerHandle, type SubscribeMetadata,
}

/// What a consumer stage must do when one of its subscriptions stops. A
/// subscription stops when the producer stops, when the producer fails, or
/// when you cancel the subscription.
pub type CancelMode {
  /// The consumer stops also, for each stop reason. This is the default.
  Permanent
  /// The consumer stops only if the producer stopped abnormally.
  Transient
  /// The consumer continues and removes the subscription.
  Temporary
}

/// A change of the subscriber group of a producer stage. The producer
/// hooks set with `on_subscribers` receive it.
pub type SubscriberChange {
  /// A subscriber arrived. `count` is the new quantity of subscribers.
  SubscriberArrived(count: Int)
  /// A subscriber left. `count` is the new quantity of subscribers.
  SubscriberLeft(count: Int)
}

/// The cause of the end of a subscription, from the view of the consumer.
/// The consumer hooks set with `on_cancelled` receive it.
pub type SubscriptionEnd {
  /// The producer stopped with the normal reason or with a shutdown, or
  /// it acknowledged a cancellation.
  ProducerStopped
  /// The producer failed, or it refused the subscription.
  ProducerFailed(reason: String)
}

/// How a subscription requests events.
pub type DemandMode {
  /// The consumer asks by itself: `max_demand` at the start, and again
  /// while it processes events. This is the default.
  Automatic
  /// The consumer asks only when you call `ask`. Use this mode when the
  /// consumer must control the flow, for example because it sends the
  /// events to a different process.
  Manual
}

/// The events that a buffer keeps when the buffer is full.
pub type Keep {
  /// Keep the oldest events. Discard the new events.
  KeepFirst
  /// Keep the newest events. Discard the oldest events. This is the
  /// default.
  KeepLast
}

pub type SubscribeError {
  ProducerNotAlive
  ConsumerNotAlive
  SelfSubscription
  AlreadySubscribed
  /// The demand configuration must obey `0 <= min_demand < max_demand`.
  InvalidDemand(min_demand: Int, max_demand: Int)
  /// This consumer owns its demand loop and does not accept manual demand.
  UnsupportedDemandMode
  SubscribeTimeout
}

/// The output face of a `Source` or a `Gate`. Events of type `event` come
/// out of it. Get one from `source.start` or `gate.outlet`. For stages
/// that have names, get one from `source.outlet_of` or `gate.outlet_of`.
pub opaque type Outlet(event) {
  Outlet(producer: ProducerHandle(event))
}

/// The input face of a `Sink` or a `Gate`. Events of type `event` go into
/// it. Get one from `sink.start` or `gate.inlet`. For stages that have
/// names, get one from the `inlet_of` functions.
pub opaque type Inlet(event) {
  Inlet(
    send: fn(ConsumerControl(event)) -> Nil,
    owner: fn() -> Result(Pid, Nil),
  )
}

/// A live connection between an outlet and an inlet. `subscribe` returns
/// it. The `on_events` callbacks receive it as the source of each batch.
pub opaque type Subscription {
  Subscription(cancel: fn() -> Nil, ask: fn(Int) -> Nil)
}

/// The control messages that a consumer stage accepts.
@internal
pub type ConsumerControl(event) {
  SubscribeTo(
    producer: ProducerHandle(event),
    min_demand: Int,
    max_demand: Int,
    cancel: CancelMode,
    metadata: SubscribeMetadata(event),
    mode: DemandMode,
    reply: Subject(Result(Subscription, SubscribeError)),
  )
  CancelSubscription(subject: Subject(ConsumerMessage(event)))
  RequestDemand(subject: Subject(ConsumerMessage(event)), demand: Int)
  ConfirmSubscribe(
    reply: Subject(Result(Subscription, SubscribeError)),
    confirmed: Subject(Nil),
    deadline: Int,
  )
  AbandonSubscribe(reply: Subject(Result(Subscription, SubscribeError)))
}

/// The options for a connection between an inlet and an outlet. Make the
/// options with `subscription`. Change them with `min_demand`,
/// `max_demand`, and `cancel_mode`. Then give them to `subscribe`.
pub opaque type SubscriptionOptions(event) {
  SubscriptionOptions(
    producer: ProducerHandle(event),
    min_demand: Int,
    max_demand: Int,
    cancel: CancelMode,
    metadata: SubscribeMetadata(event),
    mode: DemandMode,
    timeout: Int,
  )
}

/// Start the description of a subscription to the given outlet. The
/// default options are: `min_demand` 750, `max_demand` 1000, the
/// `Permanent` cancel mode, and a `subscribe_timeout` of 5000
/// milliseconds.
///
/// * `outlet`: The outlet to subscribe to.
pub fn subscription(to outlet: Outlet(event)) -> SubscriptionOptions(event) {
  SubscriptionOptions(
    producer: outlet.producer,
    min_demand: 750,
    max_demand: 1000,
    cancel: Permanent,
    metadata: protocol.default_metadata(),
    mode: Automatic,
    timeout: 5000,
  )
}

/// The consumer asks the producer for more events when the open demand
/// decreases to this value or less. The value must be 0 or more, and less
/// than `max_demand`.
///
/// * `options`: The subscription options to change.
/// * `min_demand`: The demand value at which the consumer asks again.
pub fn min_demand(
  options options: SubscriptionOptions(event),
  min_demand min_demand: Int,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(..options, min_demand:)
}

/// The maximum open demand of this subscription. The open demand is the
/// quantity of events that the consumer asked for and did not process. The
/// value must be 1 or more.
///
/// * `options`: The subscription options to change.
/// * `max_demand`: The maximum open demand of the subscription.
pub fn max_demand(
  options options: SubscriptionOptions(event),
  max_demand max_demand: Int,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(..options, max_demand:)
}

/// Set the cancel mode of the subscription. The default is `Permanent`.
///
/// * `options`: The subscription options to change.
/// * `cancel`: The `CancelMode` of the subscription.
pub fn cancel_mode(
  options options: SubscriptionOptions(event),
  cancel cancel: CancelMode,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(..options, cancel:)
}

/// Select the partition of this subscription. Only a producer with a
/// partition dispatcher reads this value. Such a producer refuses a
/// subscription without a partition.
///
/// * `options`: The subscription options to change.
/// * `index`: The partition of this subscription, from 0 to the partition
///   count of the dispatcher - 1.
pub fn partition(
  options options: SubscriptionOptions(event),
  index index: Int,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(
    ..options,
    metadata: protocol.SubscribeMetadata(
      ..options.metadata,
      partition: Some(index),
    ),
  )
}

/// Give the subscription an event filter. Only a producer with a
/// broadcast dispatcher reads this value: the subscriber receives only
/// the events for which `keep` returns `True`.
///
/// * `options`: The subscription options to change.
/// * `keep`: The filter. It examines one event and returns `True` when
///   the subscriber must receive the event.
pub fn selector(
  options options: SubscriptionOptions(event),
  keep keep: fn(event) -> Bool,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(
    ..options,
    metadata: protocol.SubscribeMetadata(
      ..options.metadata,
      selector: Some(keep),
    ),
  )
}

/// Set how the subscription requests events. The default is `Automatic`.
/// With `Manual`, the consumer receives events only after a call of `ask`.
///
/// * `options`: The subscription options to change.
/// * `mode`: The `DemandMode` of the subscription.
pub fn demand_mode(
  options options: SubscriptionOptions(event),
  mode mode: DemandMode,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(..options, mode:)
}

/// The maximum time that `subscribe` waits for the answer of the consumer
/// stage. The default is 5000 milliseconds.
///
/// When this time expires, `subscribe` returns `Error(SubscribeTimeout)` and
/// withdraws the request. It cannot become a live subscription later: no
/// subscription or initial demand is sent to the producer, and the consumer's
/// `on_subscribed` and `on_cancelled` callbacks are not called for it. It is
/// safe to retry the subscription.
///
/// * `options`: The subscription options to change.
/// * `milliseconds`: The maximum wait time in milliseconds.
pub fn subscribe_timeout(
  options options: SubscriptionOptions(event),
  milliseconds milliseconds: Int,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(..options, timeout: int.max(milliseconds, 0))
}

/// Connect a consumer stage to a producer stage. The consumer immediately
/// asks for `max_demand` events. The consumer asks again while it
/// processes events. Thus events start to move when the producer has
/// events.
///
/// A successful result means that the consumer has sent the subscription to
/// the producer; it does not wait for the producer to accept it. A dispatcher
/// refusal therefore arrives later as an abnormal end of the subscription.
/// If the configured subscription timeout expires first, the request is
/// withdrawn and cannot become live later. See `subscribe_timeout`.
///
/// This compiles only when the two stages have the same event type.
///
/// * `options`: The subscription options, from `subscription`.
/// * `inlet`: The inlet of the consumer stage.
pub fn subscribe(
  options options: SubscriptionOptions(event),
  consumer inlet: Inlet(event),
) -> Result(Subscription, SubscribeError) {
  let valid =
    options.min_demand >= 0
    && options.max_demand >= 1
    && options.min_demand < options.max_demand
  case valid {
    False -> Error(InvalidDemand(options.min_demand, options.max_demand))
    True ->
      case inlet_alive(inlet) {
        False -> Error(ConsumerNotAlive)
        True -> {
          let reply = process.new_subject()
          let deadline = platform.monotonic_milliseconds() + options.timeout
          inlet.send(SubscribeTo(
            producer: options.producer,
            min_demand: options.min_demand,
            max_demand: options.max_demand,
            cancel: options.cancel,
            metadata: options.metadata,
            mode: options.mode,
            reply:,
          ))
          await_subscribe_reply(inlet:, reply:, deadline:)
        }
      }
  }
}

fn await_subscribe_reply(
  inlet inlet: Inlet(event),
  reply reply: Subject(Result(Subscription, SubscribeError)),
  deadline deadline: Int,
) -> Result(Subscription, SubscribeError) {
  case process.receive(reply, remaining_time(deadline)) {
    Ok(Ok(subscription)) -> {
      let confirmed = process.new_subject()
      inlet.send(ConfirmSubscribe(reply, confirmed, deadline))
      case process.receive(confirmed, remaining_time(deadline)) {
        Ok(Nil) -> Ok(subscription)
        Error(Nil) -> {
          inlet.send(AbandonSubscribe(reply))
          Error(SubscribeTimeout)
        }
      }
    }
    Ok(Error(error)) -> Error(error)
    Error(Nil) -> {
      // Withdraw the request. SubscribeTo and AbandonSubscribe come from
      // this process, so the consumer sees them in sequence and removes
      // the provisional state without activating the subscription.
      inlet.send(AbandonSubscribe(reply))
      Error(SubscribeTimeout)
    }
  }
}

fn remaining_time(deadline: Int) -> Int {
  int.max(deadline - platform.monotonic_milliseconds(), 0)
}

fn inlet_alive(inlet: Inlet(event)) -> Bool {
  case inlet.owner() {
    Error(Nil) -> False
    Ok(pid) -> process.is_alive(pid)
  }
}

/// End the accumulation of demand on a source that
/// `source.accumulate_demand` configured. The source then replays the
/// held asks and starts to make events. A source without accumulation
/// ignores this call.
///
/// * `outlet`: The outlet of the source.
pub fn forward_demand(outlet outlet: Outlet(event)) -> Nil {
  outlet.producer.send(protocol.ForwardDemand)
}

/// Cancel a subscription. The consumer disconnects from the producer. Then
/// the consumer applies its cancel mode. Thus, if you cancel a `Permanent`
/// subscription, the consumer stage stops.
///
/// * `subscription`: The subscription to cancel.
pub fn cancel(subscription: Subscription) -> Nil {
  subscription.cancel()
}

/// Ask the producer of a `Manual` subscription for more events. The
/// consumer then receives a maximum of `count` more events. You can call
/// this function from each process, and also from inside an `on_events`
/// callback. A call on an `Automatic` subscription adds to the demand of
/// the automatic loop. This is usually not what you want.
///
/// * `subscription`: The subscription through which to ask.
/// * `count`: The maximum quantity of new events that the consumer
///   receives for this ask.
pub fn ask(subscription subscription: Subscription, count count: Int) -> Nil {
  subscription.ask(count)
}

@internal
pub fn make_outlet(producer: ProducerHandle(event)) -> Outlet(event) {
  Outlet(producer)
}

@internal
pub fn outlet_producer(outlet: Outlet(event)) -> ProducerHandle(event) {
  outlet.producer
}

@internal
pub fn make_inlet(
  send: fn(ConsumerControl(event)) -> Nil,
  owner: fn() -> Result(Pid, Nil),
) -> Inlet(event) {
  Inlet(send:, owner:)
}

@internal
pub fn make_subscription(
  cancel: fn() -> Nil,
  ask: fn(Int) -> Nil,
) -> Subscription {
  Subscription(cancel:, ask:)
}

/// The fields of `SubscriptionOptions`, with labels, for the internal
/// consumers of the opaque type.
@internal
pub type OptionsFields(event) {
  OptionsFields(
    producer: ProducerHandle(event),
    min_demand: Int,
    max_demand: Int,
    cancel: CancelMode,
    metadata: SubscribeMetadata(event),
    mode: DemandMode,
  )
}

@internal
pub fn options_fields(
  options: SubscriptionOptions(event),
) -> OptionsFields(event) {
  OptionsFields(
    producer: options.producer,
    min_demand: options.min_demand,
    max_demand: options.max_demand,
    cancel: options.cancel,
    metadata: options.metadata,
    mode: options.mode,
  )
}
