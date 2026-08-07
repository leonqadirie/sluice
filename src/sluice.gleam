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
  Subscription(cancel: fn() -> Nil)
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
    reply: Subject(Result(Subscription, SubscribeError)),
  )
  CancelSubscription(subject: Subject(ConsumerMessage(event)))
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
    timeout: Int,
  )
}

/// Start the description of a subscription to the given outlet. The
/// default options are: `min_demand` 750, `max_demand` 1000, the
/// `Permanent` cancel mode, and a `subscribe_timeout` of 5000
/// milliseconds.
pub fn subscription(to outlet: Outlet(event)) -> SubscriptionOptions(event) {
  SubscriptionOptions(
    producer: outlet.producer,
    min_demand: 750,
    max_demand: 1000,
    cancel: Permanent,
    metadata: protocol.default_metadata(),
    timeout: 5000,
  )
}

/// The consumer asks the producer for more events when the open demand
/// decreases to this value or less. The value must be 0 or more, and less
/// than `max_demand`.
pub fn min_demand(
  options options: SubscriptionOptions(event),
  min_demand min_demand: Int,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(..options, min_demand:)
}

/// The maximum open demand of this subscription. The open demand is the
/// quantity of events that the consumer asked for and did not process. The
/// value must be 1 or more.
pub fn max_demand(
  options options: SubscriptionOptions(event),
  max_demand max_demand: Int,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(..options, max_demand:)
}

pub fn cancel_mode(
  options options: SubscriptionOptions(event),
  cancel cancel: CancelMode,
) -> SubscriptionOptions(event) {
  SubscriptionOptions(..options, cancel:)
}

/// The maximum time that `subscribe` waits for the answer of the consumer
/// stage. The default is 5000 milliseconds.
///
/// When this time expires, `subscribe` returns `Error(SubscribeTimeout)` and
/// withdraws the request. It cannot become a live subscription later: no
/// subscription or initial demand is sent to the producer, and the consumer's
/// `on_subscribed` and `on_cancelled` callbacks are not called for it. It is
/// safe to retry the subscription.
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
          inlet.send(SubscribeTo(
            producer: options.producer,
            min_demand: options.min_demand,
            max_demand: options.max_demand,
            cancel: options.cancel,
            metadata: options.metadata,
            reply:,
          ))
          case process.receive(reply, options.timeout) {
            Ok(result) -> result
            Error(Nil) -> Error(SubscribeTimeout)
          }
        }
      }
  }
}

fn inlet_alive(inlet: Inlet(event)) -> Bool {
  case inlet.owner() {
    Error(Nil) -> False
    Ok(pid) -> process.is_alive(pid)
  }
}

/// Cancel a subscription. The consumer disconnects from the producer. Then
/// the consumer applies its cancel mode. Thus, if you cancel a `Permanent`
/// subscription, the consumer stage stops.
pub fn cancel(subscription: Subscription) -> Nil {
  subscription.cancel()
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
pub fn make_subscription(cancel: fn() -> Nil) -> Subscription {
  Subscription(cancel:)
}

@internal
pub fn options_fields(
  options: SubscriptionOptions(event),
) -> #(ProducerHandle(event), Int, Int, CancelMode, SubscribeMetadata(event)) {
  #(
    options.producer,
    options.min_demand,
    options.max_demand,
    options.cancel,
    options.metadata,
  )
}
