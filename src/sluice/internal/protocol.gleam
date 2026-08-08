//// The typed message protocol between the stages.
////
//// Demand moves to the producers as `Ask` messages. Events move to the
//// consumers as `NewEvents` batches, on a subject that belongs to one
//// subscription. The consumer stage makes this
//// `Subject(ConsumerMessage(event))`. On the producer side, the subject
//// also identifies the subscription.

import gleam/erlang/process.{type Pid, type Subject}
import gleam/option.{type Option, None}

/// The messages that a producer face (an outlet) receives from its
/// subscribers.
pub type ProducerMessage(event) {
  Subscribe(
    from: Subject(ConsumerMessage(event)),
    metadata: SubscribeMetadata(event),
  )
  Ask(from: Subject(ConsumerMessage(event)), demand: Int)
  Cancel(from: Subject(ConsumerMessage(event)))
  /// End the accumulation of demand on a source.
  ForwardDemand
}

/// The messages that a consumer face receives from the producer, on the
/// subscription subject. `Cancelled` is always the last message on a
/// subscription. It tells the consumer that the producer stopped the
/// subscription, or it acknowledges a cancellation from the consumer. The
/// sequence of the messages from one sender does not change. Thus the
/// consumer sees each `NewEvents` message before it sees `Cancelled`.
pub type ConsumerMessage(event) {
  NewEvents(events: List(event))
  Cancelled(reason: CancelReason)
}

/// The cause of the stop of a subscription, or of its process.
pub type CancelReason {
  Normal
  Shutdown
  Abnormal(String)
}

/// More subscription options. The dispatcher of the producer stage reads
/// them. The protocol contains the `selector` and `partition` fields now,
/// before a dispatcher uses them. Thus subsequent dispatchers can use them
/// without a protocol change. The consumer runtime fills the `max_demand`
/// field, and the demand dispatcher compares the values from different
/// subscribers.
pub type SubscribeMetadata(event) {
  SubscribeMetadata(
    selector: Option(fn(event) -> Bool),
    partition: Option(Int),
    max_demand: Option(Int),
  )
}

pub fn default_metadata() -> SubscribeMetadata(event) {
  SubscribeMetadata(selector: None, partition: None, max_demand: None)
}

/// A reference to a producer face that you can send to other code. It has
/// a function that sends protocol messages, and a function that finds the
/// owner process. These are closures, not a bare subject. Thus one type is
/// applicable to stages with direct addresses and to stages with names.
pub type ProducerHandle(event) {
  ProducerHandle(
    send: fn(ProducerMessage(event)) -> Nil,
    owner: fn() -> Result(Pid, Nil),
  )
}
