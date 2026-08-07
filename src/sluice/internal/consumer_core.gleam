//// The shared runtime for the consumer face of a stage (Sink and Gate).
//// It records the subscriptions, operates the automatic demand loop, and
//// applies the cancel modes.

import gleam/bool
import gleam/dict.{type Dict}
import gleam/erlang/process.{
  type Down, type Monitor, type Pid, type Selector, type Subject,
}
import gleam/int
import gleam/list
import gleam/option.{type Option, Some}
import gleam/result
import sluice.{type CancelMode, type SubscribeError, type Subscription}
import sluice/internal/protocol.{
  type CancelReason, type ConsumerMessage, type ProducerHandle,
  type SubscribeMetadata, SubscribeMetadata,
}

pub type SubscriptionState(event) {
  SubscriptionState(
    subject: Subject(ConsumerMessage(event)),
    producer: ProducerHandle(event),
    producer_pid: Pid,
    /// The partition from the subscription metadata. It is part of the
    /// duplicate check, so a subsequent partition dispatcher can accept
    /// one subscription for each partition to the same producer.
    partition: Option(Int),
    monitor: Monitor,
    cancel: CancelMode,
    min_demand: Int,
    max_demand: Int,
    /// The quantity of asked events that the consumer did not process.
    outstanding: Int,
    /// The demand that the consumer must ask again, but keeps until the
    /// subsequent stages ask for events. The gates use this value to
    /// connect their two faces.
    held_ask: Int,
    handle: Subscription,
    cancelling: Bool,
  )
}

pub type Core(event) {
  Core(
    subscriptions: Dict(
      Subject(ConsumerMessage(event)),
      SubscriptionState(event),
    ),
  )
}

pub fn new() -> Core(event) {
  Core(subscriptions: dict.new())
}

/// Make a new subscription: monitor the producer, make the subscription
/// subject, select the subject and the monitor, and send `Subscribe` and
/// the first `Ask`.
pub fn add_subscription(
  core: Core(event),
  selector: Selector(message),
  producer: ProducerHandle(event),
  min_demand min_demand: Int,
  max_demand max_demand: Int,
  cancel cancel: CancelMode,
  metadata metadata: SubscribeMetadata(event),
  tag_message tag_message: fn(
    Subject(ConsumerMessage(event)),
    ConsumerMessage(event),
  ) -> message,
  tag_down tag_down: fn(Subject(ConsumerMessage(event)), Down) -> message,
  make_cancel make_cancel: fn(Subject(ConsumerMessage(event))) -> fn() -> Nil,
) -> Result(#(Core(event), Selector(message), Subscription), SubscribeError) {
  use pid <- result.try(
    resolve_live_owner(producer)
    |> result.replace_error(sluice.ProducerNotAlive),
  )
  use <- bool.guard(
    when: pid == process.self(),
    return: Error(sluice.SelfSubscription),
  )
  use <- bool.guard(
    when: already_subscribed(core:, pid:, partition: metadata.partition),
    return: Error(sluice.AlreadySubscribed),
  )
  let subject = process.new_subject()
  let monitor = process.monitor(pid)
  let selector =
    selector
    |> process.select_map(subject, fn(message) { tag_message(subject, message) })
    |> process.select_specific_monitor(monitor, fn(down) {
      tag_down(subject, down)
    })
  let handle = sluice.make_subscription(make_cancel(subject))
  producer.send(protocol.Subscribe(
    from: subject,
    metadata: SubscribeMetadata(..metadata, max_demand: Some(max_demand)),
  ))
  producer.send(protocol.Ask(from: subject, demand: max_demand))
  let subscription =
    SubscriptionState(
      subject:,
      producer:,
      producer_pid: pid,
      partition: metadata.partition,
      monitor:,
      cancel:,
      min_demand:,
      max_demand:,
      outstanding: max_demand,
      held_ask: 0,
      handle:,
      cancelling: False,
    )
  let core =
    Core(subscriptions: dict.insert(core.subscriptions, subject, subscription))
  Ok(#(core, selector, handle))
}

// The `owner` function alone does not show that the process is alive.
// For a producer with a direct address, it only gets the pid. The pid can
// be dead.
fn resolve_live_owner(producer: ProducerHandle(event)) -> Result(Pid, Nil) {
  case producer.owner() {
    Error(Nil) -> Error(Nil)
    Ok(pid) ->
      case process.is_alive(pid) {
        True -> Ok(pid)
        False -> Error(Nil)
      }
  }
}

// The duplicate key is the producer pid together with the partition. Thus
// a subsequent partition dispatcher can accept one subscription for each
// partition to the same producer.
fn already_subscribed(
  core core: Core(event),
  pid pid: Pid,
  partition partition: Option(Int),
) -> Bool {
  dict.fold(core.subscriptions, False, fn(found, _subject, subscription) {
    found
    || {
      subscription.producer_pid == pid && subscription.partition == partition
    }
  })
}

pub type BatchResult(state) {
  BatchContinue(state: state)
  BatchStop(state: state)
  BatchStopAbnormal(state: state, reason: String)
}

pub type Outcome {
  KeepRunning
  StopNormal
  StopAbnormal(reason: String)
}

/// Process a delivered batch of events. Divide the batch into parts of
/// `max_demand - min_demand` events. Give each part to the handler of the
/// stage. Between the parts, ask the producer again when the open demand
/// decreases to `min_demand` or less. This sequence keeps the pipeline
/// full. The open demand does not become more than `max_demand`.
pub fn deliver(
  core: Core(event),
  subject: Subject(ConsumerMessage(event)),
  events: List(event),
  state: state,
  handle_batch: fn(state, List(event), Subscription) -> BatchResult(state),
  can_ask: fn(state) -> Bool,
) -> #(Core(event), state, Outcome) {
  case dict.get(core.subscriptions, subject) {
    // The subscription is removed. Ignore this delivery.
    Error(Nil) -> #(core, state, KeepRunning)
    Ok(subscription) ->
      case subscription.cancelling {
        // A cancellation is in progress. Discard these events.
        True -> #(core, state, KeepRunning)
        False -> {
          let chunk_size =
            int.max(subscription.max_demand - subscription.min_demand, 1)
          let chunks = list.sized_chunk(events, chunk_size)
          deliver_loop(core, subscription, chunks, state, handle_batch, can_ask)
        }
      }
  }
}

fn deliver_loop(
  core: Core(event),
  subscription: SubscriptionState(event),
  chunks: List(List(event)),
  state: state,
  handle_batch: fn(state, List(event), Subscription) -> BatchResult(state),
  can_ask: fn(state) -> Bool,
) -> #(Core(event), state, Outcome) {
  case chunks {
    [] -> #(store(core, subscription), state, KeepRunning)
    [chunk, ..remaining] -> {
      let subscription = consume(subscription, list.length(chunk))
      case handle_batch(state, chunk, subscription.handle) {
        BatchContinue(state) -> {
          let subscription = maybe_ask(subscription, can_ask(state))
          deliver_loop(
            core,
            subscription,
            remaining,
            state,
            handle_batch,
            can_ask,
          )
        }
        BatchStop(state) -> #(store(core, subscription), state, StopNormal)
        BatchStopAbnormal(state, reason) -> #(
          store(core, subscription),
          state,
          StopAbnormal(reason),
        )
      }
    }
  }
}

fn store(
  core: Core(event),
  subscription: SubscriptionState(event),
) -> Core(event) {
  Core(subscriptions: dict.insert(
    core.subscriptions,
    subscription.subject,
    subscription,
  ))
}

fn consume(
  subscription: SubscriptionState(event),
  count: Int,
) -> SubscriptionState(event) {
  SubscriptionState(
    ..subscription,
    outstanding: int.max(subscription.outstanding - count, 0),
  )
}

fn maybe_ask(
  subscription: SubscriptionState(event),
  can_ask: Bool,
) -> SubscriptionState(event) {
  use <- bool.guard(
    when: subscription.outstanding > subscription.min_demand,
    return: subscription,
  )
  let amount = subscription.max_demand - subscription.outstanding
  case can_ask {
    True -> {
      subscription.producer.send(protocol.Ask(
        from: subscription.subject,
        demand: amount,
      ))
      SubscriptionState(..subscription, outstanding: subscription.max_demand)
    }
    // The subsequent stages do not ask for events. Keep the ask. The
    // flush sends it when demand comes again. Increase `outstanding` as if
    // the ask occurred. Thus the trigger does not fire again.
    False ->
      SubscriptionState(
        ..subscription,
        outstanding: subscription.max_demand,
        held_ask: subscription.held_ask + amount,
      )
  }
}

/// Send the demand that the consumer kept while the subsequent stages
/// did not ask for events.
pub fn flush_held_asks(core: Core(event)) -> Core(event) {
  Core(
    subscriptions: dict.map_values(
      core.subscriptions,
      fn(_subject, subscription) {
        case subscription.held_ask > 0 && !subscription.cancelling {
          False -> subscription
          True -> {
            subscription.producer.send(protocol.Ask(
              from: subscription.subject,
              demand: subscription.held_ask,
            ))
            SubscriptionState(..subscription, held_ask: 0)
          }
        }
      },
    ),
  )
}

/// A local cancellation: tell the producer. Then wait for its
/// acknowledgment or its exit. Then clean the subscription. Thus the
/// consumer first sees the events that are in the mailbox.
pub fn request_cancel(
  core core: Core(event),
  subject subject: Subject(ConsumerMessage(event)),
) -> Core(event) {
  case dict.get(core.subscriptions, subject) {
    Error(Nil) -> core
    Ok(subscription) ->
      case subscription.cancelling {
        True -> core
        False -> {
          subscription.producer.send(protocol.Cancel(from: subject))
          store(core, SubscriptionState(..subscription, cancelling: True))
        }
      }
  }
}

/// The subscription is complete: the producer acknowledged a
/// cancellation, cancelled the consumer, or stopped. Remove the
/// subscription. Report what the stage must do. The cancel mode of the
/// subscription gives the answer.
pub fn closed(
  core core: Core(event),
  selector selector: Selector(message),
  subject subject: Subject(ConsumerMessage(event)),
  reason reason: CancelReason,
) -> #(Core(event), Selector(message), Outcome) {
  case dict.get(core.subscriptions, subject) {
    Error(Nil) -> #(core, selector, KeepRunning)
    Ok(subscription) -> {
      process.demonitor_process(subscription.monitor)
      let selector =
        selector
        |> process.deselect(subject)
        |> process.deselect_specific_monitor(subscription.monitor)
      let core = Core(subscriptions: dict.delete(core.subscriptions, subject))
      #(core, selector, cancel_outcome(subscription.cancel, reason))
    }
  }
}

fn cancel_outcome(mode: CancelMode, reason: CancelReason) -> Outcome {
  case mode, reason {
    sluice.Temporary, _ -> KeepRunning
    sluice.Transient, protocol.Abnormal(reason) -> StopAbnormal(reason)
    sluice.Transient, _ -> KeepRunning
    sluice.Permanent, protocol.Abnormal(reason) -> StopAbnormal(reason)
    sluice.Permanent, _ -> StopNormal
  }
}
