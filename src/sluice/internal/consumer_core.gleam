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
import gleam/option.{type Option, None, Some}
import gleam/result
import sluice.{
  type CancelMode, type DemandMode, type SubscribeError, type Subscription,
}
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
    metadata: SubscribeMetadata(event),
    monitor: Monitor,
    cancel: CancelMode,
    mode: DemandMode,
    min_demand: Int,
    max_demand: Int,
    /// The quantity of asked events that the consumer did not process.
    outstanding: Int,
    /// The demand that the consumer must ask again, but keeps until the
    /// subsequent stages ask for events. The gates use this value to
    /// connect their two faces.
    held_ask: Int,
    handle: Subscription,
    /// The reply subject of the subscribe request, for runtime
    /// subscriptions. An abandonment after a timeout finds the
    /// subscription through it.
    reply: Option(Subject(Result(Subscription, SubscribeError))),
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

/// Prepare a new subscription: monitor the producer, make the subscription
/// subject, and select the subject and monitor. Declared subscriptions send
/// `Subscribe` and the first `Ask` immediately. Runtime subscriptions wait
/// for the caller's confirmation.
pub fn add_subscription(
  core: Core(event),
  selector: Selector(message),
  producer: ProducerHandle(event),
  min_demand min_demand: Int,
  max_demand max_demand: Int,
  cancel cancel: CancelMode,
  mode mode: DemandMode,
  metadata metadata: SubscribeMetadata(event),
  reply reply: Option(Subject(Result(Subscription, SubscribeError))),
  tag_message tag_message: fn(
    Subject(ConsumerMessage(event)),
    ConsumerMessage(event),
  ) -> message,
  tag_down tag_down: fn(Subject(ConsumerMessage(event)), Down) -> message,
  make_cancel make_cancel: fn(Subject(ConsumerMessage(event))) -> fn() -> Nil,
  make_ask make_ask: fn(Subject(ConsumerMessage(event))) -> fn(Int) -> Nil,
) -> Result(
  #(
    Core(event),
    Selector(message),
    Subject(ConsumerMessage(event)),
    Subscription,
  ),
  SubscribeError,
) {
  // The runtime subscribe path checks the demand values before it sends
  // the request. This check also covers the declared subscriptions,
  // which do not go through that path.
  use <- bool.guard(
    when: min_demand < 0 || max_demand < 1 || min_demand >= max_demand,
    return: Error(sluice.InvalidDemand(min_demand, max_demand)),
  )
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
  let handle = sluice.make_subscription(make_cancel(subject), make_ask(subject))
  // Runtime subscriptions remain provisional until the caller confirms
  // the successful reply. Declared subscriptions activate immediately.
  let outstanding = case mode {
    sluice.Automatic -> max_demand
    sluice.Manual -> 0
  }
  let subscription =
    SubscriptionState(
      subject:,
      producer:,
      producer_pid: pid,
      partition: metadata.partition,
      metadata:,
      monitor:,
      cancel:,
      mode:,
      min_demand:,
      max_demand:,
      outstanding:,
      held_ask: 0,
      handle:,
      reply:,
      cancelling: False,
    )
  let subscription = case reply {
    None -> activate(subscription, metadata)
    Some(_) -> subscription
  }
  let core =
    Core(subscriptions: dict.insert(core.subscriptions, subject, subscription))
  Ok(#(core, selector, subject, handle))
}

fn activate(
  subscription: SubscriptionState(event),
  metadata: SubscribeMetadata(event),
) -> SubscriptionState(event) {
  subscription.producer.send(protocol.Subscribe(
    from: subscription.subject,
    metadata: SubscribeMetadata(
      ..metadata,
      max_demand: Some(subscription.max_demand),
    ),
  ))
  case subscription.mode {
    sluice.Automatic ->
      subscription.producer.send(protocol.Ask(
        from: subscription.subject,
        demand: subscription.max_demand,
      ))
    sluice.Manual -> Nil
  }
  subscription
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
    Ok(subscription) -> {
      // A cancellation in progress does not discard deliveries: the
      // producer sent these events before it saw the cancellation, and
      // the consumer processes all events from before the
      // acknowledgement. Only the asks stop.
      let chunks = divide(subscription, events)
      deliver_loop(core, subscription, chunks, state, handle_batch, can_ask)
    }
  }
}

// In the manual mode the batch goes to the handler in one part: the
// division only serves the automatic ask sequence.
fn divide(
  subscription: SubscriptionState(event),
  events: List(event),
) -> List(List(event)) {
  case subscription.mode {
    sluice.Manual -> [events]
    sluice.Automatic -> {
      let chunk_size =
        int.max(subscription.max_demand - subscription.min_demand, 1)
      list.sized_chunk(events, chunk_size)
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
  // A subscription with a cancellation in progress asks for nothing.
  use <- bool.guard(when: subscription.cancelling, return: subscription)
  // A manual subscription never asks by itself.
  use <- bool.guard(
    when: subscription.mode == sluice.Manual,
    return: subscription,
  )
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

/// A request for more events on a manual subscription: forward the ask to
/// the producer and record the open demand.
pub fn request_demand(
  core core: Core(event),
  subject subject: Subject(ConsumerMessage(event)),
  demand demand: Int,
) -> Core(event) {
  use <- bool.guard(when: demand <= 0, return: core)
  case dict.get(core.subscriptions, subject) {
    Error(Nil) -> core
    Ok(subscription) -> {
      use <- bool.guard(when: subscription.cancelling, return: core)
      subscription.producer.send(protocol.Ask(from: subject, demand:))
      store(
        core,
        SubscriptionState(
          ..subscription,
          outstanding: subscription.outstanding + demand,
        ),
      )
    }
  }
}

/// Confirm a runtime subscription after its caller receives the successful
/// reply. Activation sends the protocol messages only now, so the
/// `on_subscribed` hook and event flow cannot precede the caller's success.
pub fn confirm(
  core core: Core(event),
  reply reply: Subject(Result(Subscription, SubscribeError)),
) -> #(Core(event), Option(#(Subject(ConsumerMessage(event)), Subscription))) {
  case find_by_reply(core, reply) {
    Error(Nil) -> #(core, None)
    Ok(subscription) -> {
      let subscription =
        activate(
          SubscriptionState(..subscription, reply: None),
          subscription.metadata,
        )
      #(
        store(core, subscription),
        Some(#(subscription.subject, subscription.handle)),
      )
    }
  }
}

/// Remove a provisional subscription after its caller times out. No
/// producer messages or application hooks have run for it.
pub fn abandon(
  core core: Core(event),
  selector selector: Selector(message),
  reply reply: Subject(Result(Subscription, SubscribeError)),
) -> #(Core(event), Selector(message)) {
  case find_by_reply(core, reply) {
    Error(Nil) -> #(core, selector)
    Ok(subscription) -> {
      process.demonitor_process(subscription.monitor)
      let selector =
        selector
        |> process.deselect(subscription.subject)
        |> process.deselect_specific_monitor(subscription.monitor)
      let core =
        Core(subscriptions: dict.delete(
          core.subscriptions,
          subscription.subject,
        ))
      #(core, selector)
    }
  }
}

fn find_by_reply(
  core: Core(event),
  reply: Subject(Result(Subscription, SubscribeError)),
) -> Result(SubscriptionState(event), Nil) {
  dict.fold(core.subscriptions, Error(Nil), fn(found, _subject, subscription) {
    case subscription.reply == Some(reply) {
      True -> Ok(subscription)
      False -> found
    }
  })
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
