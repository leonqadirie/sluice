# sluice

[![Package Version](https://img.shields.io/hexpm/v/sluice)](https://hex.pm/packages/sluice)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/sluice/)

Stage pipelines for Gleam, with demand control and backpressure.

A pipeline is a chain of stages that events flow through. There are three
kinds of stages:

- A **Source** produces events.
- A **Gate** receives events, transforms them, and passes them on. A gate
  can emit zero, one, or many events for each event it receives, so gates
  can also filter, expand, and aggregate.
- A **Sink** consumes events.

Events flow forward through the pipeline. Demand flows backward:

```
events:   Source ──▶ Gate ──▶ Sink
demand:   Source ◀── Gate ◀── Sink
```

Nothing moves until the consuming side asks for events. Because of this, a
slow sink slows down every stage before it, mailboxes stay bounded, and
you never need to poll. No event is dropped silently: a stage that must
drop events logs a warning first.

## When to use sluice

Use sluice when events keep arriving and handling them is slower than
their arrival — packets from a socket, jobs from a queue, readings from
sensors. Without demand control, a fast producer fills a slow consumer's
mailbox until memory runs out. With sluice, the consumer asks for exactly
as much as it can handle, and that limit travels backward through every
stage to the source.

You don't need sluice for a one-off parallel computation over data you
already have in memory; spawning ordinary processes is simpler. Reach for
sluice when the stages are long-lived processes and the data never stops.

## Outlets and inlets

Every connection runs from an **outlet** to an **inlet**. An outlet is
the face of a stage that provides events. An inlet is the face that
receives them. A source has only an outlet and a sink has only an inlet,
so you pass their handles (`counter.data`, `printer.data` below) straight
to `subscribe`. A gate has both faces, so you pick one with
`gate.outlet(...)` or `gate.inlet(...)`.

The compiler checks every connection: an `Outlet(event)` only connects to
an `Inlet` of the same event type. If you connect a source of `Int` to a
sink of `String`, you get a compile error.

## Quick start

<!-- x-release-please-start-major -->
```sh
gleam add sluice@0
```
<!-- x-release-please-end -->

```gleam
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import sluice
import sluice/gate
import sluice/sink
import sluice/source

pub fn main() -> Nil {
  // This source counts up. It only produces the events that consumers
  // ask for.
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      let events =
        int.range(from: counter, to: counter + demand, with: [], run: list.prepend)
        |> list.reverse
      source.emit(events, counter + demand)
    })
    |> source.start()

  // This gate doubles each event.
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(list.map(events, fn(event) { event * 2 }), state)
    })
    |> gate.start()

  // This sink prints slowly, so it sets the pace for the whole
  // pipeline.
  let assert Ok(printer) =
    sink.new(init: Nil, on_events: fn(state, events, _subscription) {
      list.each(events, fn(event) { io.println(int.to_string(event)) })
      process.sleep(500)
      sink.continue(state)
    })
    |> sink.start()

  // Connect the stages: counter -> doubler -> printer.
  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.subscribe(consumer: gate.inlet(doubler.data))
  let assert Ok(_) =
    sluice.subscription(to: gate.outlet(doubler.data))
    |> sluice.min_demand(5)
    |> sluice.max_demand(10)
    |> sluice.subscribe(consumer: printer.data)

  process.sleep_forever()
}
```

## Examples

The [`example`](example) directory contains one runnable module for each
pattern: the basic pipeline, pushed events and buffer overflow, a
broadcast with a filter, partitions, a worker pool, supervised event
workers, and a pipeline under a supervisor. Run one from that directory:

```sh
cd example
gleam run --module counter_pipeline
```

## Demand

When an inlet subscribes to an outlet, the consumer immediately asks for
`max_demand` events. Delivered events reach the `on_events` callback in
batches of `max_demand - min_demand`. After each batch, the consumer
checks its open demand — the number of events it has asked for but not
yet processed. When open demand falls to `min_demand` or below, the
consumer asks for enough events to bring it back up to `max_demand`.

Here is the cycle with the defaults, `max_demand: 1000` and
`min_demand: 750`:

1. The consumer subscribes and asks for 1,000 events.
2. The producer sends 1,000 events. The consumer processes them in
   batches of 250 (`max_demand - min_demand`).
3. After the first batch, open demand is down to 750, so the consumer
   asks for 250 more.
4. The producer creates new events while the consumer works through the
   remaining batches. The cycle repeats after every batch.

Producer and consumer work at the same time, and open demand never rises
above `max_demand`. To tune a subscription: use a small `max_demand`
(down to 1) when each event is expensive to process, and a large one when
events are cheap and you want fewer messages between stages.

A source may produce more events than were asked for. That's fine: the
source keeps the extra events in its buffer and serves later demand from
the buffer before it produces anything new. The default buffer holds
10,000 events. When the buffer is full, the overflow policy decides what
to drop: `KeepLast` (the default) drops the oldest events, `KeepFirst`
drops the new ones. A stage that drops events logs a warning; set a
different response with `on_discard`. Buffering also works in the other
direction: a producer that receives demand before it has events remembers
that demand and fills it as soon as events appear.

A gate connects its two faces. It asks the stages before it for more
events only when the stages after it ask, or when its own buffer is
empty. That is how backpressure travels through the whole pipeline. The
gate buffer has no size limit, but the demand cycle keeps it small: it
never holds much more than the sum of the `max_demand` values of the
subscriptions feeding the gate.

## Defaults

| Option              | Where              | Default     |
| ------------------- | ------------------ | ----------- |
| `max_demand`        | per subscription   | 1000        |
| `min_demand`        | per subscription   | 750         |
| `cancel_mode`       | per subscription   | `Permanent` |
| `subscribe_timeout` | per subscription   | 5000 ms     |
| `buffer_capacity`   | source builder     | 10,000      |
| `buffer_capacity`   | gate builder       | no limit    |
| `buffer_keep`       | source and gate    | `KeepLast`  |
| `start_timeout`     | stage builders     | 5000 ms     |
| `on_discard`        | source and gate    | log warning |
| child restart       | consumer supervisor | `Temporary` |
| restart tolerance   | consumer supervisor | 3 in 5 s    |
| `shutdown_timeout`  | consumer supervisor | 5000 ms     |

### Subscription timeouts

`subscribe` waits for the consumer stage for up to `subscribe_timeout`.
If time runs out, it returns `Error(SubscribeTimeout)` and withdraws the
request. The request can't become a live subscription later: sluice does
not send its subscription or initial demand to the producer, and does not
call the consumer's `on_subscribed` or `on_cancelled` callbacks for it.
It's safe to retry the subscription.

A successful `subscribe` means the consumer sent the subscription to the
producer. The producer's acceptance is still asynchronous — for example,
a dispatcher's refusal arrives later as an abnormal end of the
subscription.

## Push events from outside

Some sources can't produce events on request. Their events come from a
socket, a timer, or another process. `source.new_with_emitter` passes an
`Emitter` to the initialiser, and any process can push events through it:

```gleam
let assert Ok(measurements) =
  source.new_with_emitter(init: fn(emitter) {
    my_socket.on_packet(fn(packet) { source.push(emitter, [packet]) })
    Ok(Nil)
  })
  |> source.start()
```

The source sends pushed events to its consumers right away, up to the
open demand. The buffer holds the rest.

## Dispatchers

A dispatcher decides which subscriber of a source or gate receives which
events. Set it with the `dispatcher` builder function. There are three:

- `dispatcher.demand()` (the default): each event goes to exactly one
  subscriber — the one with the most open demand. Over time this spreads
  the load evenly across subscribers.
- `dispatcher.broadcast()`: each event goes to every subscriber, at the
  pace of the slowest one. A subscriber can set a filter with
  `sluice.selector(keep: fn(event) { ... })`; it then receives only the
  events the filter keeps.
- `dispatcher.partition(count:, by:)`: the `by` function assigns each
  event to a partition, and each partition has at most one subscriber. A
  subscriber picks its partition with `sluice.partition(index:)`. Events
  for a partition without open demand wait in that partition's queue, so
  a slow partition doesn't block the others.

```gleam
import sluice/dispatcher

let assert Ok(measurements) =
  sensor_source()
  |> source.dispatcher(dispatcher.partition(count: 4, by: fn(measurement) {
    measurement.sensor_id
  }))
  |> source.start()
```

## Subscription hooks

A producer stage can watch its subscriber group: `on_subscribers` runs
whenever a subscriber arrives or leaves, with the new subscriber count.
Use it to start work when the first subscriber appears and to stop work
when the last one leaves. A sink has two hooks: `on_subscribed` receives
each new `Subscription`, and `on_cancelled` runs when a subscription
ends. When `on_cancelled` is set, it decides what the sink does, and the
cancel mode no longer applies.

## Accumulate demand at the start

A source with `accumulate_demand` holds incoming asks and produces
nothing. Calling `sluice.forward_demand(outlet:)` releases the held asks.
Use this to wire up a whole pipeline before any events start to move.

## Manual demand

Some consumers must control the flow themselves — for example, because
they forward events to another process. Set the subscription's demand
mode to `Manual`. The consumer then receives events only after you call
`sluice.ask`:

```gleam
let assert Ok(subscription) =
  sluice.subscription(to: counter.data)
  |> sluice.demand_mode(sluice.Manual)
  |> sluice.subscribe(consumer: printer.data)

// The consumer receives at most 10 events.
sluice.ask(subscription:, count: 10)
```

The `on_events` callback also receives the subscription, so a sink can
ask for its next batch from inside the callback and set its own pace.

## Send messages to a stage

A stage can receive messages of any type you choose, alongside the event
flow. Use this for timers, configuration changes, and queries. Give the
stage a message channel with `on_message`. At startup, the `initialise`
callback receives the channel's subject. The handler receives each
message together with the state, and it can do everything the stage can
do — a source or a gate can emit events from it:

```gleam
let assert Ok(prices) =
  source.new(init: Nil, on_demand: fn(state, _demand) {
    source.emit([], state)
  })
  |> source.on_message(
    initialise: fn(subject) {
      // A tick every second.
      let _timer = process.send_after(subject, 1000, Tick)
      Nil
    },
    handler: fn(state, message) {
      case message {
        Tick -> source.emit([read_price()], state)
        Refresh(new_source) -> source.emit([], new_source)
      }
    },
  )
  |> source.start()
```

## When stages stop

Every subscription has a cancel mode, which tells the consumer stage what
to do when the subscription ends. A subscription ends when the producer
stops, when the producer crashes, or when you cancel the subscription:

- `Permanent` (the default): the consumer stops too, whatever the
  reason.
- `Transient`: the consumer stops only if the producer crashed. If a
  source stops normally with `source.stop()`, the consumer keeps going.
- `Temporary`: the consumer keeps going and forgets the subscription.

You can also cancel a subscription with `sluice.cancel`. Every
`on_events` callback receives its `Subscription`, so a stage can
disconnect itself while events are flowing.

## Process events in parallel

`pool.sink` builds a sink that runs each event in its own worker process,
with a cap on how many workers run at once. Demand follows the workers:
the pool asks for one more event whenever a worker finishes. Subscribe it
with the `Manual` demand mode; the pool does its own asking. A crashing
worker doesn't take the pool down:

```gleam
import sluice/pool

let assert Ok(deliveries) =
  pool.sink(concurrency: 8, run: fn(order) { deliver(order) })
  |> sink.start()
let assert Ok(_) =
  sluice.subscription(to: orders.data)
  |> sluice.demand_mode(sluice.Manual)
  |> sluice.subscribe(consumer: deliveries.data)
```

For supervised event workers, use `consumer_supervisor`. It starts one
linked OTP child for each event. `max_demand` sets the number of credits
for each subscription: every running child holds one credit, and the
credit returns only when the child finally terminates. An abnormal exit
followed by a transient restart keeps the same event and credit:

```gleam
import gleam/erlang/process
import gleam/otp/actor
import sluice/consumer_supervisor

fn start_delivery(order: Order) -> actor.StartResult(Nil) {
  let pid = process.spawn(fn() { deliver(order) })
  Ok(actor.Started(pid:, data: Nil))
}

let assert Ok(deliveries) =
  consumer_supervisor.new(start_delivery)
  |> consumer_supervisor.restart(consumer_supervisor.Transient)
  |> consumer_supervisor.restart_tolerance(
    max_restarts: 3,
    within_seconds: 5,
  )
  |> consumer_supervisor.start()

let assert Ok(_) =
  sluice.subscription(to: orders.data)
  |> sluice.min_demand(4)
  |> sluice.max_demand(8)
  |> sluice.subscribe(consumer: consumer_supervisor.inlet(deliveries.data))
```

The child start function must return a process linked to its caller, as
normal OTP child start functions do. Initialisation is synchronous and
must finish quickly; the child does the long-running work after it
starts. Children are `Temporary` by default. `Transient` children restart
only after abnormal exits. `Permanent` children are not supported,
because an event that completed successfully must not restart.

The consumer supervisor owns its demand loop, so it accepts the default
`Automatic` mode and rejects `Manual` subscriptions. If a producer
connection ends, the existing children keep running. The subscription's
cancel mode then decides whether the consumer supervisor stays alive;
when it stops, it shuts down all remaining children and kills any that
exceed `shutdown_timeout`.

`pool.sink` remains useful for lightweight, best-effort work: its workers
are unlinked, failures are logged and dropped, and each completion
immediately asks for a replacement. `consumer_supervisor` gives you OTP
restart policy, restart tolerance, bounded shutdown, and child inspection
instead.

## Yielders and folds

`source.from_yielder` builds a source from a yielder; the source stops
normally when the yielder ends. A source you write yourself must end with
`source.emit_final(events)`: the last events go out, and then the source
stops. Don't wait for a later demand to return `source.stop()` — after a
batch that doesn't fill the demand, the consumers won't ask again, and
the pipeline waits forever. In the other direction, `sink.fold` runs the
full flow of an outlet through a fold and returns the final value:

```gleam
import gleam/yielder

let assert Ok(numbers) =
  source.from_yielder(yielder.range(1, 100)) |> source.start()
let assert Ok(total) =
  sink.fold(from: numbers.data, initial: 0, with: int.add, within: 5000)
```

## Supervision

Stages start as standard OTP actors, so a `static_supervisor` can hold
them as children. For connections that survive restarts, give the stages
names and declare the subscriptions on the builder. The stage then makes
its declared subscriptions during startup. Startup fails for the checks
the stage can make itself — a dead producer, a duplicate subscription,
invalid demand values — and the supervisor retries. A refusal from the
producer's dispatcher arrives later, as an abnormal end of the
subscription:

```gleam
import gleam/otp/static_supervisor as supervisor
import gleam/otp/supervision

pub fn start_pipeline() {
  let counter_name = source.new_name("counter")

  supervisor.new(supervisor.RestForOne)
  |> supervisor.add(
    supervision.worker(fn() {
      counter_source() |> source.named(counter_name) |> source.start()
    }),
  )
  |> supervisor.add(
    supervision.worker(fn() {
      printer_sink()
      |> sink.subscribe(sluice.subscription(to: source.outlet_of(counter_name)))
      |> sink.start()
    }),
  )
  |> supervisor.start()
}
```

Use `Permanent` subscriptions with `RestForOne`. When a source stops, its
consumers stop with it, the supervisor restarts the later stages in
order, and each stage reconnects through the names.

## Design notes

- A consumer can hold at most one subscription per producer. A second
  `subscribe` call to the same producer returns
  `Error(AlreadySubscribed)`.
- When a stage stops, the events on their way to it are lost with its
  mailbox. The producer acknowledges a cancellation in order, so a live
  consumer processes every event the producer sent before the
  cancellation.
- One sink can consume from two sources with different event types. To
  do this, define a sum type for the sink, then put a small gate after
  each source that wraps the source's type in the sum type.

## Inspiration

Heavily inspired by Elixir's [GenStage](https://gen-stage.hexdocs.pm/GenStage.html).
