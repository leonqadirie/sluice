# sluice

[![Package Version](https://img.shields.io/hexpm/v/sluice)](https://hex.pm/packages/sluice)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/sluice/)

Stage pipelines for Gleam, with demand control and backpressure.

A pipeline has three types of stages:

- A **Source** makes events.
- A **Gate** changes events.
- A **Sink** uses events.

Events move only when the consumer side asks for them. Thus a slow sink
decreases the speed of all stages before it. Mailboxes do not grow without a
limit. Events are not lost. You do not poll.

The compiler examines each connection. An `Outlet(event)` can connect only
to an `Inlet` that has the same event type. If you connect a source of `Int`
to a sink of `String`, you get a compile error.

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
  // This source counts up. It makes only the events that the demand asks
  // for.
  let assert Ok(counter) =
    source.new(init: 0, on_demand: fn(counter, demand) {
      let events =
        int.range(from: counter, to: counter + demand, with: [], run: list.prepend)
        |> list.reverse
      source.emit(events, counter + demand)
    })
    |> source.start()

  // This gate multiplies each event by two.
  let assert Ok(doubler) =
    gate.new(init: Nil, on_events: fn(state, events, _subscription) {
      gate.emit(list.map(events, fn(event) { event * 2 }), state)
    })
    |> gate.start()

  // This sink prints slowly. Thus it decreases the speed of the full
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

## Demand

When an inlet subscribes to an outlet, the consumer immediately asks for
`max_demand` events. The consumer then processes events in batches. When the
open demand decreases to `min_demand`, the consumer asks again. Thus the
producer makes events while the consumer processes events. The open demand
of a subscription is never more than `max_demand`.

A source can make more events than the demand. This is not an error. The
source keeps the extra events in its buffer. The buffer supplies the
subsequent demand before the source makes new events. The default buffer
limit is 10,000 events. When the buffer is full, the overflow policy
applies. `KeepLast` (the default) discards the oldest events. `KeepFirst`
discards the new events. When a stage discards events, it writes a warning
to the log. Set a different response with `on_discard`.

A gate connects its two faces. The gate asks the stages before it for more
events only in one of two conditions: the stages after it ask for events,
or the gate buffer is empty. Thus backpressure moves through the full
pipeline. The gate buffer has no default limit. The demand connection keeps
the buffer small. The maximum buffer content is approximately the sum of
the `max_demand` values of the subscriptions before the gate.

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

`subscribe` waits for the consumer stage for at most `subscribe_timeout`.
When that time expires, it returns `Error(SubscribeTimeout)` and withdraws
the request. The request cannot become a live subscription later: Sluice
does not send its subscription or initial demand to the producer, and does
not call the consumer's `on_subscribed` or `on_cancelled` callbacks for it.
It is therefore safe to retry the subscription.

A successful `subscribe` means that the consumer sent the subscription to
the producer. Producer acceptance remains asynchronous; for example, a
dispatcher refusal arrives later as an abnormal end of the subscription.

## Push events from outside

Some sources cannot make events on request. Their events come from a
socket, from a timer, or from a different process. `source.new_with_emitter`
gives an `Emitter` to the initialiser. Each process can push events through
the `Emitter`:

```gleam
let assert Ok(measurements) =
  source.new_with_emitter(init: fn(emitter) {
    my_socket.on_packet(fn(packet) { source.push(emitter, [packet]) })
    Ok(Nil)
  })
  |> source.start()
```

The source sends pushed events to the consumers immediately, up to the open
demand. The buffer keeps the remaining events.

## Dispatchers

A dispatcher decides which subscriber of a source or a gate receives
which events. Set it with the `dispatcher` builder function; there are
three:

- `dispatcher.demand()` (the default): each event goes to exactly one
  subscriber, the one with the largest open demand. Thus the load becomes
  equal across the subscribers with time.
- `dispatcher.broadcast()`: each event goes to each subscriber, at the
  speed of the slowest subscriber. A subscriber can set a filter with
  `sluice.selector(keep: fn(event) { ... })`; it then receives only the
  events that the filter keeps.
- `dispatcher.partition(count:, by:)`: the `by` function gives each event
  a partition, and each partition has a maximum of one subscriber. A
  subscriber selects its partition with `sluice.partition(index:)`.
  Events for a partition without open demand wait in a queue for that
  partition; thus a slow partition does not stop the other partitions.

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

A producer stage can see its subscriber group: `on_subscribers` runs when
a subscriber arrives or leaves, with the new quantity of subscribers. Use
it to start work at the first subscriber and to stop work at the last
one. A sink has two hooks: `on_subscribed` receives each new
`Subscription`, and `on_cancelled` runs when a subscription ends. When
`on_cancelled` is set, it decides what the sink does, and the cancel mode
does not apply.

## Accumulate demand at the start

A source with `accumulate_demand` holds the incoming asks and makes no
events. A call of `sluice.forward_demand(outlet:)` releases the held
asks. Use this to connect a full pipeline before the events start to
move.

## Manual demand

Some consumers must control the flow themselves, for example because they
send the events to a different process. Set the demand mode of the
subscription to `Manual`. The consumer then receives events only after a
call of `sluice.ask`:

```gleam
let assert Ok(subscription) =
  sluice.subscription(to: counter.data)
  |> sluice.demand_mode(sluice.Manual)
  |> sluice.subscribe(consumer: printer.data)

// The consumer receives a maximum of 10 events.
sluice.ask(subscription:, count: 10)
```

The `on_events` callback also receives the subscription. Thus a sink can
ask for its subsequent batch from inside the callback and set its own
speed.

## Send messages to a stage

A stage can receive messages of a type of your choice, next to the event
flow. Use this for timers, for configuration changes, and for queries.
Give the stage a message channel with `on_message`. At the start, the
`initialise` callback receives the subject of the channel. The handler
receives each message together with the state, and it has the full
vocabulary of the stage. Thus a source or a gate can emit events from
it:

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

Each subscription has a cancel mode. The cancel mode tells the consumer
stage what to do when the subscription stops. A subscription stops when the
producer stops, when the producer fails, or when you cancel the
subscription:

- `Permanent` (the default): The consumer stops also, for each stop
  reason.
- `Transient`: The consumer stops only if the producer failed. If a
  source stops correctly with `source.stop()`, the consumer continues.
- `Temporary`: The consumer continues and removes the subscription.

You can also cancel a subscription with `sluice.cancel`. Each `on_events`
callback receives its `Subscription`. Thus a stage can disconnect itself
while events move.

## Process events in parallel

`pool.sink` makes a sink that runs each event in a separate worker
process, with a limit on the quantity of parallel workers. The demand
follows the workers: the pool asks for one more event when a worker
completes. Subscribe it with the `Manual` demand mode; the pool makes its
own asks. A failure in a worker does not stop the pool:

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

For supervised event workers, use `consumer_supervisor`. It starts one linked
OTP child for each event. `max_demand` is the maximum number of credited
children for each subscription. A credit returns only when its child finally
terminates; an abnormal exit followed by a transient restart keeps the same
event and credit:

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

The child start function must return a process linked to its caller, as normal
OTP child start functions do. Initialisation is synchronous and must finish
quickly; the child performs the long-running work after it starts. Children
are `Temporary` by default. `Transient` children restart only after abnormal
exits. `Permanent` children are not supported because a successfully completed
event must not restart.

The consumer supervisor owns its demand loop, so it accepts the default
`Automatic` mode and rejects `Manual` subscriptions. If a producer connection
ends, existing children continue. The subscription's cancel mode then decides
whether the consumer supervisor remains alive; when it stops, it shuts down all
remaining children and kills children that exceed `shutdown_timeout`.

`pool.sink` remains useful for lightweight best-effort work: its workers are
unlinked, failures are logged and discarded, and each completion immediately
asks for one replacement. `consumer_supervisor` provides OTP restart policy,
restart tolerance, bounded shutdown, and child inspection instead.

## Yielders and folds

`source.from_yielder` makes a source from a yielder; the source stops
with the normal reason at the end of the yielder. A source callback can
also end the flow itself with `source.emit_final(events)`: the last
events go out, and then the source stops. In the other direction,
`sink.fold` runs the full flow of an outlet through a fold and returns
the final value:

```gleam
import gleam/yielder

let assert Ok(numbers) =
  source.from_yielder(yielder.range(1, 100)) |> source.start()
let assert Ok(total) =
  sink.fold(from: numbers.data, initial: 0, with: int.add, within: 5000)
```

## Supervision

Stages start as standard OTP actors. Thus a `static_supervisor` can hold
them as children. For connections that continue through restarts, give
names to the stages. Then declare the subscriptions on the builder. The
stage makes the declared subscriptions during its start. The start fails
for the checks that the stage can make itself: a dead producer, a
duplicate, and demand values that are not correct. Then the supervisor
does the retry. A refusal from the dispatcher of the producer comes later
as an abnormal end of the subscription:

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

Use `Permanent` subscriptions with `RestForOne`. Then, when a source stops,
its consumers stop also. The supervisor restarts the subsequent stages in
the correct sequence. Each stage connects again through the names.

## Design notes

- One consumer can hold a maximum of one subscription for each producer. A
  second `subscribe` call to the same producer returns
  `Error(AlreadySubscribed)`.
- When a stage stops, the events that move to that stage are lost with its
  mailbox. The producer acknowledges a cancellation in sequence. Thus a
  live consumer processes all events that the producer sent before the
  cancellation.
- One sink can use two sources that have different event types. To do
  this, make a sum type for the sink. Then put a small gate after each
  source. Each gate changes the source type to the sum type.

## Inspiration

Heavily inspired by Elixir's [GenStage](https://gen-stage.hexdocs.pm/GenStage.html).
