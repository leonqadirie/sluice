# sluice

[![Package Version](https://img.shields.io/hexpm/v/sluice)](https://hex.pm/packages/sluice)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/sluice/)

Stage pipelines for Gleam, with demand control and backpressure.

A pipeline is a chain of stages that events flow through; stages can also
branch and merge into a graph. There are three kinds of stages:

- A **Source** produces events.
- A **Gate** receives events, transforms them, and passes them on. A gate
  can emit 0-n events for each event it receives.
- A **Sink** consumes events.

Events flow forward through the pipeline. Demand flows backward:

```
events:   Source ──▶ Gate ──▶ Sink
demand:   Source ◀── Gate ◀── Sink
```

Nothing moves until the consuming side asks for events. Because of this,
a slow sink is never overwhelmed: backpressure spreads to the stages
before it and mailboxes stay bounded. No event is dropped silently:
a stage that must drop events logs a warning first.

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

## What sluice does not do

- Sluice does not store events. Events live in mailboxes and buffers,
  and they are lost when a stage stops or a full buffer drops them. Keep
  a durable copy outside the pipeline if you need one.
- Sluice does not deliver events a second time. When a consumer crashes,
  the events on their way to it are lost. If every event must be
  handled, feed the pipeline from a source that can replay, such as a
  job queue or an event store.
- Sluice does not spread work across machines. A pipeline runs on one
  BEAM node, using as many processes on that node as you give it.

## How do stages connect?

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
gleam add sluice@1
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

## Who sets the pace?

The consumer does. When an inlet subscribes to an outlet, the consumer
asks the producer for at most `max_demand` events. The events arrive in
batches, and when the open demand — asked for but not yet processed —
falls to `min_demand`, the consumer asks again. It never holds more than
`max_demand` unprocessed events, so its mailbox stays small.

A producer that makes more events than were asked for keeps the extras
in a buffer and serves later demand from the buffer first. A gate passes
demand along: it asks the stages before it only when the stages after it
ask. That is how the pace of the slowest sink reaches the source.

The [Demand and buffers](https://hexdocs.pm/sluice/demand.html) guide
walks through the full cycle, the buffer overflow policies, and how to
tune `min_demand` and `max_demand`.

## What happens when a stage stops?

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

When a stage stops, the events on their way to it are lost with its
mailbox. The producer acknowledges a cancellation in order, so a live
consumer processes every event the producer sent before the
cancellation.

## Guides

The details live in the guides on hexdocs:

- [Demand and buffers](https://hexdocs.pm/sluice/demand.html): the full
  demand cycle, buffer overflow policies, and tuning.
- [Fan out and fan in](https://hexdocs.pm/sluice/fan-out.html): many
  subscribers, many producers, and the dispatchers that route events
  between them.
- [Sources and sinks in practice](https://hexdocs.pm/sluice/sources-and-sinks.html):
  push events from outside, send messages to a stage, and use yielders
  and folds.
- [Control demand yourself](https://hexdocs.pm/sluice/manual-demand.html):
  manual demand and accumulated demand.
- [Process events in parallel](https://hexdocs.pm/sluice/parallel.html):
  worker pools and supervised event workers.
- [Supervision](https://hexdocs.pm/sluice/supervision.html): pipelines
  under supervision trees that reconnect after restarts.
- [Defaults and timeouts](https://hexdocs.pm/sluice/defaults.html):
  every default value, and what a subscription timeout means.

## Inspiration

Heavily inspired by Elixir's [GenStage](https://gen-stage.hexdocs.pm/GenStage.html).
