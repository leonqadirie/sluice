# Control demand yourself

Most pipelines run well on automatic demand. Two tools help when you
need direct control: manual demand for consumers that set their own
pace, and accumulated demand for starting a pipeline in one go.

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

## Accumulate demand at the start

A source with `accumulate_demand` holds incoming asks and produces
nothing. Calling `sluice.forward_demand(outlet:)` releases the held asks.
Use this to wire up a whole pipeline before any events start to move.
