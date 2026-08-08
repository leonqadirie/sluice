# Fan out and fan in

A pipeline doesn't have to be a straight line. A producer can have many
subscribers, and its dispatcher decides which events go where: split the
work across subscribers, copy every event to all of them, or partition
by key (see [Dispatchers](#dispatchers) below). With the default demand
dispatcher, a slow subscriber doesn't hold the others back — events flow
to whoever has open demand.

A consumer can also subscribe to many producers. Each subscription
tracks its own demand, and the `on_events` callback receives the
subscription a batch came from, so a sink can tell its producers apart.
A consumer holds at most one subscription per producer: a second
`subscribe` call to the same producer returns `Error(AlreadySubscribed)`.

The producers feeding one consumer must share one event type. To merge
sources with different types, define a sum type for the consumer, then
put a small gate after each source that wraps the source's type in the
sum type.

Combine both directions and a pipeline becomes a graph: one source
feeding several gates, or several gates feeding one sink.

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
