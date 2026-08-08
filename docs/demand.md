# Demand and buffers

The readme explains why demand flows backward. This page explains the
numbers: how much a consumer asks for, when it asks again, and what
producers do with events nobody has asked for yet.

## The demand cycle

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

## Buffers

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
