# Supervision

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

A consumer supervisor joins the tree the same way. Give it a name with
`consumer_supervisor.new_name`, declare its subscription on the builder
with `consumer_supervisor.subscribe`, and add it as a worker child:

```gleam
pub fn start_deliveries() {
  let orders_name = source.new_name("orders")
  let deliveries_name = consumer_supervisor.new_name("deliveries")

  supervisor.new(supervisor.RestForOne)
  |> supervisor.add(
    supervision.worker(fn() {
      order_source() |> source.named(orders_name) |> source.start()
    }),
  )
  |> supervisor.add(
    supervision.worker(fn() {
      consumer_supervisor.new(start_delivery)
      |> consumer_supervisor.named(deliveries_name)
      |> consumer_supervisor.subscribe(
        sluice.subscription(to: source.outlet_of(orders_name)),
      )
      |> consumer_supervisor.start()
    }),
  )
  |> supervisor.start()
}
```

The two supervisors do different jobs: the static supervisor restarts
the stages of the pipeline, and the consumer supervisor restarts its
own children, one for each event.
