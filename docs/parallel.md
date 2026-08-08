# Process events in parallel

Two tools run each event in its own process: a worker pool for
lightweight, best-effort work, and a consumer supervisor for work that
needs restart policies.

## Worker pools

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

## Supervised event workers

`consumer_supervisor` starts one linked OTP child for each event.
`max_demand` sets the number of slots for each subscription: every
running child occupies one slot, and the slot frees up only when the
child finally terminates. An abnormal exit followed by a transient
restart keeps the same event and slot:

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
