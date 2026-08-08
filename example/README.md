# Sluice examples

Each module in `src` is one runnable example. Each example prints its
flow of events, ends by itself, and explains its steps in comments.
Read the examples in this sequence; each one builds on the ones before
it:

1. `counter_pipeline` — The basic pipeline: a source, a gate, and a
   sink. The output shows how the demand of a slow sink sets the speed
   of the full pipeline.
2. `push_and_buffer` — A source that receives pushed events from a
   different process, and a bounded buffer that overflows on purpose.
3. `broadcast_with_filter` — One event flow to many sinks. One sink
   filters the events with a selector.
4. `partition_by_key` — Events divided across sinks by a key. A slow
   partition does not stop a fast partition.
5. `worker_pool` — Events processed in parallel worker processes, with
   a limit on the quantity of parallel workers.
6. `event_workers` — A supervised OTP child process for each event.
7. `supervised_pipeline` — Named stages under a supervisor. The example
   kills a stage, and the supervisor reconnects the pipeline.

## Run an example

Run each example from this directory:

```sh
gleam run --module counter_pipeline
```

Run `gleam run` without a module to print this list.
