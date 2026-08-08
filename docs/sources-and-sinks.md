# Sources and sinks in practice

Three patterns for connecting a pipeline to the rest of your program:
push events in from outside, send messages to a running stage, and move
data between pipelines and ordinary values.

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
open demand. The buffer holds the rest. Backpressure can't slow the
outside world down: if pushes keep outrunning demand, the buffer fills
and the overflow policy decides what to drop.

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
