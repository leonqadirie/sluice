//// Tests for the partition dispatcher: routes by hash, queues for
//// partitions without demand, and refusal of subscriptions without a
//// partition.

import gleam/erlang/process.{type Subject}
import gleam/list
import sluice
import sluice/dispatcher
import sluice/sink
import sluice/source
import support

fn partitioned_counter() {
  source.new(init: 0, on_demand: fn(counter, demand) {
    source.emit(support.count_up(counter, demand), counter + demand)
  })
  |> source.dispatcher(dispatcher.partition(count: 2, by: fn(event) { event }))
}

fn collect(
  probe: Subject(List(Int)),
  target: Int,
  received: List(Int),
) -> List(Int) {
  case list.length(received) >= target {
    True -> received
    False ->
      case process.receive(probe, 1000) {
        Error(Nil) -> received
        Ok(events) -> collect(probe, target, list.append(received, events))
      }
  }
}

// Events go to the partition of their hash. Events for a partition
// without open demand wait in the queue of that partition, and a
// subsequent ask takes them from the queue.
pub fn partition_routes_by_hash_test() {
  let even_probe = process.new_subject()
  let odd_probe = process.new_subject()
  let assert Ok(counter) = partitioned_counter() |> source.start()
  let assert Ok(evens) = support.collector(even_probe) |> sink.start()
  let assert Ok(odds) = support.collector(odd_probe) |> sink.start()

  let assert Ok(even_subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.partition(index: 0)
    |> sluice.subscribe(consumer: evens.data)
  let assert Ok(odd_subscription) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.partition(index: 1)
    |> sluice.subscribe(consumer: odds.data)

  // The even partition asks first. The source makes [0, 1]: 0 goes out,
  // 1 waits in the queue of the odd partition.
  sluice.ask(subscription: even_subscription, count: 2)
  let assert Ok([0]) = process.receive(even_probe, 1000)
  assert process.receive(odd_probe, 150) == Error(Nil)

  // The ask of the odd partition first takes the queued event, and the
  // new production supplies the remaining demand of both partitions.
  sluice.ask(subscription: odd_subscription, count: 2)
  assert collect(even_probe, 2, [0]) == [0, 2]
  assert collect(odd_probe, 2, []) == [1, 3]

  support.shutdown(odds)
  support.shutdown(evens)
  support.shutdown(counter)
}

// A partition dispatcher refuses a subscription without a partition. The
// refusal comes as an abnormal end of the subscription, and a Permanent
// sink stops because of it.
pub fn partition_requires_an_index_test() {
  let assert Ok(counter) = partitioned_counter() |> source.start()
  let assert Ok(lost) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.start()
  process.unlink(counter.pid)
  process.unlink(lost.pid)
  let sink_monitor = process.monitor(lost.pid)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.subscribe(consumer: lost.data)

  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(sink_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Abnormal(_))) =
    process.selector_receive(down_selector, 2000)

  support.shutdown(counter)
}

// One partition accepts a maximum of one subscriber.
pub fn partition_accepts_one_subscriber_test() {
  let batch_probe = process.new_subject()
  let assert Ok(counter) = partitioned_counter() |> source.start()
  let assert Ok(holder) = support.collector(batch_probe) |> sink.start()
  let assert Ok(second) =
    sink.new(init: Nil, on_events: fn(state, _events, _subscription) {
      sink.continue(state)
    })
    |> sink.start()
  process.unlink(second.pid)
  let second_monitor = process.monitor(second.pid)

  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.demand_mode(sluice.Manual)
    |> sluice.partition(index: 0)
    |> sluice.subscribe(consumer: holder.data)
  let assert Ok(_) =
    sluice.subscription(to: counter.data)
    |> sluice.partition(index: 0)
    |> sluice.subscribe(consumer: second.data)

  let down_selector =
    process.new_selector()
    |> process.select_specific_monitor(second_monitor, fn(down) { down })
  let assert Ok(process.ProcessDown(_, _, process.Abnormal(_))) =
    process.selector_receive(down_selector, 2000)

  support.shutdown(holder)
  support.shutdown(counter)
}
