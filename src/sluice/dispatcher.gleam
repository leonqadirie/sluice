//// A dispatcher decides which subscriber of a producer stage receives
//// which events. Set the dispatcher of a source or a gate with the
//// `dispatcher` builder function of the stage. The default is `demand`.

import sluice/internal/broadcast
import sluice/internal/dispatcher as runtime
import sluice/internal/partition

/// A dispatcher for a producer stage. Each start of a stage receives a
/// new, empty instance.
pub opaque type Dispatcher(event) {
  Dispatcher(build: fn() -> runtime.Dispatcher(event))
}

/// The default dispatcher. Events go to the subscriber with the largest
/// open demand. Thus, with time, the load becomes equal across the
/// subscribers. Each event goes to exactly one subscriber.
pub fn demand() -> Dispatcher(event) {
  Dispatcher(build: runtime.demand)
}

/// Each event goes to each subscriber. The flow moves at the speed of the
/// slowest subscriber. A subscriber can set a filter on its subscription
/// with `sluice.selector`. It then receives only the events that the
/// filter keeps. A new subscriber stops the flow until it asks.
pub fn broadcast() -> Dispatcher(event) {
  Dispatcher(build: broadcast.new)
}

/// The `by` function gives each event a partition from 0 to `count` - 1,
/// and each event goes to the subscriber of its partition. Each partition
/// accepts a maximum of one subscriber. A subscriber selects its
/// partition with `sluice.partition`. Events for a partition without open
/// demand wait in a queue for that partition. Thus a slow partition does
/// not stop the other partitions.
///
/// * `count`: The quantity of partitions.
/// * `by`: The function that gives one event its partition, from 0 to
///   `count` - 1.
pub fn partition(
  count count: Int,
  by by: fn(event) -> Int,
) -> Dispatcher(event) {
  Dispatcher(build: fn() { partition.new(count, by) })
}

@internal
pub fn build(dispatcher: Dispatcher(event)) -> runtime.Dispatcher(event) {
  dispatcher.build()
}
