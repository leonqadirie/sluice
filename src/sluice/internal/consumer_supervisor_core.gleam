//// Pure demand and restart decisions for a consumer supervisor.
////
//// This module owns no processes and sends no messages. The actor shell
//// records deliveries and child exits here, then executes returned asks or
//// stops itself when the restart limit is reached.

import gleam/dict.{type Dict}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result

type Demand {
  Demand(min: Int, max: Int, freed: Int)
}

pub opaque type Core(subscription) {
  Core(
    demands: Dict(subscription, Demand),
    max_restarts: Int,
    restart_period_ms: Int,
    restarts: List(Int),
  )
}

pub type DemandState {
  DemandState(min: Int, max: Int, freed: Int)
}

pub fn new(
  max_restarts max_restarts: Int,
  restart_period_ms restart_period_ms: Int,
) -> Core(subscription) {
  Core(
    demands: dict.new(),
    max_restarts: int.max(max_restarts, 0),
    restart_period_ms: int.max(restart_period_ms, 1),
    restarts: [],
  )
}

pub fn subscribed(
  core core: Core(subscription),
  subscription subscription: subscription,
  min min: Int,
  max max: Int,
) -> Core(subscription) {
  Core(
    ..core,
    demands: dict.insert(
      core.demands,
      subscription,
      Demand(min:, max:, freed: 0),
    ),
  )
}

pub fn unsubscribed(
  core core: Core(subscription),
  subscription subscription: subscription,
) -> Core(subscription) {
  Core(..core, demands: dict.delete(core.demands, subscription))
}

/// Record one finally terminated child. Closed subscriptions do not change
/// demand.
pub fn released(
  core core: Core(subscription),
  subscription subscription: subscription,
) -> Core(subscription) {
  case dict.get(core.demands, subscription) {
    Error(Nil) -> core
    Ok(demand) ->
      Core(
        ..core,
        demands: dict.insert(
          core.demands,
          subscription,
          Demand(..demand, freed: demand.freed + 1),
        ),
      )
  }
}

/// Return an ask when the count of occupied slots has decreased to `min`
/// or less. The amount restores the open demand exactly to `max`,
/// including when several completions were recorded before this function
/// ran.
pub fn take_ask(
  core core: Core(subscription),
  subscription subscription: subscription,
) -> #(Core(subscription), Option(Int)) {
  case dict.get(core.demands, subscription) {
    Error(Nil) -> #(core, None)
    Ok(demand) -> {
      let occupied = demand.max - demand.freed
      case occupied <= demand.min && demand.freed > 0 {
        False -> #(core, None)
        True -> {
          let demands =
            dict.insert(core.demands, subscription, Demand(..demand, freed: 0))
          #(Core(..core, demands:), Some(demand.freed))
        }
      }
    }
  }
}

/// Record a restart attempt. `False` means that this attempt exceeds the
/// configured tolerance and the supervisor must stop.
pub fn restart_allowed(
  core core: Core(subscription),
  now_ms now_ms: Int,
) -> #(Core(subscription), Bool) {
  let recent =
    core.restarts
    |> list.filter(fn(then) { now_ms - then <= core.restart_period_ms })
  let restarts = [now_ms, ..recent]
  #(Core(..core, restarts:), list.length(restarts) <= core.max_restarts)
}

pub fn demand_state(
  core core: Core(subscription),
  subscription subscription: subscription,
) -> Result(DemandState, Nil) {
  dict.get(core.demands, subscription)
  |> result.map(fn(demand) {
    DemandState(min: demand.min, max: demand.max, freed: demand.freed)
  })
}

pub fn restart_count(core: Core(subscription)) -> Int {
  list.length(core.restarts)
}
