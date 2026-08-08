//// Properties for the process-free consumer-supervisor decisions.

import gleam/list
import gleam/option.{None, Some}
import qcheck
import sluice/internal/consumer_supervisor_core as core

const property_cases = 300

type DemandScenario {
  DemandScenario(max: Int, min_seed: Int, completions: Int)
}

type RestartScenario {
  RestartScenario(max_restarts: Int, period_ms: Int, deltas: List(Int))
}

pub fn demand_replenishment_matches_credit_model_property_test() {
  qcheck.run(property_config(), demand_scenario_generator(), fn(scenario) {
    let DemandScenario(max, min_seed, completions) = scenario
    let min = min_seed % max
    let decisions = core.new(3, 5000)
    let decisions = core.subscribed(decisions, 0, min, max)

    let #(decisions, model_freed) =
      list.repeat(Nil, times: completions)
      |> list.fold(#(decisions, 0), fn(state, _completion) {
        let #(decisions, freed) = state
        let decisions = core.released(decisions, 0)
        let #(decisions, ask) = core.take_ask(decisions, 0)
        let expected_freed = freed + 1
        let expected_ask = case max - expected_freed <= min {
          True -> Some(expected_freed)
          False -> None
        }
        assert ask == expected_ask
        let freed = case expected_ask {
          Some(_) -> 0
          None -> expected_freed
        }
        let assert Ok(core.DemandState(_, _, actual_freed)) =
          core.demand_state(decisions, 0)
        assert actual_freed == freed
        #(decisions, freed)
      })

    let assert Ok(core.DemandState(actual_min, actual_max, actual_freed)) =
      core.demand_state(decisions, 0)
    assert actual_min == min
    assert actual_max == max
    assert actual_freed == model_freed
    assert actual_freed == 0 || max - actual_freed > min
  })
}

pub fn closed_subscription_never_releases_more_demand_property_test() {
  use scenario <- qcheck.run(property_config(), demand_scenario_generator())
  let DemandScenario(max, min_seed, completions) = scenario
  let min = min_seed % max
  let decisions =
    core.new(3, 5000)
    |> core.subscribed(0, min, max)
    |> core.unsubscribed(0)
  let decisions =
    list.repeat(Nil, times: completions)
    |> list.fold(decisions, fn(decisions, _completion) {
      core.released(decisions, 0)
    })
  let #(_decisions, ask) = core.take_ask(decisions, 0)
  assert ask == None
  assert core.demand_state(decisions, 0) == Error(Nil)
}

pub fn restart_window_matches_reference_model_property_test() {
  use scenario <- qcheck.run(property_config(), restart_scenario_generator())
  let RestartScenario(max_restarts, period_ms, deltas) = scenario
  let decisions = core.new(max_restarts, period_ms)
  let #(_decisions, _now, _model) =
    list.fold(deltas, #(decisions, 0, []), fn(state, delta) {
      let #(decisions, now, model) = state
      let now = now + delta
      let model =
        model
        |> list.filter(fn(then) { now - then <= period_ms })
        |> fn(recent) { [now, ..recent] }
      let #(decisions, allowed) = core.restart_allowed(decisions, now)
      assert allowed == { list.length(model) <= max_restarts }
      assert core.restart_count(decisions) == list.length(model)
      #(decisions, now, model)
    })
  Nil
}

fn property_config() -> qcheck.Config {
  qcheck.default_config() |> qcheck.with_test_count(property_cases)
}

fn demand_scenario_generator() -> qcheck.Generator(DemandScenario) {
  qcheck.map3(
    qcheck.bounded_int(1, 50),
    qcheck.bounded_int(0, 100),
    qcheck.bounded_int(0, 200),
    DemandScenario,
  )
}

fn restart_scenario_generator() -> qcheck.Generator(RestartScenario) {
  qcheck.map3(
    qcheck.bounded_int(0, 10),
    qcheck.bounded_int(1, 100),
    qcheck.generic_list(qcheck.bounded_int(0, 50), qcheck.bounded_int(0, 40)),
    RestartScenario,
  )
}
