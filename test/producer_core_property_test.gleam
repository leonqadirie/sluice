//// Property tests for the pure event and demand path of the producer core.
//// Generated command traces run against both the core and a small reference
//// model. qcheck shrinks a failure to a minimal trace.

import gleam/erlang/process.{type Subject}
import gleam/int
import gleam/list
import qcheck
import sluice.{type Keep, KeepFirst, KeepLast}
import sluice/internal/buffer.{type Buffer}
import sluice/internal/dispatcher.{type Dispatcher}
import sluice/internal/producer_core.{type Core, type Outbound}
import sluice/internal/protocol.{type ConsumerMessage, NewEvents}

const property_cases = 200

const subscribers_count = 3

type Command {
  Ask(subscriber: Int, demand: Int)
  Emit(count: Int)
}

type Scenario {
  Scenario(capacity: Int, keep: Keep, commands: List(Command))
}

type BufferScenario {
  BufferScenario(capacity: Int, keep: Keep, batches: List(List(Int)))
}

type Requested {
  Requested(subscriber: Int, demand: Int)
}

type AccumulationScenario {
  AccumulationScenario(buffered: Int, asks: List(Requested))
}

type CancellationScenario {
  CancellationScenario(orphaned: Int, subsequent: List(Int))
}

type Subscribers {
  Subscribers(
    first: Subject(ConsumerMessage(Int)),
    second: Subject(ConsumerMessage(Int)),
    third: Subject(ConsumerMessage(Int)),
  )
}

type Model {
  Model(buffered: List(Int), demand: List(Int), next_event: Int)
}

pub fn demand_core_matches_model_property_test() {
  qcheck.run(property_config(), scenario_generator(), run_scenario)
}

pub fn buffer_store_is_independent_of_batching_property_test() {
  use scenario <- qcheck.run(property_config(), buffer_scenario_generator())
  let BufferScenario(capacity, keep, batches) = scenario
  let empty = buffer.new(capacity: buffer.Bounded(capacity), keep:)
  let #(one_store, one_discarded) = buffer.store(empty, list.flatten(batches))
  let #(many_stores, many_discarded) =
    list.fold(batches, #(empty, 0), fn(accumulated, batch) {
      let #(stored, discarded) = accumulated
      let #(stored, more_discarded) = buffer.store(stored, batch)
      #(stored, discarded + more_discarded)
    })

  assert buffer_contents(one_store) == buffer_contents(many_stores)
  assert one_discarded == many_discarded
  assert buffer.size(many_stores) <= capacity
}

pub fn accumulated_asks_replay_like_forwarded_asks_property_test() {
  use scenario <- qcheck.run(
    property_config(),
    accumulation_scenario_generator(),
  )
  let AccumulationScenario(buffered_count, asks) = scenario
  let subjects = new_subscribers()
  let buffered = count_up(from: 0, count: buffered_count)
  let accumulated =
    new_core(subjects, capacity: 64, keep: KeepFirst, buffered:)
    |> producer_core.accumulate
  let forwarding = new_core(subjects, capacity: 64, keep: KeepFirst, buffered:)

  let accumulated =
    list.fold(asks, accumulated, fn(core, request) {
      let Requested(subscriber, demand) = request
      let #(core, outbound, unfilled) =
        producer_core.on_ask(
          core:,
          from: subject_at(subjects, subscriber),
          demand:,
        )
      assert outbound == []
      assert unfilled == 0
      core
    })
  let #(accumulated, accumulated_outbound, accumulated_unfilled) =
    producer_core.forward(accumulated)

  let #(forwarding, reversed_chunks, forwarding_unfilled) =
    list.fold(asks, #(forwarding, [], 0), fn(accumulated, request) {
      let #(core, chunks, unfilled) = accumulated
      let Requested(subscriber, demand) = request
      let #(core, outbound, more_unfilled) =
        producer_core.on_ask(
          core:,
          from: subject_at(subjects, subscriber),
          demand:,
        )
      #(core, [outbound, ..chunks], unfilled + more_unfilled)
    })
  let forwarding_outbound = reversed_chunks |> list.reverse |> list.flatten

  assert accumulated_outbound == forwarding_outbound
  assert accumulated_unfilled == forwarding_unfilled
  assert_snapshot_matches(accumulated, snapshot(forwarding))
  let assert producer_core.Forwarding = accumulated.mode
  Nil
}

pub fn cancelled_demand_absorbs_subsequent_asks_property_test() {
  use scenario <- qcheck.run(
    property_config(),
    cancellation_scenario_generator(),
  )
  let CancellationScenario(orphaned, subsequent) = scenario
  let leaver = process.new_subject()
  let stayer = process.new_subject()
  let dispatcher =
    dispatcher.demand()
    |> subscribe(leaver)
    |> subscribe(stayer)
  let #(initial_delta, _, dispatcher) = dispatcher.ask(orphaned, leaver)
  assert initial_delta == orphaned
  let #(released, dispatcher) = dispatcher.cancel(leaver)
  assert released == 0

  let #(_dispatcher, total_asked, total_delta) =
    list.fold(subsequent, #(dispatcher, 0, 0), fn(accumulated, demand) {
      let #(dispatcher, asked, released) = accumulated
      let #(delta, outbound, dispatcher) = dispatcher.ask(demand, stayer)
      let asked = asked + demand
      let released = released + delta
      assert outbound == []
      assert released == int.max(asked - orphaned, 0)
      assert dispatcher.total_demand() == asked
      #(dispatcher, asked, released)
    })

  assert orphaned + total_delta == int.max(orphaned, total_asked)
}

fn property_config() -> qcheck.Config {
  qcheck.default_config() |> qcheck.with_test_count(property_cases)
}

fn scenario_generator() -> qcheck.Generator(Scenario) {
  qcheck.map3(
    qcheck.bounded_int(0, 12),
    keep_generator(),
    qcheck.generic_list(command_generator(), qcheck.bounded_int(0, 50)),
    Scenario,
  )
}

fn buffer_scenario_generator() -> qcheck.Generator(BufferScenario) {
  let batch =
    qcheck.generic_list(qcheck.bounded_int(-20, 20), qcheck.bounded_int(0, 8))
  qcheck.map3(
    qcheck.bounded_int(0, 12),
    keep_generator(),
    qcheck.generic_list(batch, qcheck.bounded_int(0, 12)),
    BufferScenario,
  )
}

fn accumulation_scenario_generator() -> qcheck.Generator(AccumulationScenario) {
  let request =
    qcheck.map2(
      qcheck.bounded_int(0, subscribers_count - 1),
      qcheck.bounded_int(1, 8),
      Requested,
    )
  qcheck.map2(
    qcheck.bounded_int(0, 20),
    qcheck.generic_list(request, qcheck.bounded_int(0, 30)),
    AccumulationScenario,
  )
}

fn cancellation_scenario_generator() -> qcheck.Generator(CancellationScenario) {
  qcheck.map2(
    qcheck.bounded_int(1, 20),
    qcheck.generic_list(qcheck.bounded_int(1, 10), qcheck.bounded_int(0, 20)),
    CancellationScenario,
  )
}

fn keep_generator() -> qcheck.Generator(Keep) {
  qcheck.bool()
  |> qcheck.map(fn(keep_last) {
    case keep_last {
      True -> KeepLast
      False -> KeepFirst
    }
  })
}

fn command_generator() -> qcheck.Generator(Command) {
  let ask =
    qcheck.map2(
      qcheck.bounded_int(0, subscribers_count - 1),
      qcheck.bounded_int(1, 8),
      Ask,
    )
  let emit = qcheck.bounded_int(0, 8) |> qcheck.map(Emit)
  qcheck.from_weighted_generators(#(3, ask), [#(2, emit)])
}

fn run_scenario(scenario: Scenario) -> Nil {
  let Scenario(capacity, keep, commands) = scenario
  let subjects = new_subscribers()
  let core = new_core(subjects, capacity:, keep:, buffered: [])
  let model = Model(buffered: [], demand: [0, 0, 0], next_event: 0)
  let #(core, model) =
    list.fold(commands, #(core, model), fn(state, command) {
      step(state.0, state.1, subjects, capacity, keep, command)
    })
  assert_snapshot_matches(core, model)
}

fn step(
  core: Core(Int),
  model: Model,
  subjects: Subscribers,
  capacity: Int,
  keep: Keep,
  command: Command,
) -> #(Core(Int), Model) {
  case command {
    Ask(subscriber, demand) -> {
      let model =
        Model(
          ..model,
          demand: update_demand(model.demand, subscriber, by: fn(current) {
            current + demand
          }),
        )
      let #(core, outbound, unfilled) =
        producer_core.on_ask(
          core:,
          from: subject_at(subjects, subscriber),
          demand:,
        )
      let model = apply_outbound(model, subjects, outbound)
      assert unfilled >= 0
      assert unfilled <= demand
      assert_snapshot_matches(core, model)
      #(core, model)
    }
    Emit(count) -> {
      let events = count_up(from: model.next_event, count:)
      let model =
        Model(
          ..model,
          buffered: list.append(model.buffered, events),
          next_event: model.next_event + count,
        )
      let producer_core.EmitResult(core:, outbound:, discarded:, unfilled:) =
        producer_core.emit(core:, events:)
      let model = apply_outbound(model, subjects, outbound)
      let #(buffered, expected_discarded) =
        apply_capacity(model.buffered, capacity, keep)
      let model = Model(..model, buffered:)
      assert discarded == expected_discarded
      assert unfilled == 0
      assert_snapshot_matches(core, model)
      #(core, model)
    }
  }
}

fn apply_outbound(
  model: Model,
  subjects: Subscribers,
  outbound: List(Outbound(Int)),
) -> Model {
  list.fold(outbound, model, fn(model, entry) {
    let producer_core.Outbound(to, message) = entry
    let assert NewEvents(events) = message
    assert events != []
    let subscriber = subject_index(subjects, to)
    let open = demand_at(model.demand, subscriber)
    let greatest = list.fold(model.demand, 0, int.max)
    assert open == greatest
    assert list.length(events) <= open
    let #(expected, buffered) = list.split(model.buffered, list.length(events))
    assert events == expected
    let demand =
      update_demand(model.demand, subscriber, by: fn(current) {
        current - list.length(events)
      })
    Model(..model, buffered:, demand:)
  })
}

fn assert_snapshot_matches(core: Core(Int), model: Model) -> Nil {
  assert buffer_contents(core.buffer) == model.buffered
  assert core.dispatcher.total_demand() == list.fold(model.demand, 0, int.add)
}

fn snapshot(core: Core(Int)) -> Model {
  Model(
    buffered: buffer_contents(core.buffer),
    demand: [core.dispatcher.total_demand()],
    next_event: 0,
  )
}

fn new_core(
  subjects: Subscribers,
  capacity capacity: Int,
  keep keep: Keep,
  buffered buffered: List(Int),
) -> Core(Int) {
  let stored = buffer.new(capacity: buffer.Bounded(capacity), keep:)
  let #(stored, _discarded) = buffer.store(stored, buffered)
  producer_core.new(dispatcher: subscribed_dispatcher(subjects), buffer: stored)
}

fn subscribed_dispatcher(subjects: Subscribers) -> Dispatcher(Int) {
  dispatcher.demand()
  |> subscribe(subjects.first)
  |> subscribe(subjects.second)
  |> subscribe(subjects.third)
}

fn subscribe(
  dispatcher: Dispatcher(event),
  subject: Subject(ConsumerMessage(event)),
) -> Dispatcher(event) {
  let assert Ok(dispatcher) =
    dispatcher.subscribe(subject, protocol.default_metadata())
  dispatcher
}

fn new_subscribers() -> Subscribers {
  Subscribers(
    first: process.new_subject(),
    second: process.new_subject(),
    third: process.new_subject(),
  )
}

fn subject_at(
  subjects: Subscribers,
  index: Int,
) -> Subject(ConsumerMessage(Int)) {
  case index {
    0 -> subjects.first
    1 -> subjects.second
    _ -> subjects.third
  }
}

fn subject_index(
  subjects: Subscribers,
  subject: Subject(ConsumerMessage(Int)),
) -> Int {
  case subject == subjects.first, subject == subjects.second {
    True, _ -> 0
    False, True -> 1
    False, False -> {
      assert subject == subjects.third
      2
    }
  }
}

fn demand_at(demands: List(Int), index: Int) -> Int {
  case demands, index {
    [demand, ..], 0 -> demand
    [_, ..remaining], index -> demand_at(remaining, index - 1)
    [], _ -> panic as "subscriber index outside the demand model"
  }
}

fn update_demand(
  demands: List(Int),
  index: Int,
  by update: fn(Int) -> Int,
) -> List(Int) {
  case demands, index {
    [demand, ..remaining], 0 -> [update(demand), ..remaining]
    [demand, ..remaining], index -> [
      demand,
      ..update_demand(remaining, index - 1, by: update)
    ]
    [], _ -> panic as "subscriber index outside the demand model"
  }
}

fn apply_capacity(
  events: List(Int),
  capacity: Int,
  keep: Keep,
) -> #(List(Int), Int) {
  let discarded = int.max(list.length(events) - capacity, 0)
  case keep {
    KeepFirst -> {
      let #(kept, _) = list.split(events, capacity)
      #(kept, discarded)
    }
    KeepLast -> {
      let #(_, kept) = list.split(events, discarded)
      #(kept, discarded)
    }
  }
}

fn buffer_contents(buffer: Buffer(event)) -> List(event) {
  buffer.take(buffer, buffer.size(buffer)).0
}

fn count_up(from from: Int, count count: Int) -> List(Int) {
  int.range(from: from, to: from + count, with: [], run: list.prepend)
  |> list.reverse
}
