//// A demand-driven supervisor that starts one linked child for each event.
////
//// The supervisor owns both sides of the coordination: it receives events,
//// starts and tracks linked children, applies restart policy, and returns
//// demand only after a child finally terminates. Keeping those decisions in
//// one process avoids races between a separate consumer and supervisor.
////
//// Child start functions must follow the OTP child contract: they return an
//// `actor.Started` value whose process is linked to the caller. Child
//// initialisation is synchronous, so it should finish quickly.

import gleam/bool
import gleam/dict.{type Dict}
import gleam/dynamic.{type Dynamic}
import gleam/erlang/atom
import gleam/erlang/process.{
  type Down, type ExitMessage, type ExitReason, type Pid, type Selector,
  type Subject,
}
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string
import sluice.{type Inlet, type SubscriptionOptions}
import sluice/internal/consumer_core
import sluice/internal/consumer_supervisor_core as supervisor_core
import sluice/internal/exit_reason
import sluice/internal/platform
import sluice/internal/protocol.{type ConsumerMessage}

const default_start_timeout = 5000

const default_call_timeout = 30_000

const default_max_restarts = 3

const default_restart_period = 5

const default_shutdown_timeout = 5000

/// When an event child is restarted.
pub type Restart {
  /// Restart a child only after an abnormal exit.
  Transient
  /// Never restart a child.
  Temporary
}

/// The visible state of a supervised child.
pub type ChildStatus {
  Running(pid: Pid)
  Restarting
  Terminating(pid: Pid)
}

/// A supervised child and its stable identifier.
pub type Child {
  Child(id: Int, status: ChildStatus)
}

/// Counts of the children owned by a consumer supervisor.
pub type ChildrenCount {
  ChildrenCount(active: Int, restarting: Int, total: Int)
}

pub type TerminateError {
  ChildNotFound
  TerminateSupervisorNotAlive
}

/// A reference to a running consumer supervisor.
pub opaque type Supervisor(event, child_data) {
  Supervisor(inlet: Inlet(event), send: fn(Control(event, child_data)) -> Nil)
}

/// A permanent name for a consumer supervisor.
pub opaque type Name(event, child_data) {
  Name(name: process.Name(Control(event, child_data)))
}

/// Make a permanent name. Create names during application start, not inside
/// a dynamic loop.
pub fn new_name(prefix prefix: String) -> Name(event, child_data) {
  Name(process.new_name(prefix))
}

/// Find the process currently using a name.
pub fn whereis(name: Name(event, child_data)) -> Result(Pid, Nil) {
  process.named(name.name)
}

/// Make a reference to the process using a name.
pub fn supervisor_of(
  name: Name(event, child_data),
) -> Supervisor(event, child_data) {
  let send = fn(message) { platform.send_named(name.name, message) }
  Supervisor(inlet: make_inlet(send, fn() { process.named(name.name) }), send:)
}

/// Get the input face of a named supervisor.
pub fn inlet_of(name: Name(event, child_data)) -> Inlet(event) {
  supervisor_of(name).inlet
}

/// Get the input face of a running supervisor.
pub fn inlet(supervisor: Supervisor(event, child_data)) -> Inlet(event) {
  supervisor.inlet
}

/// Configuration for a consumer supervisor.
pub opaque type Builder(event, child_data) {
  Builder(
    start_child: fn(event) -> actor.StartResult(child_data),
    restart: Restart,
    max_restarts: Int,
    restart_period_seconds: Int,
    shutdown_timeout: Int,
    start_timeout: Int,
    subscriptions: List(SubscriptionOptions(event)),
    name: Option(Name(event, child_data)),
  )
}

/// Define a consumer supervisor. Children are temporary by default.
pub fn new(
  start_child start_child: fn(event) -> actor.StartResult(child_data),
) -> Builder(event, child_data) {
  Builder(
    start_child:,
    restart: Temporary,
    max_restarts: default_max_restarts,
    restart_period_seconds: default_restart_period,
    shutdown_timeout: default_shutdown_timeout,
    start_timeout: default_start_timeout,
    subscriptions: [],
    name: None,
  )
}

/// Set the child restart policy.
pub fn restart(
  builder builder: Builder(event, child_data),
  policy policy: Restart,
) -> Builder(event, child_data) {
  Builder(..builder, restart: policy)
}

/// Set the maximum child restarts allowed during a period in seconds.
pub fn restart_tolerance(
  builder builder: Builder(event, child_data),
  max_restarts max_restarts: Int,
  within_seconds within_seconds: Int,
) -> Builder(event, child_data) {
  Builder(
    ..builder,
    max_restarts: int.max(max_restarts, 0),
    restart_period_seconds: int.max(within_seconds, 1),
  )
}

/// Set how long children have to stop before they are killed.
pub fn shutdown_timeout(
  builder builder: Builder(event, child_data),
  milliseconds milliseconds: Int,
) -> Builder(event, child_data) {
  Builder(..builder, shutdown_timeout: int.max(milliseconds, 0))
}

/// Set the maximum time allowed for supervisor initialisation.
pub fn start_timeout(
  builder builder: Builder(event, child_data),
  milliseconds milliseconds: Int,
) -> Builder(event, child_data) {
  Builder(..builder, start_timeout: int.max(milliseconds, 1))
}

/// Declare a subscription that is established when the supervisor starts.
/// Consumer supervisors own their demand loop, so the subscription must use
/// the default `Automatic` demand mode.
pub fn subscribe(
  builder builder: Builder(event, child_data),
  options options: SubscriptionOptions(event),
) -> Builder(event, child_data) {
  Builder(..builder, subscriptions: [options, ..builder.subscriptions])
}

/// Give the supervisor a permanent name.
pub fn named(
  builder builder: Builder(event, child_data),
  name name: Name(event, child_data),
) -> Builder(event, child_data) {
  Builder(..builder, name: Some(name))
}

/// Start the supervisor as a linked OTP child.
pub fn start(
  builder: Builder(event, child_data),
) -> actor.StartResult(Supervisor(event, child_data)) {
  let parent = process.self()
  actor.new_with_initialiser(builder.start_timeout, fn(_default) {
    process.trap_exits(True)
    use #(state, supervisor) <- result.try(initialise(builder, parent))
    actor.initialised(state)
    |> actor.selecting(state.selector)
    |> actor.returning(supervisor)
    |> Ok
  })
  |> actor.on_message(handle_actor_message)
  |> actor.start()
}

/// Ask a child to stop. Successfully terminated event children return one
/// demand credit to their producer.
pub fn terminate_child(
  supervisor supervisor: Supervisor(event, child_data),
  pid pid: Pid,
) -> Result(Nil, TerminateError) {
  let reply = process.new_subject()
  supervisor.send(TerminateChild(pid, reply))
  process.receive(reply, default_call_timeout)
  |> result.unwrap(Error(TerminateSupervisorNotAlive))
}

/// Return the supervisor's current children.
pub fn which_children(
  supervisor: Supervisor(event, child_data),
) -> Result(List(Child), Nil) {
  let reply = process.new_subject()
  supervisor.send(WhichChildren(reply))
  process.receive(reply, default_call_timeout)
}

/// Return child counts.
pub fn count_children(
  supervisor: Supervisor(event, child_data),
) -> Result(ChildrenCount, Nil) {
  let reply = process.new_subject()
  supervisor.send(CountChildren(reply))
  process.receive(reply, default_call_timeout)
}

type Control(event, child_data) {
  Stage(sluice.ConsumerControl(event))
  FromUpstream(
    subject: Subject(ConsumerMessage(event)),
    message: ConsumerMessage(event),
  )
  ProducerDown(subject: Subject(ConsumerMessage(event)), down: Down)
  LinkedExit(ExitMessage)
  RetryRestart(job_id: Int)
  TerminateChild(pid: Pid, reply: Subject(Result(Nil, TerminateError)))
  TerminateTimedOut(job_id: Int, pid: Pid)
  WhichChildren(reply: Subject(List(Child)))
  CountChildren(reply: Subject(ChildrenCount))
}

type Pending(event) {
  Pending(subject: Subject(ConsumerMessage(event)), min: Int, max: Int)
}

type Job(event) {
  Job(
    id: Int,
    event: event,
    subscription: Subject(ConsumerMessage(event)),
    pid: Option(Pid),
    terminating: Option(Subject(Result(Nil, TerminateError))),
  )
}

type Runtime(event, child_data) {
  Runtime(
    start_child: fn(event) -> actor.StartResult(child_data),
    restart: Restart,
    shutdown_timeout: Int,
    next_job_id: Int,
    jobs: Dict(Int, Job(event)),
    decisions: supervisor_core.Core(Subject(ConsumerMessage(event))),
    pending: Dict(
      Subject(Result(sluice.Subscription, sluice.SubscribeError)),
      Pending(event),
    ),
  )
}

type State(event, child_data) {
  State(
    parent: Pid,
    core: consumer_core.Core(event),
    selector: Selector(Control(event, child_data)),
    control_subject: Subject(Control(event, child_data)),
    runtime: Runtime(event, child_data),
  )
}

type Next(event, child_data) {
  Continue(State(event, child_data))
  Stop(State(event, child_data), ExitReason)
}

// nolint: stringly_typed_error -- the public actor start contract uses String init errors
fn initialise(
  builder: Builder(event, child_data),
  parent: Pid,
) -> Result(#(State(event, child_data), Supervisor(event, child_data)), String) {
  use control_subject <- result.try(select_control_subject(builder.name))
  let send = fn(message) { process.send(control_subject, message) }
  let supervisor =
    Supervisor(
      inlet: make_inlet(send, fn() { process.subject_owner(control_subject) }),
      send:,
    )
  let selector =
    process.new_selector()
    |> process.select(control_subject)
    |> process.select_trapped_exits(LinkedExit)
  let runtime =
    Runtime(
      start_child: builder.start_child,
      restart: builder.restart,
      shutdown_timeout: builder.shutdown_timeout,
      next_job_id: 0,
      jobs: dict.new(),
      decisions: supervisor_core.new(
        builder.max_restarts,
        builder.restart_period_seconds * 1000,
      ),
      pending: dict.new(),
    )
  let state =
    State(
      parent:,
      core: consumer_core.new(),
      selector:,
      control_subject:,
      runtime:,
    )
  use state <- result.try(establish_declared(
    state,
    list.reverse(builder.subscriptions),
  ))
  Ok(#(state, supervisor))
}

// nolint: stringly_typed_error -- feeds the actor start contract
fn select_control_subject(
  name: Option(Name(event, child_data)),
) -> Result(Subject(Control(event, child_data)), String) {
  case name {
    None -> Ok(process.new_subject())
    Some(name) ->
      case process.register(process.self(), name.name) {
        Ok(Nil) -> Ok(process.named_subject(name.name))
        Error(Nil) -> Error("name is already registered")
      }
  }
}

fn make_inlet(
  send: fn(Control(event, child_data)) -> Nil,
  owner: fn() -> Result(Pid, Nil),
) -> Inlet(event) {
  sluice.make_inlet(fn(control) { send(Stage(control)) }, owner)
}

// nolint: stringly_typed_error -- feeds the actor start contract
fn establish_declared(
  state: State(event, child_data),
  subscriptions: List(SubscriptionOptions(event)),
) -> Result(State(event, child_data), String) {
  case subscriptions {
    [] -> Ok(state)
    [options, ..remaining] -> {
      let sluice.OptionsFields(
        producer:,
        min_demand:,
        max_demand:,
        cancel:,
        metadata:,
        mode:,
      ) = sluice.options_fields(options)
      use <- bool.guard(
        when: mode == sluice.Manual,
        return: Error("consumer supervisors do not accept manual demand"),
      )
      use #(core, selector, subject, _handle) <- result.try(
        consumer_core.add_subscription(
          state.core,
          state.selector,
          producer,
          min_demand:,
          max_demand:,
          cancel:,
          mode: sluice.Manual,
          metadata:,
          reply: None,
          tag_message: FromUpstream,
          tag_down: ProducerDown,
          make_cancel: cancel_closure(state.control_subject),
          make_ask: ask_closure(state.control_subject),
        )
        |> result.map_error(fn(error) {
          // nolint: string_inspect -- a typed subscription error is useful init context
          "could not subscribe: " <> string.inspect(error)
        }),
      )
      let core =
        consumer_core.request_demand(core:, subject:, demand: max_demand)
      let runtime =
        add_demand(
          runtime: state.runtime,
          subject:,
          min: min_demand,
          max: max_demand,
        )
      establish_declared(State(..state, core:, selector:, runtime:), remaining)
    }
  }
}

fn cancel_closure(
  control_subject: Subject(Control(event, child_data)),
) -> fn(Subject(ConsumerMessage(event))) -> fn() -> Nil {
  fn(subject) {
    fn() {
      process.send(control_subject, Stage(sluice.CancelSubscription(subject)))
    }
  }
}

fn ask_closure(
  control_subject: Subject(Control(event, child_data)),
) -> fn(Subject(ConsumerMessage(event))) -> fn(Int) -> Nil {
  fn(subject) {
    fn(demand) {
      process.send(
        control_subject,
        Stage(sluice.RequestDemand(subject, demand)),
      )
    }
  }
}

fn handle_actor_message(
  state: State(event, child_data),
  message: Control(event, child_data),
) -> actor.Next(State(event, child_data), Control(event, child_data)) {
  case handle(state, message) {
    Continue(state) ->
      actor.continue(state) |> actor.with_selector(state.selector)
    Stop(state, reason) -> {
      shutdown_children(state.runtime.jobs, state.runtime.shutdown_timeout)
      stop_with(reason)
    }
  }
}

fn handle(
  state: State(event, child_data),
  message: Control(event, child_data),
) -> Next(event, child_data) {
  case message {
    Stage(control) -> handle_stage(state, control)
    FromUpstream(subject, protocol.NewEvents(events)) ->
      start_events(state:, subject:, events:)
    FromUpstream(subject, protocol.Cancelled(reason)) ->
      subscription_closed(state:, subject:, reason:)
    ProducerDown(subject, down) ->
      case down {
        process.ProcessDown(_, _, reason) ->
          subscription_closed(
            state:,
            subject:,
            reason: exit_reason.classify(reason),
          )
        process.PortDown(..) -> Continue(state)
      }
    LinkedExit(process.ExitMessage(pid, reason)) ->
      case pid == state.parent {
        True -> Stop(state, reason)
        False -> child_exited(state:, pid:, reason:)
      }
    RetryRestart(job_id) -> retry_restart(state, job_id)
    TerminateChild(pid, reply) -> terminate_job(state:, pid:, reply:)
    TerminateTimedOut(job_id, pid) -> {
      case dict.get(state.runtime.jobs, job_id) {
        Ok(Job(pid: Some(current), terminating: Some(_), ..))
          if current == pid
        -> process.kill(pid)
        _ -> Nil
      }
      Continue(state)
    }
    WhichChildren(reply) -> {
      process.send(reply, visible_children(state.runtime.jobs))
      Continue(state)
    }
    CountChildren(reply) -> {
      process.send(reply, child_counts(state.runtime.jobs))
      Continue(state)
    }
  }
}

fn handle_stage(
  state: State(event, child_data),
  control: sluice.ConsumerControl(event),
) -> Next(event, child_data) {
  case control {
    sluice.SubscribeTo(_, _, _, _, _, sluice.Manual, reply) -> {
      process.send(reply, Error(sluice.UnsupportedDemandMode))
      Continue(state)
    }
    sluice.SubscribeTo(
      producer,
      min_demand,
      max_demand,
      cancel,
      metadata,
      sluice.Automatic,
      reply,
    ) ->
      case
        consumer_core.add_subscription(
          state.core,
          state.selector,
          producer,
          min_demand:,
          max_demand:,
          cancel:,
          mode: sluice.Manual,
          metadata:,
          reply: Some(reply),
          tag_message: FromUpstream,
          tag_down: ProducerDown,
          make_cancel: cancel_closure(state.control_subject),
          make_ask: ask_closure(state.control_subject),
        )
      {
        Error(error) -> {
          process.send(reply, Error(error))
          Continue(state)
        }
        Ok(#(core, selector, subject, handle)) -> {
          process.send(reply, Ok(handle))
          let pending =
            dict.insert(
              state.runtime.pending,
              reply,
              Pending(subject, min_demand, max_demand),
            )
          let runtime = Runtime(..state.runtime, pending:)
          Continue(State(..state, core:, selector:, runtime:))
        }
      }
    sluice.CancelSubscription(subject) -> {
      let core = consumer_core.request_cancel(state.core, subject)
      Continue(State(..state, core:))
    }
    // External asks could exceed the configured child limit. The supervisor
    // is the sole owner of this demand loop, so it deliberately ignores them.
    sluice.RequestDemand(_, _) -> Continue(state)
    sluice.ConfirmSubscribe(reply, confirmed, deadline) ->
      confirm_subscription(state:, reply:, confirmed:, deadline:)
    sluice.AbandonSubscribe(reply) -> {
      let #(core, selector) =
        consumer_core.abandon(state.core, state.selector, reply)
      let runtime = delete_pending(state.runtime, reply)
      Continue(State(..state, core:, selector:, runtime:))
    }
  }
}

fn confirm_subscription(
  state state: State(event, child_data),
  reply reply: Subject(Result(sluice.Subscription, sluice.SubscribeError)),
  confirmed confirmed: Subject(Nil),
  deadline deadline: Int,
) -> Next(event, child_data) {
  let state = case platform.monotonic_milliseconds() < deadline {
    True -> activate_subscription(state, reply)
    False -> abandon_subscription(state, reply)
  }
  process.send(confirmed, Nil)
  Continue(state)
}

fn activate_subscription(
  state: State(event, child_data),
  reply: Subject(Result(sluice.Subscription, sluice.SubscribeError)),
) -> State(event, child_data) {
  let #(core, activated) = consumer_core.confirm(state.core, reply)
  case activated, dict.get(state.runtime.pending, reply) {
    Some(#(subject, _handle)), Ok(Pending(_, min, max)) -> {
      let core = consumer_core.request_demand(core, subject, max)
      let runtime =
        state.runtime
        |> add_demand(subject, min, max)
        |> delete_pending(reply)
      State(..state, core:, runtime:)
    }
    _, _ -> State(..state, core:, runtime: delete_pending(state.runtime, reply))
  }
}

fn abandon_subscription(
  state: State(event, child_data),
  reply: Subject(Result(sluice.Subscription, sluice.SubscribeError)),
) -> State(event, child_data) {
  let #(core, selector) =
    consumer_core.abandon(state.core, state.selector, reply)
  State(
    ..state,
    core:,
    selector:,
    runtime: delete_pending(state.runtime, reply),
  )
}

fn start_events(
  state state: State(event, child_data),
  subject subject: Subject(ConsumerMessage(event)),
  events events: List(event),
) -> Next(event, child_data) {
  let #(core, runtime, _outcome) =
    consumer_core.deliver(
      state.core,
      subject,
      events,
      state.runtime,
      fn(runtime, batch, _subscription) {
        let runtime =
          list.fold(batch, runtime, fn(runtime, event) {
            let #(runtime, _started) =
              start_job(runtime:, event:, subscription: subject)
            runtime
          })
        consumer_core.BatchContinue(runtime)
      },
      fn(_runtime) { True },
    )
  let #(core, runtime) = flush_demand(core:, runtime:, subject:)
  Continue(State(..state, core:, runtime:))
}

fn start_job(
  runtime runtime: Runtime(event, child_data),
  event event: event,
  subscription subscription: Subject(ConsumerMessage(event)),
) -> #(Runtime(event, child_data), actor.StartResult(child_data)) {
  let id = runtime.next_job_id
  case call_child_start(runtime, event) {
    Ok(started) -> {
      let job =
        Job(
          id:,
          event:,
          subscription:,
          pid: Some(started.pid),
          terminating: None,
        )
      let runtime =
        Runtime(
          ..runtime,
          next_job_id: id + 1,
          jobs: dict.insert(runtime.jobs, id, job),
        )
      #(runtime, Ok(started))
    }
    Error(error) -> {
      platform.log_warning(
        "sluice: consumer supervisor failed to start child: "
        <> string.inspect(error),
      )
      let runtime =
        Runtime(..release(runtime, subscription), next_job_id: id + 1)
      #(runtime, Error(error))
    }
  }
}

fn child_exited(
  state state: State(event, child_data),
  pid pid: Pid,
  reason reason: ExitReason,
) -> Next(event, child_data) {
  case find_job_by_pid(state.runtime.jobs, pid) {
    Error(Nil) -> Continue(state)
    Ok(job) ->
      case job.terminating {
        Some(reply) -> {
          process.send(reply, Ok(Nil))
          finish_job(state, job)
        }
        None ->
          case should_restart(state.runtime.restart, reason) {
            False -> finish_job(state, job)
            True -> restart_job(state, Job(..job, pid: None))
          }
      }
  }
}

fn finish_job(
  state: State(event, child_data),
  job: Job(event),
) -> Next(event, child_data) {
  let runtime =
    state.runtime
    |> delete_job(job.id)
    |> release(job.subscription)
  let #(core, runtime) =
    flush_demand(core: state.core, runtime:, subject: job.subscription)
  Continue(State(..state, core:, runtime:))
}

fn restart_job(
  state: State(event, child_data),
  job: Job(event),
) -> Next(event, child_data) {
  let #(decisions, allowed) =
    supervisor_core.restart_allowed(
      state.runtime.decisions,
      platform.monotonic_milliseconds(),
    )
  case allowed {
    False ->
      Stop(
        State(..state, runtime: Runtime(..state.runtime, decisions:)),
        process.Abnormal(unsafe_reason("reached maximum restart intensity")),
      )
    True -> {
      let runtime =
        Runtime(
          ..state.runtime,
          decisions:,
          jobs: dict.insert(state.runtime.jobs, job.id, job),
        )
      case call_child_start(runtime, job.event) {
        Ok(started) -> {
          let job = Job(..job, pid: Some(started.pid))
          Continue(
            State(
              ..state,
              runtime: Runtime(
                ..runtime,
                jobs: dict.insert(runtime.jobs, job.id, job),
              ),
            ),
          )
        }
        Error(error) -> {
          platform.log_warning(
            "sluice: consumer supervisor failed to restart child: "
            <> string.inspect(error),
          )
          process.send(state.control_subject, RetryRestart(job.id))
          Continue(State(..state, runtime:))
        }
      }
    }
  }
}

fn retry_restart(
  state: State(event, child_data),
  job_id: Int,
) -> Next(event, child_data) {
  case dict.get(state.runtime.jobs, job_id) {
    Ok(Job(pid: None, terminating: None, ..) as job) -> restart_job(state, job)
    _ -> Continue(state)
  }
}

fn call_child_start(
  runtime: Runtime(event, child_data),
  event: event,
) -> actor.StartResult(child_data) {
  case platform.try_call(fn() { runtime.start_child(event) }) {
    Ok(result) -> result
    Error(reason) -> Error(actor.InitExited(process.Abnormal(reason)))
  }
}

fn terminate_job(
  state state: State(event, child_data),
  pid pid: Pid,
  reply reply: Subject(Result(Nil, TerminateError)),
) -> Next(event, child_data) {
  case find_job_by_pid(state.runtime.jobs, pid) {
    Error(Nil) -> {
      process.send(reply, Error(ChildNotFound))
      Continue(state)
    }
    Ok(job) -> {
      let job = Job(..job, terminating: Some(reply))
      let jobs = dict.insert(state.runtime.jobs, job.id, job)
      process.send_abnormal_exit(pid, atom.create("shutdown"))
      process.send_after(
        state.control_subject,
        state.runtime.shutdown_timeout,
        TerminateTimedOut(job.id, pid),
      )
      Continue(State(..state, runtime: Runtime(..state.runtime, jobs:)))
    }
  }
}

fn subscription_closed(
  state state: State(event, child_data),
  subject subject: Subject(ConsumerMessage(event)),
  reason reason: protocol.CancelReason,
) -> Next(event, child_data) {
  let #(core, selector, outcome) =
    consumer_core.closed(state.core, state.selector, subject, reason)
  let runtime =
    Runtime(
      ..state.runtime,
      decisions: supervisor_core.unsubscribed(state.runtime.decisions, subject),
    )
  let state = State(..state, core:, selector:, runtime:)
  case outcome {
    consumer_core.KeepRunning -> Continue(state)
    consumer_core.StopNormal -> Stop(state, process.Normal)
    consumer_core.StopAbnormal(reason) ->
      Stop(state, process.Abnormal(unsafe_reason(reason)))
  }
}

fn add_demand(
  runtime runtime: Runtime(event, child_data),
  subject subject: Subject(ConsumerMessage(event)),
  min min: Int,
  max max: Int,
) -> Runtime(event, child_data) {
  Runtime(
    ..runtime,
    decisions: supervisor_core.subscribed(runtime.decisions, subject, min, max),
  )
}

fn release(
  runtime: Runtime(event, child_data),
  subscription: Subject(ConsumerMessage(event)),
) -> Runtime(event, child_data) {
  Runtime(
    ..runtime,
    decisions: supervisor_core.released(runtime.decisions, subscription),
  )
}

fn flush_demand(
  core core: consumer_core.Core(event),
  runtime runtime: Runtime(event, child_data),
  subject subject: Subject(ConsumerMessage(event)),
) -> #(consumer_core.Core(event), Runtime(event, child_data)) {
  let #(decisions, ask) = supervisor_core.take_ask(runtime.decisions, subject)
  let runtime = Runtime(..runtime, decisions:)
  case ask {
    None -> #(core, runtime)
    Some(amount) -> #(
      consumer_core.request_demand(core, subject, amount),
      runtime,
    )
  }
}

fn delete_pending(
  runtime: Runtime(event, child_data),
  reply: Subject(Result(sluice.Subscription, sluice.SubscribeError)),
) -> Runtime(event, child_data) {
  Runtime(..runtime, pending: dict.delete(runtime.pending, reply))
}

fn delete_job(
  runtime: Runtime(event, child_data),
  job_id: Int,
) -> Runtime(event, child_data) {
  Runtime(..runtime, jobs: dict.delete(runtime.jobs, job_id))
}

fn find_job_by_pid(
  jobs: Dict(Int, Job(event)),
  pid: Pid,
) -> Result(Job(event), Nil) {
  dict.fold(jobs, Error(Nil), fn(found, _id, job) {
    case job.pid == Some(pid) {
      True -> Ok(job)
      False -> found
    }
  })
}

fn should_restart(policy: Restart, reason: ExitReason) -> Bool {
  case policy, exit_reason.classify(reason) {
    Temporary, _ -> False
    Transient, protocol.Abnormal(_) -> True
    Transient, _ -> False
  }
}

fn visible_children(jobs: Dict(Int, Job(event))) -> List(Child) {
  jobs
  |> dict.values()
  |> list.map(fn(job) {
    let status = case job.pid, job.terminating {
      Some(pid), Some(_) -> Terminating(pid)
      Some(pid), None -> Running(pid)
      None, _ -> Restarting
    }
    Child(job.id, status)
  })
}

fn child_counts(jobs: Dict(Int, Job(event))) -> ChildrenCount {
  let #(active, restarting) =
    dict.fold(jobs, #(0, 0), fn(counts, _id, job) {
      case job.pid {
        Some(_) -> #(counts.0 + 1, counts.1)
        None -> #(counts.0, counts.1 + 1)
      }
    })
  ChildrenCount(active:, restarting:, total: active + restarting)
}

fn shutdown_children(jobs: Dict(Int, Job(event)), timeout: Int) -> Nil {
  let pids =
    jobs
    |> dict.values()
    |> list.filter_map(fn(job) {
      case job.pid {
        Some(pid) ->
          case process.is_alive(pid) {
            True -> Ok(pid)
            False -> Error(Nil)
          }
        None -> Error(Nil)
      }
    })
  let shutdown = atom.create("shutdown")
  list.each(pids, fn(pid) { process.send_abnormal_exit(pid, shutdown) })
  wait_for_children(pids, platform.monotonic_milliseconds() + timeout)
}

fn wait_for_children(pids: List(Pid), deadline: Int) -> Nil {
  case pids {
    [] -> Nil
    _ -> {
      let remaining = int.max(deadline - platform.monotonic_milliseconds(), 0)
      let selector =
        process.new_selector()
        |> process.select_trapped_exits(fn(exit) { exit })
      case process.selector_receive(selector, remaining) {
        Ok(process.ExitMessage(pid, _reason)) ->
          wait_for_children(
            list.filter(pids, fn(child) { child != pid }),
            deadline,
          )
        Error(Nil) -> list.each(pids, process.kill)
      }
    }
  }
}

fn stop_with(
  reason: ExitReason,
) -> actor.Next(State(event, child_data), Control(event, child_data)) {
  case reason {
    process.Normal -> actor.stop()
    process.Killed -> {
      process.kill(process.self())
      actor.stop()
    }
    process.Abnormal(reason) -> {
      process.send_abnormal_exit(process.self(), reason)
      actor.stop()
    }
  }
}

@external(erlang, "gleam_otp_external", "identity")
fn unsafe_reason(reason: String) -> Dynamic
