//// Functions from the Erlang platform that the dependencies do not give.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/process.{type Name}

/// Send a message to a registered name. If the name has no process, or if
/// the process stops at the same time, the message is lost without an
/// error. This is the same result as a message to a dead pid.
@external(erlang, "sluice_ffi", "send_named")
pub fn send_named(name: Name(message), message: message) -> Nil

/// Write a warning to the logger.
@external(erlang, "sluice_ffi", "log_warning")
pub fn log_warning(message: String) -> Nil

/// Run a function and report the success. A failure inside the function
/// does not stop the caller.
@external(erlang, "sluice_ffi", "safely")
pub fn safely(run: fn() -> Nil) -> Bool

/// Run a function and return a caught exception or exit as dynamic data.
@external(erlang, "sluice_ffi", "try_call")
pub fn try_call(run: fn() -> value) -> Result(value, Dynamic)

/// A monotonic clock for timeout deadlines, in milliseconds.
@external(erlang, "sluice_ffi", "monotonic_milliseconds")
pub fn monotonic_milliseconds() -> Int
