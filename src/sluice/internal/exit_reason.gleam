//// This module changes raw process exit reasons into the `CancelReason`
//// of the protocol. Thus the cancel modes can know the difference between
//// an intentional shutdown and a failure.

import gleam/dynamic.{type Dynamic}
import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/process.{type ExitReason}
import gleam/result
import gleam/string
import sluice/internal/protocol.{type CancelReason}

pub fn classify(reason: ExitReason) -> CancelReason {
  case reason {
    process.Normal -> protocol.Normal
    process.Killed -> protocol.Abnormal("killed")
    process.Abnormal(details) -> classify_abnormal(details)
  }
}

fn classify_abnormal(details: Dynamic) -> CancelReason {
  case is_shutdown(details) {
    True -> protocol.Shutdown
    False ->
      // nolint: string_inspect -- an exit reason is an unknown foreign term, and inspection is the only correct text output
      protocol.Abnormal(string.inspect(details))
  }
}

// A supervisor shutdown is the atom `shutdown` or a `{shutdown, term}`
// tuple.
fn is_shutdown(details: Dynamic) -> Bool {
  decode.run(details, atom.decoder())
  |> result.or(decode.run(details, decode.at([0], atom.decoder())))
  |> result.map(fn(value) { atom.to_string(value) == "shutdown" })
  |> result.unwrap(False)
}
