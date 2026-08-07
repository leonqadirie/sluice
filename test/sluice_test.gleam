import gleeunit

@external(erlang, "sluice_test_ffi", "silence_supervisor_reports")
fn silence_supervisor_reports() -> Nil

pub fn main() -> Nil {
  silence_supervisor_reports()
  gleeunit.main()
}
