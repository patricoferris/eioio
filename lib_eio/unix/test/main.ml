open Eio

let () =
  Logs.(set_level ~all:true (Some Debug));
  Logs.set_reporter @@ Logs.format_reporter ();
  Printexc.record_backtrace true

let main stdout =
  Flow.copy_string "Hello world\n" stdout

let () =
  Eio_unix.Backend.run @@ fun env ->
  main env#stdout