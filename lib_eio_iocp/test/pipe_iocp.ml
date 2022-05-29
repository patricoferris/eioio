let () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_level (Some Info);
  Logs.set_reporter (Logs_fmt.reporter ())

let () =
  Eio_iocp.run @@ fun env ->
  Eio.Flow.copy_string "Write to stdin:\n" (Eio.Stdenv.stdout env);
  Eio.Flow.copy (Eio.Stdenv.stdin env) (Eio.Stdenv.stdout env)