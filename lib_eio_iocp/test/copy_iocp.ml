let () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_level (Some Info);
  Logs.set_reporter (Logs_fmt.reporter ())

let () =
  Eio_iocp.run @@ fun env ->
  let fs = Eio.Stdenv.fs env in
  Eio.Dir.with_open_in fs "./README.md" @@ fun src ->
  Eio.Dir.with_open_out ~create:(`Exclusive 0o600) fs "./README2.md" @@ fun dst ->
  Eio.Flow.copy src dst