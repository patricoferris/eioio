let run_iocp fn =
  Logs.info (fun f -> f "Selecting iocp backend");
  Eio_iocp.run (fun env -> fn (env :> Eio.Stdenv.t))

let run_luv fn =
  Eio_luv.run (fun env -> fn (env :> Eio.Stdenv.t))

let run fn =
  match Sys.getenv_opt "EIO_BACKEND" with
  | Some "iocp" -> run_iocp fn
  | _ ->
    Logs.info (fun f -> f "Using luv backend");
    run_luv fn
