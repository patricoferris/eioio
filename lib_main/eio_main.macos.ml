let run_gcd fn =
  Logs.info (fun f -> f "Selecting GCD backend");
  Eio_gcd.run (fun env -> fn (env :> Eio.Stdenv.t))

let run_luv fn =
  Logs.info (fun f -> f "Selecting luv backend for macOS");
  Eio_luv.run (fun env -> fn (env :> Eio.Stdenv.t))

let run fn =
  match Sys.getenv_opt "EIO_MACOS_BACKEND" with
  | Some "gcd" -> run_gcd fn
  | _ -> run_luv fn
