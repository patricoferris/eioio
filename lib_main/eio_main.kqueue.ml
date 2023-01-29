let run_luv fn = Eio_luv.run (fun env -> fn (env :> Eio.Stdenv.t))
let run_kqueue fn = Eio_kqueue.run (fun env -> fn (env :> Eio.Stdenv.t))

let run fn =
  match Sys.getenv_opt "EIO_BACKEND" with
  | Some "kqueue" ->
    run_kqueue fn
  | _ -> run_luv fn