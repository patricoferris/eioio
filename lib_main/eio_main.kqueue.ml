let run_kqueue fn = Eio_kqueue.run (fun env -> fn (env :> Eio.Stdenv.t))

let run fn = run_kqueue fn