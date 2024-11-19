let run ~fallback fn = Eio_posix.run ~fallback (fun env -> fn (env :> Eio_unix.Stdenv.base))
