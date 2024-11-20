let ( / ) = Eio.Path.( / )

let () =
  Eio_main.run @@ fun env ->
  Eio.Path.chown ~follow:false ~uid:501L ~gid:12L (env#cwd / "README.md") 
