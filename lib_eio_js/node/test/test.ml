open Eio

let () =
  Eio_node.run @@ fun env ->
  Flow.copy_string "Hello from node.js" env#stdout