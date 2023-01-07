open Eio

let () =
  let main =
    Eio_browser.run @@ fun () ->
    Fiber.both
      (fun () -> Eio_browser.Timeout.sleep ~ms:1000; traceln "World")
      (fun () -> traceln "Hello, ");
    "Done"
  in
  Fut.await main (fun v -> Brr.Console.log [ Jstr.v v ])