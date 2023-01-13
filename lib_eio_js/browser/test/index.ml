open Eio

let () =
  let main =
    Eio_browser.run @@ fun () ->
    Fiber.both
      (fun () -> Eio_browser.Timeout.sleep ~ms:1000; traceln "World")
      (fun () -> traceln "Hello, ");
    let p1, r1 = Fut.create () in
    let p2, r2 = Fut.create () in
    Fiber.both
      (fun () -> Eio_browser.await p1; traceln "Waited for p1"; r2 ())
      (fun () -> r1 (); Eio_browser.await p2; traceln "Waited for p2");
      "Done"
  in
  Fut.await main (fun v -> Brr.Console.log [ Jstr.v v ])