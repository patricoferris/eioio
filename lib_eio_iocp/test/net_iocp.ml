open Eio

let () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_level (Some Debug);
  Logs.set_reporter (Logs_fmt.reporter ())

let read_all flow =
  let b = Buffer.create 100 in
  traceln "copying from flow...";
  Eio.Flow.copy flow (Eio.Flow.buffer_sink b);
  traceln "copy done...";
  Buffer.contents b

exception Graceful_shutdown

let run_client ~sw ~net ~addr =
  traceln "Connecting to server...";
  let flow = Eio.Net.connect ~sw net addr in
  traceln "Connected...";
  Eio.Flow.copy_string "Hello from client" flow;
  traceln "Message sent, shuting down...";
  Eio.Flow.shutdown flow `Send;
  let msg = read_all flow in
  traceln "Client received: %S" msg

let run_server ~sw socket =
  while true do
    traceln "Accepting on server...";
    Eio.Net.accept_sub socket ~sw (fun ~sw:_ flow _addr ->
      traceln "Server accepted connection from client";
      Fun.protect (fun () ->
        traceln "Reading from socket...";
        let msg = read_all flow in
        traceln "Server received: %S" msg
      ) ~finally:(fun () -> Eio.Flow.copy_string "Bye" flow)
    )
    ~on_error:(function
      | Graceful_shutdown -> traceln "Graceful shutdown"
      | ex -> traceln "Error handling connection: %s" (Printexc.to_string ex)
    );
  done

let test_address addr ~net sw =
  let server = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  Fiber.both
    (fun () -> run_server ~sw server)
    (fun () ->
      run_client ~sw ~net ~addr;
      traceln "Client finished - cancelling server";
      raise Graceful_shutdown
    )

let () =
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 8009) in
  Eio_iocp.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  Switch.run (test_address addr ~net)
