open Eio.Std

let () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_level (Some Debug);
  Logs.set_reporter (Logs_fmt.reporter ())

let run (fn : net:Eio.Net.t -> Switch.t -> unit) =
  Eio_gcd.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  Switch.run (fn ~net)

(* let addr = `Tcp (Unix.inet_addr_of_string "::1", int_of_string @@ Sys.argv.(1)) *)
let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, int_of_string @@ Sys.argv.(1))

let read_all flow =
  let b = Buffer.create 100 in
  Eio.Flow.copy flow (Eio.Flow.buffer_sink b);
  Buffer.contents b

exception Graceful_shutdown

let run_client ~sw ~net ~addr =
  traceln "Connecting to server...";
  let flow = Eio.Net.connect ~sw net addr in
  Eio.Flow.copy_string "Hello from client" flow;
  Eio.Flow.shutdown flow `Send;
  let msg = read_all flow in
  traceln "Client received: %S" msg

let run_server ~sw socket =
  traceln "Starting up the server...";
  while true do
    Eio.Net.accept_sub socket ~sw (fun ~sw:_ flow _addr ->
      traceln "Server accepted connection from client";
      Fun.protect (fun () ->
        let msg = read_all flow in
        traceln "Server received: %S" msg
      ) ~finally:(fun () -> Eio.Flow.copy_string "Bye" flow);
    )
    ~on_error:(function
      | Graceful_shutdown -> ()
      | ex -> traceln "Error handling connection: %s" (Printexc.to_string ex)
    )
  done

let test_address addr ~net sw =
  let server = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  Fibre.both
    (fun () -> run_server ~sw server)
    (fun () ->
      run_client ~sw ~net ~addr;
      traceln "Client finished - cancelling server";
      raise Graceful_shutdown
    )

let () = run (test_address addr)