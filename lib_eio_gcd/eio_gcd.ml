(*
 * Copyright (C) 2021 Thomas Leonard
 * Copyright (C) 2021 Patrick Ferris
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

 let src = Logs.Src.create "eio_gcd" ~doc:"Eio backend using gcd and luv"
 module Log = (val Logs.src_log src : Logs.LOG)

 open Eio.Std

 (* SIGPIPE makes no sense in a modern application. *)
 let () = Sys.(set_signal sigpipe Signal_ignore)

 module Buffer = struct
  type t = Dispatch.Data.t ref
  let empty () = ref @@ Dispatch.Data.empty ()

  (* let set buff buff' = buff := buff' *)

  let concat buff buff' = buff := Dispatch.Data.concat !buff buff'

  let of_bigarray buf = ref @@ Dispatch.Data.create buf

  let to_string buff =
    let buff = !buff in
    Cstruct.(to_string @@ of_bigarray @@ (Dispatch.Data.to_buff ~offset:0 (Dispatch.Data.size buff) buff))
 end

 module File = struct
   type t = {
     mutable release_hook : Eio.Hook.t;        (* Use this on close to remove switch's [on_release] hook. *)
     mutable fd : [`Open of Dispatch.Io.t | `Closed]
   }

   let get op = function
     | { fd = `Open fd; _ } -> fd
     | { fd = `Closed ; _ } -> invalid_arg (op ^ ": file descriptor used after calling close!")

   let is_open = function
     | { fd = `Open _; _ } -> true
     | { fd = `Closed; _ } -> false

   let close t =
     Ctf.label "close";
     let fd = get "close" t in
     t.fd <- `Closed;
     Eio.Hook.remove t.release_hook;
     Dispatch.Io.close fd

   let ensure_closed t =
     if is_open t then close t

   let to_gcd = get "to_gcd"

   let of_gcd_no_hook fd =
     { fd = `Open fd; release_hook = Eio.Hook.null }

   let of_gcd ~sw fd =
     let t = of_gcd_no_hook fd in
     t.release_hook <- Switch.on_release_cancellable sw (fun () -> ensure_closed t);
     t

    (* Dispatch Queue
     Ideally, I think, this would be [Concurrent] but then the OCaml
     runtime lock would block the thread and cause GCD to "thread explode".

     BUG: Something in ocaml-dispatch needs to change with the retention/allocation of
     queues, if this `lazy` is removed then this is initialised when the module is and
     sometimes causes segfaults *)
   let io_queue = lazy Dispatch.Queue.(create ~typ:Serial ())

   let realpath = Eio_luv.File.realpath

   let mkdir = Eio_luv.File.mkdir

   (* A few notes:
        - Creating a channel doesn't open the file until you try and do something with it (e.g. a write)
        - [Dispatch.Io.create_with_path] needs an absolute path hence the [realpath] mangling... *)
   let open_ ~sw ?mode path flags =
    let create path =
        try
          let io_channel = Dispatch.Io.create_with_path ~path ~flags ~mode:(Option.value ~default:0 mode) Stream (Lazy.force io_queue) in
          Ok (of_gcd ~sw io_channel)
        with
        | Invalid_argument m -> Error (`Msg m)
    in
   match Filename.is_relative path with
   | false -> create path
   | true -> match realpath Filename.current_dir_name with
     | Ok parent -> create Filename.(concat parent path)
     | Error e -> Error (`Msg (Luv.Error.strerror e))

   (* A read can call the callback multiple times with more and more chunks of
     data -- here we concatenate them and only once the read is finished reading
     do we enqueue thread. *)
    let read ~off ~length fd (buf : Buffer.t) =
      Log.debug (fun f -> f "gcd read");
      let read = ref 0 in
      Eio_luv.enter (fun t k ->
        Dispatch.Io.with_read ~off ~length ~queue:(Lazy.force io_queue) (get "with_read" fd) ~f:(fun ~err ~finished r ->
          let size = Dispatch.Data.size r in
          if err = 0 && finished then begin
            if size = 0 then Eio_luv.enqueue_thread t k !read
            else (
              read := !read + size;
              buf := Dispatch.Data.concat r !buf;
              Eio_luv.enqueue_thread t k !read
            )
          end
          else if err <> 0 then failwith "err"
          else begin
            read := !read + size;
            buf := Dispatch.Data.concat r !buf
          end
        )
      )

   let write fd (bufs : Buffer.t) =
    Eio_luv.enter (fun t k ->
        Dispatch.Io.with_write ~off:0 ~data:(!bufs) ~queue:(Lazy.force io_queue) (get "with_write" fd) ~f:(fun ~err:_ ~finished _remaining ->
            if finished then Eio_luv.enqueue_thread t k ()
            else ()
          )
      )

  let fast_copy src dst =
    let data = Buffer.empty () in
    try
      while true do
        let _got = read ~off:0 ~length:max_int src data in
        write dst data
      done
    with End_of_file -> ()
 end

 module Conn = struct
    let receive (conn : Network.Connection.t) buf =
      let r = Eio_luv.enter (fun t k ->
        Network.Connection.receive ~min:0 ~max:max_int conn ~completion:(fun data _context is_complete err ->
          match data with
            | None -> Eio_luv.enqueue_thread t k (Ok (0, true))
            | Some data ->
              let err_code = Network.Error.to_int err in
              let res =
                if err_code = 0 then begin
                  let size = Dispatch.Data.size data in
                  Buffer.concat buf data;
                  Ok (size, is_complete)
                  end else Error (`Msg (string_of_int err_code))
                in
              Eio_luv.enqueue_thread t k res
        )
      ) in match r with
      | Ok (_, true) -> raise End_of_file
      | Ok (got, false) ->
        Logs.debug (fun f -> f "GOT %i" got);
        got
      | Error (`Msg e) ->
        failwith ("Connection receive failed with " ^ e)

    let send (conn : Network.Connection.t) buf =
      Eio_luv.enter (fun t k ->
        Network.Connection.send ~is_complete:true ~context:Final conn ~data:(!buf) ~completion:(fun e ->
          match Network.Error.to_int e with
          | 0 -> Eio_luv.enqueue_thread t k (Ok ())
          | i -> Eio_luv.enqueue_thread t k (Error (`Msg (string_of_int i)))
        )
      )

 end

 let socket sock = object
  inherit Eio.Flow.two_way

  (* Lots of copying :/ *)
  method read_into buff =
    let data = Buffer.empty () in
    let res = Conn.receive sock data in
    let cs = Cstruct.of_bigarray @@ Dispatch.Data.to_buff ~offset:0 res !data in
    Cstruct.blit cs 0 buff 0 res;
    match res with
    | 0 -> raise End_of_file
    | got -> got

  method read_methods = []

  method write src =
    let buf = Cstruct.create 4096 in
    try
      while true do
        let got = Eio.Flow.read_into src buf in
        let data = Buffer.of_bigarray @@ Cstruct.(to_bigarray (sub buf 0 got)) in
        match Conn.send sock data with
          | Ok () -> ()
          | Error (`Msg m) -> failwith m
      done
    with End_of_file -> ()

  method close =
    Network.Connection.cancel sock

   method shutdown = function
     | `Send -> (
      (* Deep in a connection.h file it says:
          "In order to close a connection on the sending side (a "write close"), send
          a context that is marked as "final" and mark is_complete. The convenience definition
          NW_CONNECTION_FINAL_MESSAGE_CONTEXT may be used to define the default final context
          for a connection."

          TODO: I think the data argument can also be NULL, so could save the allocation here I think. *)
      let r = Eio_luv.enter (fun t k ->
        Network.Connection.send ~is_complete:true ~context:Final sock ~data:(Dispatch.Data.empty ()) ~completion:(fun e ->
          match Network.Error.to_int e with
          | 0 ->
            Logs.debug (fun f -> f "Sending 'write close' to connection");
            Eio_luv.enqueue_thread t k (Ok ())
          | i ->
            Eio_luv.enqueue_thread t k (Error (`Msg (string_of_int i)))
        )
      ) in match r with
          | Ok () -> ()
          | Error (`Msg m) -> failwith m
     )
     | `Receive -> failwith "shutdown receive not supported"
     | `All ->
       Log.warn (fun f -> f "shutdown receive not supported")
 end


let net_queue = lazy (Dispatch.Queue.create ~typ:Serial ())

class virtual ['a] listening_socket ~backlog:_ sock = object (self)
  inherit Eio.Net.listening_socket

  method private virtual get_endpoint_addr : Network.Endpoint.t -> Eio.Net.Sockaddr.t

  val connected = Eio.Semaphore.make 0

  val mutable conn_sock = None;

  val mutable accept_params = None

  method close =
    Network.Listener.cancel sock

  method accept_sub ~sw ~on_error fn =
    Eio.Semaphore.acquire connected;
    Fibre.fork_sub_ignore ~sw ~on_error
      (fun sw ->
        Logs.debug (fun f -> f "Doinging stugffg");
        let (conn, sockaddr) = Option.get conn_sock in
        fn ~sw (socket conn) sockaddr
      )
      ~on_release:(fun () -> ())

  initializer
    let handler (state : Network.Listener.State.t) err =
      match (Network.Error.to_int err, state) with
        | i, _ when i <> 0 -> failwith ("Listener failed with error code: " ^ string_of_int i)
        | _, Ready -> Log.debug (fun f -> f "Listening on port %i..." (Network.Listener.get_port sock));
        | _, Failed -> failwith "Network listener failed"
        | _, Invalid -> Logs.debug (fun f -> f "Listener changed to invalid state")
        | _, Waiting ->Logs.debug (fun f -> f "Listener is waiting...")
        | _, Cancelled -> Logs.debug (fun f -> f "Listener is cancelled")
  in
   let conn_handler conn =
    Eio.Semaphore.release connected;
    Network.Connection.retain conn;
    Network.Connection.set_queue ~queue:(Lazy.force net_queue) conn;
    Network.Connection.start conn;
    let endpoint = Network.Connection.copy_endpoint conn in
    let sockaddr = self#get_endpoint_addr endpoint in
    conn_sock <- Some (conn, sockaddr)
    in
      Logs.debug (fun f -> f "Initialising socket");
      Network.Listener.set_state_changed_handler ~handler sock;
      Network.Listener.set_new_connection_handler ~handler:conn_handler sock;
      Network.Listener.start sock


end
  let listening_socket ~backlog sock = object
    inherit [[ `TCP ]] listening_socket ~backlog sock

    method private get_endpoint_addr e =
      match Option.get @@ Network.Endpoint.get_address e with
        | Unix.ADDR_UNIX path         -> `Unix path
        | Unix.ADDR_INET (host, port) -> `Tcp (host, port)
  end

   let net = object
     inherit Eio.Net.t

     method listen ~reuse_addr ~backlog ~sw:_ = function
       | `Tcp (hostname, port) ->
         let open Network in
         let params = Parameters.create_tcp () in
         let endpoint = Endpoint.create_address Unix.(ADDR_INET (hostname, port)) in
         let _ =
          Parameters.set_reuse_local_address params reuse_addr;
          Parameters.set_local_endpoint ~endpoint params
         in
         let _ = Endpoint.release endpoint in
         let listener = Listener.create params in
         Listener.set_queue ~queue:(Lazy.force net_queue) listener;
         Listener.retain listener;
         listening_socket ~backlog listener
       | _ -> assert false

     method connect ~sw:_ = function
       | `Tcp (hostname, port) ->
          let open Network in
          let params = Parameters.create_tcp () in
          let endpoint = Endpoint.create_address Unix.(ADDR_INET (hostname, port)) in
          let _ =
            Parameters.set_reuse_local_address params true;
            Parameters.set_local_endpoint ~endpoint params
          in
          let _ = Endpoint.release endpoint in
          let connection = Connection.create ~params endpoint in
          Connection.retain connection;
          Connection.set_queue ~queue:(Lazy.force net_queue) connection;
          let handler t k (state : Network.Connection.State.t) _ =
            match state with
            | Waiting -> Logs.debug (fun f -> f "Connection is waiting...")
            | Ready ->
              Logs.debug (fun f -> f "Connection is ready");
              Eio_luv.enqueue_thread t k (socket connection)
            | Invalid -> Logs.warn (fun f -> f "Invalid connection")
            | Preparing -> Logs.debug (fun f -> f "Connection is being prepared")
            | Failed ->
              Logs.warn (fun f -> f "Connection failed");
              failwith "Connection Failed"
            | Cancelled -> Logs.debug (fun f -> f "Connection has been cancelled")
          in
          Eio_luv.enter (fun t k ->
            Connection.set_state_changed_handler ~handler:(handler t k) connection;
            Connection.start connection)
       | _ -> assert false
   end

module Objects = struct
  type _ Eio.Generic.ty += FD : File.t Eio.Generic.ty

  type has_fd = < fd : File.t >
  type source = < Eio.Flow.source; Eio.Flow.close; has_fd >
  type sink   = < Eio.Flow.sink  ; Eio.Flow.close; has_fd >

  let get_fd (t : <has_fd; ..>) = t#fd

  let get_fd_opt t = Eio.Generic.probe t FD

  let flow fd = object (_ : <source; sink; ..>)
    method fd = fd
    method close = File.close fd

    method probe : type a. a Eio.Generic.ty -> a option = function
      | FD -> Some fd
      | _ -> None

    method read_into buff =
      let data = Buffer.empty () in
      let res = File.read ~off:(buff.Cstruct.off) ~length:(buff.len) fd data in
      let cs = Cstruct.of_bigarray @@ Dispatch.Data.to_buff ~offset:0 res !data in
      Cstruct.blit cs 0 buff 0 res;
      match res with
      | 0 -> raise End_of_file
      | got -> got

    method read_methods = []

    method write src =
      match Eio.Generic.probe src FD with
      | Some src -> File.fast_copy src fd
      | None ->
        let chunk = Cstruct.create 1024 in
        try
          while true do
            let _got = Eio.Flow.read_into src chunk in
            File.write fd (ref @@ Dispatch.Data.create (Cstruct.to_bigarray chunk))
          done
        with End_of_file -> ()
  end

  let source fd = (flow fd :> source)
  let sink   fd = (flow fd :> sink)

  type stdenv = <
    stdin  : source;
    stdout : sink;
    stderr : sink;
    net : Eio.Net.t;
    domain_mgr : Eio.Domain_manager.t;
    clock : Eio.Time.clock;
    fs : Eio.Dir.t;
    cwd : Eio.Dir.t;
  >

  let stdenv () =
    let stdin = lazy (source (File.of_gcd_no_hook @@ Dispatch.Io.(create Stream Fd.stdin (Lazy.force File.io_queue)))) in
    let stdout = lazy (sink (File.of_gcd_no_hook @@ Dispatch.Io.(create Stream Fd.stdout (Lazy.force File.io_queue)))) in
    (* TODO: Add stderr to Dispatch *)
    (* let _stderr = lazy (sink (File.of_luv_no_hook Luv.File.stderr)) in *)
    object (_ : stdenv)
      method stdin  = (Lazy.force stdin)
      method stdout = (Lazy.force stdout)
      method stderr = failwith "unimplemented"
      method net = net
      method domain_mgr = failwith "unimplemented"
      method clock  = failwith "unimplemented"
      method fs = failwith "unimplemented"
      method cwd = failwith "unimplemented"
    end
end

let gcd_run (fn : Objects.stdenv -> unit) env =
  fn env

let run fn =
  let env = Objects.stdenv () in
  try
    gcd_run (fun env -> fn (env :> Eio.Stdenv.t)) env
  with
  | _e ->
    let st = Eio_luv.default_t () in
    Eio_luv.handler st (fun env -> fn (env :> Eio.Stdenv.t)) (env :> Eio.Stdenv.t)
