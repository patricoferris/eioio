(*
 * Copyright (C) 2022 Patrick Ferris
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

let src = Logs.Src.create "eio_iocp" ~doc:"Effect-based IO system for Windows/IOCP"
module Log = (val Logs.src_log src : Logs.LOG)

open Eio.Std
open Eio.Private.Effect
open Eio.Private.Effect.Deep

module Fiber_context = Eio.Private.Fiber_context
module Ctf = Eio.Private.Ctf

module Suspended = Eio_utils.Suspended
module Zzz = Eio_utils.Zzz
module Lf_queue = Eio_utils.Lf_queue

type source = < Eio.Flow.source; Eio.Flow.close >
type sink   = < Eio.Flow.sink  ; Eio.Flow.close >

type io_job =
  | Job : ([`R | `W | `C | `A] * int Suspended.t) -> io_job

type runnable =
  | Thread : 'a Suspended.t * 'a -> runnable
  | Failed_thread : 'a Suspended.t * exn -> runnable

module FD = struct
  type t = {
    seekable : bool;
    close_unix : bool;                          (* Whether closing this also closes the underlying FD. *)
    mutable release_hook : Eio.Switch.hook;     (* Use this on close to remove switch's [on_release] hook. *)
    mutable fd : [`Open of Iocp.Handle.t | `Closed];
    mutable off : Optint.Int63.t; (* HMMMMMM.... *)
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
    Eio.Switch.remove_hook t.release_hook;
    if t.close_unix then (
      let () = Eio_unix.run_in_systhread (fun () -> Unix.close (fd :> Unix.file_descr)) in
      Log.debug (fun l -> l "close: woken up");
    )

  let ensure_closed t =
    if is_open t then close t

  let to_unix op t =
    let fd = get "to_unix" t in
    match op with
    | `Peek -> (fd :> Unix.file_descr)
    | `Take ->
      t.fd <- `Closed;
      Eio.Switch.remove_hook t.release_hook;
      (fd :> Unix.file_descr)

  let of_unix_no_hook ~seekable ~close_unix fd =
    { seekable; close_unix; fd = `Open fd; release_hook = Eio.Switch.null_hook; off = Optint.Int63.zero }

  let of_unix ~sw ~seekable ~close_unix fd =
    let t = of_unix_no_hook ~seekable ~close_unix fd in
    t.release_hook <- Switch.on_release_cancellable sw (fun () -> ensure_closed t);
    t

  let placeholder ~seekable ~close_unix =
    { seekable; close_unix; fd = `Closed; release_hook = Eio.Switch.null_hook; off = Optint.Int63.zero }

  let pp f t =
    match t.fd with
    | `Open fd -> Fmt.pf f "%d" (Obj.magic fd : int)
    | `Closed -> Fmt.string f "(closed)"
end

type eventfd = { read : FD.t; write : FD.t }

type t = {
  run_q : runnable Lf_queue.t;
  iocp : io_job Iocp.t;
  mutable jobs : int;
  io_q: (t -> unit) Queue.t; 
  (* When adding to [run_q] from another domain, this domain may be sleeping and so won't see the event.
     In that case, [need_wakeup = true] and you must signal using [eventfd]. You must hold [eventfd_mutex]
     when writing to or closing [eventfd]. *)
  eventfd : eventfd;
  eventfd_mutex : Mutex.t;
  need_wakeup : bool Atomic.t;
}

type _ Effect.t += Enter : (t -> 'a Suspended.t -> unit) -> 'a Effect.t
let enter fn = perform (Enter fn)

let wake_buffer =
  let b = Bytes.create 8 in
  Bytes.set_int64_ne b 0 1L;
  b

let wakeup t =
  Mutex.lock t.eventfd_mutex;
  match
    Log.debug (fun f -> f "Sending wakeup on eventfd %a" FD.pp t.eventfd.write);
    Atomic.set t.need_wakeup false; (* [t] will check [run_q] after getting the event below *)
    let sent = Unix.single_write (FD.get "wakeup" t.eventfd.write :> Unix.file_descr) wake_buffer 0 8 in
    assert (sent = 8)
  with
  | ()           -> Mutex.unlock t.eventfd_mutex
  | exception ex -> Mutex.unlock t.eventfd_mutex; raise ex

let cancel ol fd =
  try Iocp.Handle.cancel (FD.get "cancel" fd) ol with Invalid_argument _ -> ()

let enqueue_thread st k x =
  Logs.debug (fun f -> f "Enqueue thread");
  Lf_queue.push st.run_q (Thread (k, x));
  if Atomic.get st.need_wakeup then wakeup st

let enqueue_failed_thread st k ex =
  Logs.debug (fun f -> f "Enqueue failed thread");
  Lf_queue.push st.run_q (Failed_thread (k, ex));
  if Atomic.get st.need_wakeup then wakeup st

let with_cancel_hook ~action st fd ol fn =
  match Fiber_context.get_error action.Suspended.fiber with
  | Some ex -> enqueue_failed_thread st action ex; false
  | None ->
    match fn () with
    | None -> true
    | Some _job ->
      Fiber_context.set_cancel_fn action.fiber (fun _ -> cancel ol fd);
      false

let rec enqueue_read args st action =
  let (file_offset,fd,buf,len) = args in
  Ctf.label "read";
  let ol = Iocp.Overlapped.create ~off:file_offset () in
  let retry = with_cancel_hook ~action st fd ol (fun () ->
      Logs.debug (fun f -> f "READ JOBS %i" st.jobs);
      st.jobs <- st.jobs + 1;
      Iocp.read st.iocp ~file_offset ~off:0 ~len (FD.get "read" fd) buf (Job (`R, action)) ol)
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_read args st action) st.io_q

let rec enqueue_write args st action =
  let (file_offset,fd,buf,len) = args in
  Ctf.label "write";
  let ol = Iocp.Overlapped.create ~off:file_offset () in
  let retry = with_cancel_hook ~action st fd ol (fun () ->
      Logs.debug (fun f -> f "WRITE JOBS %i" st.jobs);
      st.jobs <- st.jobs + 1;
      Iocp.write st.iocp ~file_offset ~off:0 ~len (FD.get "write" fd) buf (Job (`W, action)) ol
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_write args st action) st.io_q

let rec enqueue_connect fd addr st action =
  Log.debug (fun l -> l "connect: submitting call");
  Ctf.label "connect";
  let ol = Iocp.Overlapped.create () in
  let retry = with_cancel_hook ~action st fd ol (fun () ->
    Logs.debug (fun f -> f "CONNECT JOBS %i" st.jobs);
      st.jobs <- st.jobs + 1;
      let handle = FD.get "connect" fd in
      Unix.bind (handle :> Unix.file_descr) (ADDR_INET (Unix.inet_addr_any, 0));
      Iocp.connect st.iocp handle addr (Job (`C, action)) ol
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_connect fd addr st action) st.io_q

let rec enqueue_accept fd sock addr_buf st action =
  Log.debug (fun l -> l "accept: submitting call");
  Ctf.label "accept";
  let ol = Iocp.Overlapped.create () in
  let retry = with_cancel_hook ~action st fd ol (fun () ->
      Logs.debug (fun f -> f "ACCEPT JOBS %i" st.jobs);
      (* st.jobs <- st.jobs + 1; *)
      Logs.debug (fun f -> f "JOBS %i" st.jobs);
      Iocp.accept st.iocp (FD.get "accept" fd) sock addr_buf (Job (`A, action)) ol
    ) in
  if retry then (
    (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_accept fd sock addr_buf st action) st.io_q
  )

let rec enqueue_send sock bufs st action =
  Log.debug (fun l -> l "send: submitting call");
  Ctf.label "send";
  let ol = Iocp.Overlapped.create () in
  let retry = with_cancel_hook ~action st sock ol (fun () ->
      Logs.debug (fun f -> f "SEND JOBS %i" st.jobs);
      st.jobs <- st.jobs + 1;
      let handle = FD.get "send" sock in
      Iocp.send st.iocp handle bufs (Job (`W, action)) ol
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_send sock bufs st action) st.io_q

let rec enqueue_recv sock bufs st action =
  Log.debug (fun l -> l "recv: submitting call");
  Ctf.label "recv";
  let ol = Iocp.Overlapped.create () in
  let retry = with_cancel_hook ~action st sock ol (fun () ->
      Logs.debug (fun f -> f "RECV JOBS %i" st.jobs);
      st.jobs <- st.jobs + 1;
      let handle = FD.get "recv" sock in
      Iocp.recv st.iocp handle bufs (Job (`R, action)) ol
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_recv sock bufs st action) st.io_q

module Low_level = struct
  let read ?file_offset fd buf len =
    let fo = match file_offset with
      | Some x -> x
      | None -> fd.FD.off
    in
    Log.debug (fun f -> f "read: entering read");
    let res = enter (enqueue_read (fo, fd, buf, len)) in
    Log.debug (fun l -> l "readv: woken up after read %i" res);
    if res < 0 then (
      raise (Unix.Unix_error (EUNKNOWNERR res, "read", ""))
    ) else if res = 0 then (
      raise End_of_file
    ) else (
      if file_offset = None then fd.off <- Optint.Int63.(add fd.off (of_int res));
      res
    )

  let write ?file_offset fd buf len =
    let fo = match file_offset with
      | Some x -> x
      | None -> fd.FD.off
    in
    let res = enter (enqueue_write (fo, fd, buf, len)) in
    Log.debug (fun l -> l "writev: woken up after write");
    if res < 0 then (
      raise (Unix.Unix_error (EUNKNOWNERR res, "writev", ""))
    ) else (
      fd.off <- Optint.Int63.(add fd.off (of_int res))
    )
  
  let connect fd addr =
    Log.debug (fun f -> f "Connect");
    let res = enter (enqueue_connect fd addr) in
    Log.debug (fun l -> l "connect returned");
    if res < 0 then (
      raise (Unix.Unix_error (EUNKNOWNERR res, "connect", ""))
    )

  let accept ~sw fd sock =
    Ctf.label "accept";
    let addr_buf = Iocp.Accept_buffer.create () in
    let res = enter (enqueue_accept fd sock addr_buf) in
    Log.debug (fun l -> l "accept returned");
    if res < 0 then (
      raise (Unix.Unix_error (EUNKNOWNERR res, "accept", ""))
    ) else (
      let client = FD.of_unix ~sw ~seekable:false ~close_unix:true sock in
      let client_addr = Iocp.Accept_buffer.get_remote addr_buf sock in
      client, (Iocp.Sockaddr.get client_addr)
    )

  let send sock bufs =
    Log.debug (fun f -> f "Send");
    let res = enter (enqueue_send sock bufs) in
    Log.debug (fun l -> l "send returned");
    if res < 0 then (
      raise (Unix.Unix_error (EUNKNOWNERR res, "send", ""))
    )

  let recv sock bufs =
    Log.debug (fun f -> f "Receive");
    let res = enter (enqueue_recv sock bufs) in
    Log.debug (fun l -> l "receive returned");
    if res < 0 then (
      raise (Unix.Unix_error (EUNKNOWNERR res, "receive", ""))
    ) else if res = 0 then (
      raise End_of_file
    ) else res
end

type _ Eio.Generic.ty += FD : FD.t Eio.Generic.ty [@@ocaml.warning "-38"]

let flow fd =
  object (_ : <source; sink; ..>)
    method fd = fd
    method close = FD.close fd

    method probe : type a. a Eio.Generic.ty -> a option = function
      | FD -> Some fd
      | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd)
      | _ -> None

    method read_into buf =
      Low_level.read fd buf (Cstruct.length buf)

    method read_methods = []

    method copy src =
      let buf = Cstruct.create 4096 in
      try
        while true do
          let got = Eio.Flow.read src buf in
          let sub = Cstruct.sub buf 0 got in
          Low_level.write fd sub got
        done
      with End_of_file -> traceln "END OF FILE!"

    method shutdown cmd =
      traceln "Calling shutdown...";
      let () = Unix.shutdown (FD.get "shutdown" fd :> Unix.file_descr) @@ match cmd with
      | `Receive -> Unix.SHUTDOWN_RECEIVE
      | `Send -> Unix.SHUTDOWN_SEND
      | `All -> Unix.SHUTDOWN_ALL in traceln "SHUTDOWN!!!!"
  end

  let socket_flow fd =
    object (_ : <source; sink; ..>)
      method fd = fd
      method close = FD.close fd
  
      method probe : type a. a Eio.Generic.ty -> a option = function
        | FD -> Some fd
        | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd)
        | _ -> None
  
      method read_into buf =
        let i = Low_level.recv fd [ buf ] in
        Logs.debug (fun f -> f "Receive got %i, %s" i (Cstruct.to_string buf));
        i
  
      method read_methods = []
  
      method copy src =
        let buf = Cstruct.create 4096 in
        try
          while true do
            let got = Eio.Flow.read src buf in
            let sub = Cstruct.sub buf 0 got in
            Low_level.send fd [ sub ]
          done
        with End_of_file -> ()
  
      method shutdown cmd =
        let fd = (FD.get "shutdown" fd) in
        traceln "About to shutdown...";
        let () = Iocp.Handle.shutdown fd @@ match cmd with
        | `Receive -> Unix.SHUTDOWN_RECEIVE
        | `Send -> Unix.SHUTDOWN_SEND
        | `All -> Unix.SHUTDOWN_ALL in traceln "Shutdown sent..."
    end

  (* Windows stdout, stdin and stderr don't have IOCP-style asynchronous functions. *)
  let stdflow fd =
    object (_ : <source; sink; ..>)
    (* method fd = Switch.run (fun sw -> FD.of_unix ~sw ~seekable:false ~close_unix:false fd) *)
    method close = Unix.close fd
    method read_into buf =
      (* Sad times ... *)
      let bytes = Bytes.create (Cstruct.length buf) in
      let r = Eio_unix.run_in_systhread (fun () -> Unix.read (fd :> Unix.file_descr) bytes 0 (Cstruct.length buf)) in
      let _ = Cstruct.of_bytes ~allocator:(fun _ -> buf) bytes in
      r

    method probe : type a. a Eio.Generic.ty -> a option = function
      (* | FD -> Some (fd :> Unix.file_descr)
      | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd) *)
      | _ -> None

    method read_methods = []

    method copy src =
      let buf = Cstruct.create 4096 in
      let fd = (fd :> Unix.file_descr) in
      try
        while true do
          let got = Eio.Flow.read src buf in
          let bytes = Cstruct.to_bytes buf in
          let _r = Eio_unix.run_in_systhread (fun () -> Unix.single_write fd bytes 0 got) in
          ()
        done
      with End_of_file -> ()

    method shutdown cmd =
      Unix.shutdown (fd :> Unix.file_descr) @@ match cmd with
      | `Receive -> Unix.SHUTDOWN_RECEIVE
      | `Send -> Unix.SHUTDOWN_SEND
      | `All -> Unix.SHUTDOWN_ALL
  end

class dir fd = object
  inherit Eio.Dir.t

  method open_in ~sw path =
    let fd = Eio_unix.run_in_systhread (fun () -> Iocp.Handle.openfile path [ O_RDONLY ] 0) in
    (flow (FD.of_unix ~sw ~seekable:true ~close_unix:true fd) :> <Eio.Flow.source; Eio.Flow.close>)

  method open_out ~sw ~append:_ ~create path =
    let perm, flags =
      let open Unix in
      match create with
      | `Never            -> 0,    []
      | `If_missing  perm -> perm, [ O_WRONLY; O_CREAT ]
      | `Or_truncate perm -> perm, [ O_WRONLY; O_CREAT; O_TRUNC ]
      | `Exclusive   perm -> perm, [ O_WRONLY; O_CREAT; O_EXCL ]
    in
    let fd = Eio_unix.run_in_systhread (fun () -> Iocp.Handle.openfile path flags perm) in
    (flow (FD.of_unix ~sw ~seekable:true ~close_unix:true fd) :> <Eio.Dir.rw; Eio.Flow.close>)

  method open_dir ~sw path =
    let fd = Eio_unix.run_in_systhread (fun () -> Iocp.Handle.openfile path [ O_RDONLY ] 0) in
    (new dir (Some (FD.of_unix ~sw ~seekable:true ~close_unix:true fd)) :> <Eio.Dir.t; Eio.Flow.close>)

  method mkdir ~perm:_ _path = ()

  method read_dir path = Sys.readdir path |> Array.to_list

  method close =
    FD.close (Option.get fd)
end

let fs = object
  inherit dir None
end

let listening_socket fd = object
  inherit Eio.Net.listening_socket

  method! probe : type a. a Eio.Generic.ty -> a option = function
    | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd)
    | _ -> None

  method close = FD.close fd

  method accept ~sw =
    Switch.check sw;
    let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    let handle = Iocp.Handle.of_unix sock in
    let client, client_addr = Low_level.accept ~sw fd handle in
    Iocp.Handle.update_accept_ctx handle;
    let client_addr = match client_addr with
      | Unix.ADDR_UNIX path         -> `Unix path
      | Unix.ADDR_INET (host, port) -> `Tcp (Eio_unix.Ipaddr.of_unix host, port)
    in
    let flow = (socket_flow client :> <Eio.Flow.two_way; Eio.Flow.close>) in
    flow, client_addr
end

let net = object
  inherit Eio.Net.t

  method listen ~reuse_addr ~reuse_port ~backlog ~sw listen_addr =
    let socket_domain, socket_type, addr =
      match listen_addr with
      | `Unix path         ->
        if reuse_addr then (
          match Unix.lstat path with
          | Unix.{ st_kind = S_SOCK; _ } -> Unix.unlink path
          | _ -> ()
          | exception Unix.Unix_error (Unix.ENOENT, _, _) -> ()
        );
        Unix.PF_UNIX, Unix.SOCK_STREAM, Unix.ADDR_UNIX path
      | `Tcp (host, port)  ->
        let host = Eio_unix.Ipaddr.to_unix host in
        Unix.PF_INET, Unix.SOCK_STREAM, Unix.ADDR_INET (host, port)
    in
    let sock_unix = Unix.socket socket_domain socket_type 0 in
    (* For Unix domain sockets, remove the path when done (except for abstract sockets). *)
    begin match listen_addr with
      | `Unix path ->
        if String.length path > 0 && path.[0] <> Char.chr 0 then
          Switch.on_release sw (fun () -> Unix.unlink path)
      | `Tcp _ -> ()
    end;
    if reuse_addr then
      Unix.setsockopt sock_unix Unix.SO_REUSEADDR true;
    if reuse_port then
      Unix.setsockopt sock_unix Unix.SO_REUSEPORT true;
    Unix.bind sock_unix addr;
    Unix.listen sock_unix backlog;
    let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true (Iocp.Handle.of_unix sock_unix) in
    listening_socket sock

  method connect ~sw addr =
    let socket_domain, socket_type, addr =
      match addr with
      | `Unix path         -> Unix.PF_UNIX, Unix.SOCK_STREAM, Unix.ADDR_UNIX path
      | `Tcp (host, port)  ->
        let host = Eio_unix.Ipaddr.to_unix host in
        Unix.PF_INET, Unix.SOCK_STREAM, Unix.ADDR_INET (host, port)
    in
    let sock_unix = Unix.socket socket_domain socket_type 0 in
    let handle = Iocp.Handle.of_unix sock_unix in
    let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true handle in
    Low_level.connect sock addr;
    (* This allows shutdown to work, see https://docs.microsoft.com/en-us/windows/win32/winsock/sol-socket-socket-options?redirectedfrom=MSDN *)
    Iocp.Handle.update_connect_ctx handle;
    (socket_flow sock :> <Eio.Flow.two_way; Eio.Flow.close>)

  method datagram_socket ~sw = function
    | `Udp (host, port) ->
      let host = Eio_unix.Ipaddr.to_unix host in
      let addr = Unix.ADDR_INET (host, port) in
      let sock_unix = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
      Unix.setsockopt sock_unix Unix.SO_REUSEADDR true;
      Unix.setsockopt sock_unix Unix.SO_REUSEPORT true;
      let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true (Iocp.Handle.of_unix sock_unix) in
      Unix.bind sock_unix addr;
      (* udp_socket sock *)
      Obj.magic sock
end

type stdenv = <
  stdin  : source;
  stdout : sink;
  stderr : sink;
  net : Eio.Net.t;
  domain_mgr : Eio.Domain_manager.t;
  clock : Eio.Time.clock;
  fs : Eio.Dir.t;
  cwd : Eio.Dir.t;
  secure_random : Eio.Flow.source;
>

let stdenv () =
  let stdin = lazy  ((stdflow Unix.stdin) :> source) in 
  let stdout = lazy ((stdflow Unix.stdout) :> sink) in 
  let stderr = lazy ((stdflow Unix.stderr) :> sink) in 
  let cwd = new dir None in
  object (_ : stdenv)
    method stdin  = Lazy.force stdin
    method stdout = Lazy.force stdout
    method stderr = Lazy.force stderr
    method net = net
    method domain_mgr = Obj.magic ()
    method clock = Obj.magic ()
    method fs = (fs :> Eio.Dir.t)
    method cwd = (cwd :> Eio.Dir.t)
    method secure_random = Obj.magic ()
  end

(* Can only be called from our own domain, so no need to check for wakeup. *)
let enqueue_at_head st k x =
  Logs.debug (fun f -> f "Enqueue at head");
  Lf_queue.push_head st.run_q (Thread (k, x))

(* Resume the next runnable fiber, if any. *)
let rec schedule t =
  Logs.debug (fun f -> f "Scheduling...");
  match Lf_queue.pop t.run_q with
  | Some Thread (k, v) ->
    Logs.debug (fun f -> f "Resume thread");
    Suspended.continue k v               (* We already have a runnable task *)
  | Some Failed_thread (k, ex) -> 
    Logs.debug (fun f -> f "Resume failed thread");
    Suspended.discontinue k ex
  | None ->
    (* Logs.debug (fun f -> f "Peeking into the queue");
    match Iocp.peek t.iocp with
    | Some { data = runnable; bytes_transferred } ->
      Logs.debug (fun f -> f "Got completion from port after peeking!");
      handle_complete t ~runnable bytes_transferred
    | None -> *)
    Logs.debug (fun f -> f "Jobs left: %i" t.jobs);
    if not (Lf_queue.is_empty t.run_q) then (
      schedule t
    ) else if t.jobs = 0 then (
      Lf_queue.close t.run_q;      (* Just to catch bugs if something tries to enqueue later *)
      `Exit_scheduler
    ) else (
      Atomic.set t.need_wakeup true;
      if Lf_queue.is_empty t.run_q then (
        Logs.debug (fun f -> f "Waiting on completion port...");
        let result = Iocp.get_queued_completion_status t.iocp in
        Atomic.set t.need_wakeup false;
        match result with
          | None ->
            (* Woken by a timeout, which is now due, or by a signal. *)
            Logs.debug (fun f -> f "None returned from completion port");
            schedule t
          | Some { data = runnable; bytes_transferred } ->
            Logs.debug (fun f -> f "Got completion from port!");
            handle_complete t ~runnable bytes_transferred
      ) else (
        Log.debug (fun f -> f "Runq is not empty");
        Atomic.set t.need_wakeup false;
        schedule t
      )
    )
  and handle_complete st ~runnable result =
    Logs.debug (fun f -> f "Handle completion, jobs left: %i" st.jobs);
    st.jobs <- st.jobs - 1;
    match runnable with
    | Job (kind, k) ->
      (* clear_cancel k; *)
      Logs.debug (fun f -> f "Kind %s" (match kind with `R -> "READ" | `W -> "WRITE" | `C -> "CONNECT" | `A -> "ACCEPT"));
      begin match Fiber_context.get_error k.fiber with
        | None ->
          Logs.debug (fun f -> f "Result %i" result);
          Suspended.continue k result
        | Some e ->
          (* If cancelled, report that instead.
             Should we only do this on error, to avoid losing the return value?
             We already do that with rw jobs. *)
          Suspended.discontinue k e
      end

let monitor_event_fd t =
  let buf = Cstruct.create 8 in
  while true do
    Log.debug (fun f -> f "Waiting for wakeup on eventfd %a" FD.pp t.eventfd.read);
    let got = Low_level.read t.eventfd.read buf 8 in
    Log.debug (fun f -> f "Received wakeup on eventfd %a" FD.pp t.eventfd.read);
    assert (got = 8)
  done

(* Run [main] in an Eio main loop. *)
let run main =
  let run_q = Lf_queue.create () in
  let io_q = Queue.create () in
  let eventfd = { read = FD.placeholder ~seekable:false ~close_unix:false; write = FD.placeholder ~seekable:false ~close_unix:false } in
  let iocp = Iocp.create () in
  let eventfd_mutex = Mutex.create () in
  let stdenv = stdenv () in
  let st = { run_q; iocp; need_wakeup = Atomic.make false; io_q; eventfd; eventfd_mutex; jobs = 0 } in
  let rec fork ~new_fiber:fiber fn =
    let open Eio.Private.Effect in
    (* Create a new fiber and run [fn] in it. *)
    Deep.match_with fn ()
      { retc = (fun () -> Fiber_context.destroy fiber; schedule st);
        exnc = (fun ex -> Fiber_context.destroy fiber; raise ex);
        effc = fun (type a) (e : a Effect.t) ->
          match e with
          | Enter fn -> Some (fun k ->
            match Fiber_context.get_error fiber with
            | Some e -> discontinue k e
            | None ->
              let k = { Suspended.k; fiber } in
              fn st k;
              schedule st)
          | Eio.Private.Effects.Suspend f -> Some (fun k ->
            (* st.jobs <- st.jobs + 1; *)
            let k = { Suspended.k; fiber } in
            f fiber (function
                | Ok v -> enqueue_thread st k v
                | Error ex -> enqueue_failed_thread st k ex
              );
            schedule st
          )
          | Eio.Private.Effects.Fork (new_fiber, f) -> Some (fun k ->
            (* st.jobs <- st.jobs + 1; *)
            let k = { Suspended.k; fiber } in
            enqueue_at_head st k ();
            fork ~new_fiber f
          )
          | Eio.Private.Effects.Get_context -> Some (fun k ->
              Deep.continue k fiber
            )
          | Eio.Private.Effects.Trace -> Some (fun k ->
              Deep.continue k Eio_utils.Trace.default_traceln
            )
          | _ -> None
      }
  in
  let `Exit_scheduler =
    let new_fiber = Fiber_context.make_root () in
    fork ~new_fiber (fun () ->
      Switch.run_protected (fun sw ->
        let rfd, wfd = Iocp.Handle.pipe "eioIocpPipe" in
        st.eventfd.read.fd <- `Open rfd;
        st.eventfd.write.fd <- `Open wfd;
        Switch.on_release sw (fun () ->
            Mutex.lock st.eventfd_mutex;
            FD.close st.eventfd.read;
            FD.close st.eventfd.write;
            Mutex.unlock st.eventfd_mutex;
            Unix.close (rfd :> Unix.file_descr);
            Unix.close (wfd :> Unix.file_descr)
          );
        Log.debug (fun f -> f "Monitoring eventfd %a" FD.pp st.eventfd.read);
        Fiber.first
          (fun () -> main stdenv)
          (fun () -> monitor_event_fd st)
      )
    )
  in
  Log.debug (fun l -> l "exit")
