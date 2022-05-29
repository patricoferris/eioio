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
  | Job_no_cancel : int Suspended.t -> io_job
  | Job : int Suspended.t -> io_job

type runnable =
  | Thread : 'a Suspended.t * 'a -> runnable
  | Failed_thread : 'a Suspended.t * exn -> runnable

type _ Effect.t += Close : Iocp.Handle.t -> int Effect.t

module FD = struct
  type t = {
    seekable : bool;
    close_unix : bool;                          (* Whether closing this also closes the underlying FD. *)
    mutable release_hook : Eio.Switch.hook;     (* Use this on close to remove switch's [on_release] hook. *)
    mutable fd : [`Open of Iocp.Handle.t | `Closed]
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
      let res = perform (Close fd) in
      Log.debug (fun l -> l "close: woken up");
      if res < 0 then
        raise (Unix.Unix_error (EUNKNOWNERR res, "close", string_of_int (Obj.magic fd : int)))
    )

  let ensure_closed t =
    if is_open t then close t

  let is_seekable fd =
    match Unix.lseek fd 0 Unix.SEEK_CUR with
    | (_ : int) -> true
    | exception Unix.Unix_error(Unix.ESPIPE, "lseek", "") -> false

  let to_unix op t =
    let fd = get "to_unix" t in
    match op with
    | `Peek -> (fd :> Unix.file_descr)
    | `Take ->
      t.fd <- `Closed;
      Eio.Switch.remove_hook t.release_hook;
      (fd :> Unix.file_descr)

  let of_unix_no_hook ~seekable ~close_unix fd =
    { seekable; close_unix; fd = `Open fd; release_hook = Eio.Switch.null_hook }

  let of_unix ~sw ~seekable ~close_unix fd =
    let t = of_unix_no_hook ~seekable ~close_unix fd in
    t.release_hook <- Switch.on_release_cancellable sw (fun () -> ensure_closed t);
    t

  let placeholder ~seekable ~close_unix =
    { seekable; close_unix; fd = `Closed; release_hook = Eio.Switch.null_hook }

  let uring_file_offset t =
    if t.seekable then Optint.Int63.minus_one else Optint.Int63.zero

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

let cancel _ = () (* TODO *)

let enqueue_thread st k x =
  Logs.debug (fun f -> f "Enqueue thread");
  Lf_queue.push st.run_q (Thread (k, x));
  wakeup st

let enqueue_failed_thread st k ex =
  Logs.debug (fun f -> f "Enqueue failed thread");
  Lf_queue.push st.run_q (Failed_thread (k, ex));
  if Atomic.get st.need_wakeup then wakeup st

let with_cancel_hook ~action st fn =
  match Fiber_context.get_error action.Suspended.fiber with
  | Some ex -> enqueue_failed_thread st action ex; false
  | None ->
    match fn () with
    | None -> true
    | Some job ->
      Fiber_context.set_cancel_fn action.fiber (fun _ -> cancel job);
      false

let rec enqueue_read args st action =
  let (file_offset,fd,buf,len) = args in
  let file_offset =
    match file_offset with
    | Some x -> x
    | None -> FD.uring_file_offset fd
  in
  Ctf.label "read";
  let retry = with_cancel_hook ~action st (fun () ->
      st.jobs <- st.jobs + 1;
      Iocp.read st.iocp ~file_offset ~off:0 ~len (FD.get "read" fd) buf (Job action))
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_read args st action) st.io_q

let rec enqueue_write args st action =
  let (file_offset,fd,buf,len) = args in
  let file_offset =
    match file_offset with
    | Some x -> x
    | None -> FD.uring_file_offset fd
  in
  Ctf.label "write";
  let retry = with_cancel_hook ~action st (fun () ->
      st.jobs <- st.jobs + 1;
      Iocp.write st.iocp ~file_offset ~off:0 ~len (FD.get "write" fd) buf (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_write args st action) st.io_q

module Low_level = struct
  let read ?file_offset fd buf len =
    let res = enter (enqueue_read (file_offset, fd, buf, len)) in
    Log.debug (fun l -> l "readv: woken up after read");
    if res < 0 then (
      raise (Unix.Unix_error (EUNKNOWNERR res, "read", ""))
    ) else if res = 0 then (
      raise End_of_file
    ) else (
      res
    )

  let write ?file_offset fd buf len =
    let res = enter (enqueue_write (file_offset, fd, buf, len)) in
    Log.debug (fun l -> l "writev: woken up after write");
    if res < 0 then (
      raise (Unix.Unix_error (EUNKNOWNERR res, "writev", ""))
    ) else (
      ()
    )
end

type _ Eio.Generic.ty += FD : FD.t Eio.Generic.ty

let flow fd =
  object (_ : <source; sink; ..>)
    method fd = fd
    method close = FD.close fd

    method probe : type a. a Eio.Generic.ty -> a option = function
      | FD -> Some fd
      | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd)
      | _ -> None

    method read_into buf =
      Low_level.read fd buf max_int

    method read_methods = []

    method copy src =
      let buf = Cstruct.create 4096 in
      try
        while true do
          let got = Eio.Flow.read src buf in
          let sub = Cstruct.sub buf 0 got in
          Low_level.write fd sub max_int
        done
      with End_of_file -> ()

    method shutdown cmd =
      Unix.shutdown (FD.get "shutdown" fd :> Unix.file_descr) @@ match cmd with
      | `Receive -> Unix.SHUTDOWN_RECEIVE
      | `Send -> Unix.SHUTDOWN_SEND
      | `All -> Unix.SHUTDOWN_ALL
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

(* class dir fd = object
  inherit Eio.Dir.t

  val resolve_flags = Uring.Resolve.beneath

  method open_in ~sw path =
    let fd = Low_level.openat2 ~sw ?dir:fd path
        ~access:`R
        ~flags:Uring.Open_flags.cloexec
        ~perm:0
        ~resolve:resolve_flags
    in
    (flow fd :> <Eio.Flow.source; Eio.Flow.close>)

  method open_out ~sw ~append ~create path =
    let perm, flags =
      match create with
      | `Never            -> 0,    Uring.Open_flags.empty
      | `If_missing  perm -> perm, Uring.Open_flags.creat
      | `Or_truncate perm -> perm, Uring.Open_flags.(creat + trunc)
      | `Exclusive   perm -> perm, Uring.Open_flags.(creat + excl)
    in
    let flags = if append then Uring.Open_flags.(flags + append) else flags in
    let fd = Low_level.openat2 ~sw ?dir:fd path
        ~access:`RW
        ~flags:Uring.Open_flags.(cloexec + flags)
        ~perm
        ~resolve:resolve_flags
    in
    (flow fd :> <Eio.Dir.rw; Eio.Flow.close>)

  method open_dir ~sw path =
    let fd = Low_level.openat2 ~sw ~seekable:false ?dir:fd path
        ~access:`R
        ~flags:Uring.Open_flags.(cloexec + path + directory)
        ~perm:0
        ~resolve:resolve_flags
    in
    (new dir (Some fd) :> <Eio.Dir.t; Eio.Flow.close>)

  method mkdir ~perm path =
    Low_level.mkdir_beneath ~perm ?dir:fd path

  method close =
    FD.close (Option.get fd)
end *)

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
  (* let cwd = new dir None in *)
  object (_ : stdenv)
    method stdin  = Lazy.force stdin
    method stdout = Lazy.force stdout
    method stderr = Lazy.force stderr
    method net = Obj.magic ()
    method domain_mgr = Obj.magic ()
    method clock = Obj.magic ()
    method fs = Obj.magic ()
    method cwd = Obj.magic ()
    method secure_random = Obj.magic ()
  end

(* Can only be called from our own domain, so no need to check for wakeup. *)
let enqueue_at_head st k x =
  Logs.debug (fun f -> f "Enqueue at head");
  Lf_queue.push_head st.run_q (Thread (k, x))

(* Resume the next runnable fiber, if any. *)
let rec schedule t =
  match Lf_queue.pop t.run_q with
  | Some Thread (k, v) -> 
    t.jobs <- t.jobs - 1;
    Suspended.continue k v               (* We already have a runnable task *)
  | Some Failed_thread (k, ex) -> 
    t.jobs <- t.jobs - 1;
    Suspended.discontinue k ex
  | None -> 
    if not (Lf_queue.is_empty t.run_q) then (
      schedule t
    ) else if t.jobs = 0 then (
      Lf_queue.close t.run_q;      (* Just to catch bugs if something tries to enqueue later *)
      `Exit_scheduler
    ) else (
      Logs.debug (fun f -> f "Nothing in runq, jobs left: %i" t.jobs);
      Atomic.set t.need_wakeup true;
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
    )
  and handle_complete st ~runnable result =
    Logs.debug (fun f -> f "Handle completion, jobs left: %i" st.jobs);
    st.jobs <- st.jobs - 1;
    match runnable with
    | Job k ->
      (* clear_cancel k; *)
      begin match Fiber_context.get_error k.fiber with
        | None -> Suspended.continue k result
        | Some e ->
          (* If cancelled, report that instead.
             Should we only do this on error, to avoid losing the return value?
             We already do that with rw jobs. *)
          Suspended.discontinue k e
      end
    | Job_no_cancel k ->
      Suspended.continue k result

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
            st.jobs <- st.jobs + 1;
            let k = { Suspended.k; fiber } in
            f fiber (function
                | Ok v -> enqueue_thread st k v
                | Error ex -> 
                  print_endline @@ Printexc.to_string ex;
                  enqueue_failed_thread st k ex
              );
            schedule st
          )
          | Eio.Private.Effects.Fork (new_fiber, f) -> Some (fun k ->
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
