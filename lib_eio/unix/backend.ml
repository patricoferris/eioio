(*
 * Copyright (C) 2020-2022 Anil Madhavapeddy
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

(* A pure OCaml Unix backend only using non-blocking sockets
   and systhreads for reading and writing FDs. This is largely
   based on the eio_linux backend. *)

open Eio
open Eio_utils

let src =
  Logs.Src.create "eio_unix"
    ~doc:"Effect-based IO system using OCaml's Unix module"

module Log = (val Logs.src_log src : Logs.LOG)

type systhread = { kind : [ `Monitor | `Program of string ]; thread : Thread.t }

type 'a enqueue = ('a, exn) result -> unit

type _ Effect.t +=
  | Suspend_systhread :
      (Eio.Private.Fiber_context.t -> 'a enqueue -> systhread option)
      -> 'a Effect.t

let run_in_systhread ?(kind = `Program "default") fn =
  let f fiber enqueue =
    match Eio.Private.Fiber_context.get_error fiber with
    | Some err ->
        enqueue (Error err);
        None
    | None ->
        let thread : Thread.t =
          Thread.create
            (fun () -> enqueue (try Ok (fn ()) with exn -> Error exn))
            ()
        in
        Some { kind; thread }
  in
  Effect.perform (Suspend_systhread f)

module FD = struct
  type t = {
    seekable : bool;
    close_unix : bool; (* Whether closing this also closes the underlying FD. *)
    mutable release_hook : Eio.Switch.hook;
        (* Use this on close to remove switch's [on_release] hook. *)
    mutable fd : [ `Open of Unix.file_descr | `Closed ];
  }

  let get_exn op = function
    | { fd = `Open fd; _ } -> fd
    | { fd = `Closed; _ } ->
        invalid_arg (op ^ ": file descriptor used after calling close!")

  let get op = function
    | { fd = `Open fd; _ } -> Ok fd
    | { fd = `Closed; _ } ->
        Error
          (Invalid_argument (op ^ ": file descriptor used after calling close!"))

  let is_open = function
    | { fd = `Open _; _ } -> true
    | { fd = `Closed; _ } -> false

  let close t =
    Eio.Private.Ctf.label "close";
    let fd = get_exn "close" t in
    t.fd <- `Closed;
    Eio.Switch.remove_hook t.release_hook;
    if t.close_unix then run_in_systhread (fun () -> Unix.close fd)

  let ensure_closed t = if is_open t then close t

  let is_seekable fd =
    match Unix.lseek fd 0 Unix.SEEK_CUR with
    | (_ : int) -> true
    | exception Unix.Unix_error (Unix.ESPIPE, "lseek", "") -> false

  let to_unix op t =
    let fd = get_exn "to_unix" t in
    match op with
    | `Peek -> fd
    | `Take ->
        t.fd <- `Closed;
        Eio.Switch.remove_hook t.release_hook;
        fd

  let of_unix_no_hook ~seekable ~close_unix fd =
    { seekable; close_unix; fd = `Open fd; release_hook = Eio.Switch.null_hook }

  let of_unix ~sw ~seekable ~close_unix fd =
    let t = of_unix_no_hook ~seekable ~close_unix fd in
    t.release_hook <-
      Switch.on_release_cancellable sw (fun () -> ensure_closed t);
    t

  let placeholder ~seekable ~close_unix =
    { seekable; close_unix; fd = `Closed; release_hook = Eio.Switch.null_hook }

  let uring_file_offset t =
    if t.seekable then Optint.Int63.minus_one else Optint.Int63.zero

  let pp f t =
    match t.fd with
    | `Open fd -> Fmt.pf f "%d" (Obj.magic fd : int)
    | `Closed -> Fmt.string f "(closed)"

  let fstat t =
    (* todo: use uring  *)
    let ust = Unix.LargeFile.fstat (get_exn "fstat" t) in
    let st_kind : Eio.File.Stat.kind =
      match ust.st_kind with
      | Unix.S_REG -> `Regular_file
      | Unix.S_DIR -> `Directory
      | Unix.S_CHR -> `Character_special
      | Unix.S_BLK -> `Block_device
      | Unix.S_LNK -> `Symbolic_link
      | Unix.S_FIFO -> `Fifo
      | Unix.S_SOCK -> `Socket
    in
    Eio.File.Stat.
      {
        dev = ust.st_dev |> Int64.of_int;
        ino = ust.st_ino |> Int64.of_int;
        kind = st_kind;
        perm = ust.st_perm;
        nlink = ust.st_nlink |> Int64.of_int;
        uid = ust.st_uid |> Int64.of_int;
        gid = ust.st_gid |> Int64.of_int;
        rdev = ust.st_rdev |> Int64.of_int;
        size = ust.st_size |> Optint.Int63.of_int64;
        atime = ust.st_atime;
        mtime = ust.st_mtime;
        ctime = ust.st_ctime;
      }
end

type runnable =
  | IO : runnable
  | Thread : 'a Suspended.t * 'a -> runnable
  | Failed_thread : 'a Suspended.t * exn -> runnable

let pp_systhread_kind pf = function
  | `Monitor -> Fmt.string pf "monitor"
  | `Program s -> Fmt.pf pf "program: %s" s

type t = {
  (* The queue of runnable fibers ready to be resumed. Note: other domains can also add work items here. *)
  run_q : runnable Lf_queue.t;
  (* When adding to [run_q] from another domain, this domain may be sleeping and so won't see the event.
     In that case, [need_wakeup = true] and you must signal using [eventfd]. You must hold [eventfd_mutex]
     when writing to or closing [eventfd]. *)
  mutable eventfd : Eventfd.t option;
  eventfd_mutex : Mutex.t;
  (* If [false], the main thread will check [run_q] before sleeping again
     (possibly because an event has been or will be sent to [eventfd]).
     It can therefore be set to [false] in either of these cases:
     - By the receiving thread because it will check [run_q] before sleeping, or
     - By the sending thread because it will signal the main thread later *)
  need_wakeup : bool Atomic.t;
  mutable active_ops : int; (* Needs protecting ? *)
  sleep_q : Zzz.t;
  mutable threads : systhread list;
}

let wake_buffer =
  let b = Bytes.create 8 in
  Bytes.set_int64_ne b 0 1L;
  b

let get_eventfd msg t =
  match t.eventfd with
  | Some fd -> fd
  | None -> failwith ("Eventfd not set at " ^ msg)

let wakeup t =
  Mutex.lock t.eventfd_mutex;
  match
    Atomic.set t.need_wakeup false;
    (* [t] will check [run_q] after getting the event below *)
    Eventfd.send (get_eventfd "wakeup" t)
  with
  | () -> Mutex.unlock t.eventfd_mutex
  | exception ex ->
      Mutex.unlock t.eventfd_mutex;
      raise ex

let enqueue_thread st k x =
  Lf_queue.push st.run_q (Thread (k, x));
  if Atomic.get st.need_wakeup then wakeup st

let enqueue_failed_thread st k ex =
  Lf_queue.push st.run_q (Failed_thread (k, ex));
  if Atomic.get st.need_wakeup then wakeup st

(* Can only be called from our own domain, so no need to check for wakeup. *)
let enqueue_at_head st k x = Lf_queue.push_head st.run_q (Thread (k, x))

type _ Eio.Generic.ty += FD : FD.t Eio.Generic.ty

let get_fd_opt t = Eio.Generic.probe t FD

let simple_copy src dst =
  let fd = FD.get_exn "simple_copy" dst in
  let buf = Cstruct.create 4096 in
  try
    while true do
      let got = Eio.Flow.single_read src buf in
      let _wrote =
        run_in_systhread (fun () -> Unix.write fd (Cstruct.to_bytes buf) 0 got)
      in
      ()
    done
  with End_of_file -> ()

let read ?kind fd buf =
  let hack = Cstruct.to_bytes buf in
  let read =
    run_in_systhread ?kind (fun () -> Unix.read fd hack 0 (Cstruct.length buf))
  in
  read

let monitor_event_fd t =
  Eventfd.monitor ~read:(read ~kind:`Monitor) (get_eventfd "monitor_event_fd" t);
  assert false

let flow fd =
  object (_ : < Flow.source ; Flow.sink ; .. >)
    method fd = fd
    method close = FD.close fd
    method stat = FD.fstat fd

    method probe : type a. a Eio.Generic.ty -> a option =
      function FD -> Some fd | _ -> None

    method read_into buf =
      let fd = FD.get_exn "read_into" fd in
      read fd buf

    method pread ~file_offset bufs =
      let hack = Cstruct.to_bytes (List.hd bufs) in
      let fd = FD.get_exn "pread" fd in
      run_in_systhread (fun () -> Unix.read fd hack file_offset max_int)

    method pwrite ~file_offset bufs =
      let hack = Cstruct.to_bytes (List.hd bufs) in
      let fd = FD.get_exn "write" fd in
      run_in_systhread (fun () ->
          Unix.write fd hack file_offset (Bytes.length hack))
    (* Low_level.writev_single ~file_offset fd bufs *)

    method read_methods = []

    method write bufs =
      let hack = Cstruct.to_bytes (List.hd bufs) in
      let fd = FD.get_exn "write" fd in
      run_in_systhread (fun () ->
          let (_ : int) = Unix.write fd hack 0 max_int in
          ())

    method copy src = simple_copy src fd

    method shutdown cmd =
      Unix.shutdown (FD.get_exn "shutdown" fd)
      @@
      match cmd with
      | `Receive -> Unix.SHUTDOWN_RECEIVE
      | `Send -> Unix.SHUTDOWN_SEND
      | `All -> Unix.SHUTDOWN_ALL

    method unix_fd op = FD.to_unix op fd
  end

let source fd = (flow fd :> Flow.source)
let sink fd = (flow fd :> Flow.sink)

type stdenv =
  < stdin : Flow.source
  ; stdout : Flow.sink
  ; stderr : Flow.sink
  ; net : Eio.Net.t
  ; domain_mgr : Eio.Domain_manager.t
  ; clock : Eio.Time.clock
  ; mono_clock : Eio.Time.Mono.t
  ; fs : Eio.Fs.dir Eio.Path.t
  ; cwd : Eio.Fs.dir Eio.Path.t
  ; secure_random : Eio.Flow.source
  ; debug : Eio.Debug.t >

let stdenv () =
  let of_unix fd =
    FD.of_unix_no_hook ~seekable:(FD.is_seekable fd) ~close_unix:true fd
  in
  let stdin = lazy (source (of_unix Unix.stdin)) in
  let stdout = lazy (sink (of_unix Unix.stdout)) in
  let stderr = lazy (sink (of_unix Unix.stderr)) in
  let fs = Obj.magic () (* (new dir ~label:"fs" Fs, ".")  *) in
  let cwd = Obj.magic () (* (new dir ~label:"cwd" Cwd, ".") *) in
  object (_ : stdenv)
    method stdin = Lazy.force stdin
    method stdout = Lazy.force stdout
    method stderr = Lazy.force stderr
    method net = Obj.magic ()
    method domain_mgr = Obj.magic ()
    method clock = Obj.magic ()
    method mono_clock = Obj.magic ()
    method fs = (fs :> Eio.Fs.dir Eio.Path.t)
    method cwd = (cwd :> Eio.Fs.dir Eio.Path.t)
    method secure_random = Obj.magic ()
    method debug = Eio.Private.Debug.v
  end

let wait_for_work st =
  List.iter
    (fun t ->
      match t.kind with
      | `Program _ ->
          Logs.debug (fun f -> f "joining thread (%a)" pp_systhread_kind t.kind);
          Thread.join t.thread
      | `Monitor -> ())
    st.threads;
  let ths =
    List.filter_map
      (fun t -> if t.kind = `Monitor then Some t else None)
      st.threads
  in
  st.threads <- ths

let rec schedule ({ run_q; sleep_q; active_ops; _ } as st) : [ `Exit_scheduler ]
    =
  (* This is not a fair scheduler *)
  (* Wakeup any paused fibers *)
  match Lf_queue.pop run_q with
  | None -> assert false (* We should always have an IO job, at least *)
  | Some (Thread (k, v)) ->
      st.active_ops <- st.active_ops - 1;
      Suspended.continue k v (* We already have a runnable task *)
  | Some (Failed_thread (k, ex)) ->
      st.active_ops <- st.active_ops - 1;
      Suspended.discontinue k ex
  | Some IO -> (
      (* Note: be sure to re-inject the IO task before continuing! *)
      (* This is not a fair scheduler: timers always run before all other IO *)
      let now = Mtime_clock.now () in
      match Zzz.pop ~now sleep_q with
      | `Due k ->
          Lf_queue.push run_q IO;
          (* Re-inject IO job in the run queue *)
          Suspended.continue k () (* A sleeping task is now due *)
      | _ ->
          if not (Lf_queue.is_empty st.run_q) then (
            Lf_queue.push run_q IO;
            (* Re-inject IO job in the run queue *)
            schedule st)
          else if List.length st.threads = 1 then (
            (* Nothing further can happen at this point.
               If there are no events in progress but also still no memory available, something has gone wrong! *)
            Log.debug (fun l -> l "schedule: exiting");
            (* Nothing left to do *)
            Lf_queue.close st.run_q;
            (* Just to catch bugs if something tries to enqueue later *)
            `Exit_scheduler)
          else (
            Atomic.set st.need_wakeup true;
            if Lf_queue.is_empty st.run_q && List.length st.threads <> 1 then (
              (* At this point we're not going to check [run_q] again before sleeping.
                 If [need_wakeup] is still [true], this is fine because we don't promise to do that.
                 If [need_wakeup = false], a wake-up event will arrive and wake us up soon. *)
              Private.Ctf.(note_hiatus Wait_for_work);
              (* Hmm? Is this ok, it's meant to be like Uring.wait, but that just waits for
                 something as far as I know, whereas here we wait for every systhread that
                 has been spawned to be joined which sounds deadlock-y. *)
              let () = wait_for_work st in
              Atomic.set st.need_wakeup false;
              Lf_queue.push run_q IO;
              (* Re-inject IO job in the run queue *)
              schedule st)
            else (
              (* Someone added a new job while we were setting [need_wakeup] to [true].
                 They might or might not have seen that, so we can't be sure they'll send an event. *)
              Atomic.set st.need_wakeup false;
              Lf_queue.push run_q IO;
              (* Re-inject IO job in the run queue *)
              schedule st)))

let run : type a. (_ -> a) -> a =
 fun main ->
  let open Eio.Private in
  Log.debug (fun l -> l "starting run");
  let stdenv = stdenv () in
  let run_q = Lf_queue.create () in
  Lf_queue.push run_q IO;
  let eventfd_mutex = Mutex.create () in
  let sleep_q = Zzz.create () in
  let st =
    {
      run_q;
      eventfd_mutex;
      eventfd = None;
      need_wakeup = Atomic.make false;
      sleep_q;
      active_ops = 0;
      threads = [];
    }
  in
  Log.debug (fun l -> l "starting main thread");
  let rec fork ~new_fiber:fiber fn =
    let open Effect.Deep in
    Ctf.note_switch (Fiber_context.tid fiber);
    match_with fn ()
      {
        retc =
          (fun () ->
            Fiber_context.destroy fiber;
            schedule st);
        exnc =
          (fun ex ->
            Fiber_context.destroy fiber;
            Printexc.raise_with_backtrace ex (Printexc.get_raw_backtrace ()));
        effc =
          (fun (type a) (e : a Effect.t) ->
            match e with
            | Eio.Private.Effects.Get_context ->
                Some (fun (k : (a, _) continuation) -> continue k fiber)
            | Eio.Private.Effects.Suspend f ->
                Some
                  (fun k ->
                    let k = { Suspended.k; fiber } in
                    st.active_ops <- st.active_ops + 1;
                    f fiber (function
                      | Ok v -> enqueue_thread st k v
                      | Error ex -> enqueue_failed_thread st k ex);
                    schedule st)
            | Suspend_systhread f ->
                Some
                  (fun k ->
                    let k = { Suspended.k; fiber } in
                    st.active_ops <- st.active_ops + 1;
                    let t =
                      f fiber (function
                        | Ok v -> enqueue_thread st k v
                        | Error ex -> enqueue_failed_thread st k ex)
                    in
                    Option.iter (fun t -> st.threads <- t :: st.threads) t;
                    schedule st)
            | Eio.Private.Effects.Fork (new_fiber, f) ->
                Some
                  (fun k ->
                    let k = { Suspended.k; fiber } in
                    enqueue_at_head st k ();
                    fork ~new_fiber f)
            | _ -> None);
      }
  in
  let result = ref None in
  let `Exit_scheduler =
    let new_fiber = Fiber_context.make_root () in
      fork ~new_fiber (fun () ->
          Switch.run_protected (fun sw ->
              let fd = Eventfd.create 0 in
              st.eventfd <- Some fd;
              Switch.on_release sw (fun () ->
                  Mutex.lock st.eventfd_mutex;
                  Eventfd.close (get_eventfd "fork" st);
                  Mutex.unlock st.eventfd_mutex);
              result :=
                Some
                  (Fiber.first
                    (fun () -> main stdenv)
                    (fun () -> monitor_event_fd st))
          )
      )
  in
  Log.debug (fun l -> l "exit");
  match !result with Some r -> r | None -> failwith "No result!"
