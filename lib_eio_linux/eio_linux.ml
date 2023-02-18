(*
 * Copyright (C) 2020-2021 Anil Madhavapeddy
 * Copyright (C) 2023 Thomas Leonard
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

[@@@alert "-unstable"]

let src = Logs.Src.create "eio_linux" ~doc:"Effect-based IO system for Linux/io-uring"
module Log = (val Logs.src_log src : Logs.LOG)

open Eio.Std

module Fiber_context = Eio.Private.Fiber_context
module Ctf = Eio.Private.Ctf

module Suspended = Eio_utils.Suspended
module Zzz = Eio_utils.Zzz
module Lf_queue = Eio_utils.Lf_queue

type amount = Exactly of int | Upto of int

let system_thread = Ctf.mint_id ()

let unclassified_error e = Eio.Exn.create (Eio.Exn.X e)

let wrap_error code name arg =
  let ex = Eio_unix.Unix_error (code, name, arg) in
  match code with
  | ECONNREFUSED -> Eio.Net.err (Connection_failure (Refused ex))
  | ECONNRESET | EPIPE -> Eio.Net.err (Connection_reset ex)
  | _ -> unclassified_error ex

let wrap_error_fs code name arg =
  let e = Eio_unix.Unix_error (code, name, arg) in
  match code with
  | Unix.EEXIST -> Eio.Fs.err (Already_exists e)
  | Unix.ENOENT -> Eio.Fs.err (Not_found e)
  | Unix.EXDEV -> Eio.Fs.err (Permission_denied e)
  | _ -> wrap_error code name arg

module FD = struct
  module Rcfd = Eio_unix.Private.Rcfd

  type t = {
    fd : Rcfd.t;
    seekable : bool;
    close_unix : bool;                          (* Whether closing this also closes the underlying FD. *)
    mutable release_hook : Eio.Switch.hook;     (* Use this on close to remove switch's [on_release] hook. *)
  }

  let err_closed op = Invalid_argument (op ^ ": file descriptor used after calling close!")

  let use t f ~if_closed = Rcfd.use t.fd f ~if_closed

  let use_exn op t f =
    Rcfd.use t.fd f ~if_closed:(fun () -> raise (err_closed op))

  let rec use_exn_list op xs k =
    match xs with
    | [] -> k []
    | x :: xs ->
      use_exn op x @@ fun x ->
      use_exn_list op xs @@ fun xs ->
      k (x :: xs)

  let is_open t = Rcfd.is_open t.fd

  let close t =
    Ctf.label "close";
    Switch.remove_hook t.release_hook;
    if t.close_unix then (
      if not (Rcfd.close t.fd) then raise (err_closed "close")
    ) else (
      match Rcfd.remove t.fd with
      | Some _ -> ()
      | None -> raise (err_closed "close")
    )

  let is_seekable fd =
    match Unix.lseek fd 0 Unix.SEEK_CUR with
    | (_ : int) -> true
    | exception Unix.Unix_error(Unix.ESPIPE, "lseek", "") -> false

  let to_unix op t =
    match op with
    | `Peek -> Rcfd.peek t.fd
    | `Take ->
      Switch.remove_hook t.release_hook;
      match Rcfd.remove t.fd with
      | Some fd -> fd
      | None -> raise (err_closed "to_unix")

  let of_unix_no_hook ~seekable ~close_unix fd =
    { fd = Rcfd.make fd; seekable; close_unix; release_hook = Switch.null_hook }

  let of_unix ~sw ~seekable ~close_unix fd =
    let t = of_unix_no_hook ~seekable ~close_unix fd in
    t.release_hook <- Switch.on_release_cancellable sw (fun () -> close t);
    t

  let uring_file_offset t =
    if t.seekable then Optint.Int63.minus_one else Optint.Int63.zero

  let file_offset t = function
    | Some x -> `Pos x
    | None when t.seekable -> `Seekable_current
    | None -> `Nonseekable_current
end

let fstat t =
  (* todo: use uring  *)
  try
    let ust = FD.use_exn "fstat" t Unix.LargeFile.fstat in
    let st_kind : Eio.File.Stat.kind =
      match ust.st_kind with
      | Unix.S_REG  -> `Regular_file
      | Unix.S_DIR  -> `Directory
      | Unix.S_CHR  -> `Character_special
      | Unix.S_BLK  -> `Block_device
      | Unix.S_LNK  -> `Symbolic_link
      | Unix.S_FIFO -> `Fifo
      | Unix.S_SOCK -> `Socket
    in
    Eio.File.Stat.{
      dev     = ust.st_dev   |> Int64.of_int;
      ino     = ust.st_ino   |> Int64.of_int;
      kind    = st_kind;
      perm    = ust.st_perm;
      nlink   = ust.st_nlink |> Int64.of_int;
      uid     = ust.st_uid   |> Int64.of_int;
      gid     = ust.st_gid   |> Int64.of_int;
      rdev    = ust.st_rdev  |> Int64.of_int;
      size    = ust.st_size  |> Optint.Int63.of_int64;
      atime   = ust.st_atime;
      mtime   = ust.st_mtime;
      ctime   = ust.st_ctime;
    }
  with Unix.Unix_error (code, name, arg) -> raise @@ wrap_error_fs code name arg

type _ Eio.Generic.ty += FD : FD.t Eio.Generic.ty
let get_fd_opt t = Eio.Generic.probe t FD

type dir_fd =
  | FD of FD.t
  | Cwd         (* Confined to "." *)
  | Fs          (* Unconfined "."; also allows absolute paths *)

type _ Eio.Generic.ty += Dir_fd : dir_fd Eio.Generic.ty
let get_dir_fd_opt t = Eio.Generic.probe t Dir_fd

type file_offset = [
  | `Pos of Optint.Int63.t
  | `Seekable_current
  | `Nonseekable_current
]

type rw_req = {
  op : [`R|`W];
  file_offset : file_offset;    (* Read from here + cur_off (unless using current pos) *)
  fd : Unix.file_descr;
  len : amount;
  buf : Uring.Region.chunk;
  mutable cur_off : int;
  action : int Suspended.t;
}

(* Type of user-data attached to jobs. *)
type io_job =
  | Read : rw_req -> io_job
  | Job_no_cancel : int Suspended.t -> io_job
  | Cancel_job : io_job
  | Job : int Suspended.t -> io_job     (* A negative result indicates error, and may report cancellation *)
  | Write : rw_req -> io_job
  | Job_fn : 'a Suspended.t * (int -> [`Exit_scheduler]) -> io_job
  (* When done, remove the cancel_fn from [Suspended.t] and call the callback (unless cancelled). *)

type runnable =
  | IO : runnable
  | Thread : 'a Suspended.t * 'a -> runnable
  | Failed_thread : 'a Suspended.t * exn -> runnable

type t = {
  uring: io_job Uring.t;
  mem: Uring.Region.t option;
  io_q: (t -> unit) Queue.t;     (* waiting for room on [uring] *)
  mem_q : Uring.Region.chunk Suspended.t Queue.t;

  (* The queue of runnable fibers ready to be resumed. Note: other domains can also add work items here. *)
  run_q : runnable Lf_queue.t;

  (* When adding to [run_q] from another domain, this domain may be sleeping and so won't see the event.
     In that case, [need_wakeup = true] and you must signal using [eventfd]. *)
  eventfd : FD.t;

  (* If [false], the main thread will check [run_q] before sleeping again
     (possibly because an event has been or will be sent to [eventfd]).
     It can therefore be set to [false] in either of these cases:
     - By the receiving thread because it will check [run_q] before sleeping, or
     - By the sending thread because it will signal the main thread later *)
  need_wakeup : bool Atomic.t;

  sleep_q: Zzz.t;
}

let wake_buffer =
  let b = Bytes.create 8 in
  Bytes.set_int64_ne b 0 1L;
  b

(* This can be called from any systhread (including ones not running Eio),
   and also from signal handlers or GC finalizers. It must not take any locks. *)
let wakeup t =
  Atomic.set t.need_wakeup false; (* [t] will check [run_q] after getting the event below *)
  FD.use t.eventfd
    (fun fd ->
       let sent = Unix.single_write fd wake_buffer 0 8 in
       assert (sent = 8)
    )
    ~if_closed:ignore   (* Domain has shut down (presumably after handling the event) *)

(* Safe to call from anywhere (other systhreads, domains, signal handlers, GC finalizers) *)
let enqueue_thread st k x =
  Lf_queue.push st.run_q (Thread (k, x));
  if Atomic.get st.need_wakeup then wakeup st

(* Safe to call from anywhere (other systhreads, domains, signal handlers, GC finalizers) *)
let enqueue_failed_thread st k ex =
  Lf_queue.push st.run_q (Failed_thread (k, ex));
  if Atomic.get st.need_wakeup then wakeup st

(* Can only be called from our own domain, so no need to check for wakeup. *)
let enqueue_at_head st k x =
  Lf_queue.push_head st.run_q (Thread (k, x))

type _ Effect.t += Enter : (t -> 'a Suspended.t -> unit) -> 'a Effect.t
type _ Effect.t += Cancel : io_job Uring.job -> unit Effect.t
let enter fn = Effect.perform (Enter fn)

let rec enqueue_job t fn =
  match fn () with
  | Some _ as r -> r
  | None ->
    if Uring.submit t.uring > 0 then enqueue_job t fn
    else None

(* Cancellations always come from the same domain, so no need to send wake events here. *)
let rec enqueue_cancel job t =
  Ctf.label "cancel";
  match enqueue_job t (fun () -> Uring.cancel t.uring job Cancel_job) with
  | None -> Queue.push (fun t -> enqueue_cancel job t) t.io_q
  | Some _ -> ()

let cancel job = Effect.perform (Cancel job)

(* Cancellation

   For operations that can be cancelled we need to set the fiber's cancellation function.
   The typical sequence is:

   1. We submit an operation, getting back a uring job (needed for cancellation).
   2. We set the cancellation function. The function uses the uring job to cancel.

   When the job completes, we clear the cancellation function. The function
   must have been set by this point because we don't poll for completions until
   the above steps have finished.

   If the context is cancelled while the operation is running, the function will get removed and called,
   which will submit a cancellation request to uring. We know the job is still valid at this point because
   we clear the cancel function when it completes.

   If the operation completes before Linux processes the cancellation, we get [ENOENT], which we ignore. *)

(* [with_cancel_hook ~action t fn] calls [fn] to create a job,
   then sets the fiber's cancel function to cancel it.
   If [action] is already cancelled, it schedules [action] to be discontinued.
   @return Whether to retry the operation later, once there is space. *)
let with_cancel_hook ~action t fn =
  match Fiber_context.get_error action.Suspended.fiber with
  | Some ex -> enqueue_failed_thread t action ex; false
  | None ->
    match enqueue_job t fn with
    | None -> true
    | Some job ->
      Fiber_context.set_cancel_fn action.fiber (fun _ -> cancel job);
      false

let rec submit_rw_req st ({op; file_offset; fd; buf; len; cur_off; action} as req) =
  let {uring;io_q;_} = st in
  let off = Uring.Region.to_offset buf + cur_off in
  let len = match len with Exactly l | Upto l -> l in
  let len = len - cur_off in
  let retry = with_cancel_hook ~action st (fun () ->
      let file_offset =
        match file_offset with
        | `Pos x -> Optint.Int63.add x (Optint.Int63.of_int cur_off)
        | `Seekable_current -> Optint.Int63.minus_one
        | `Nonseekable_current -> Optint.Int63.zero
      in
      match op with
      |`R -> Uring.read_fixed uring ~file_offset fd ~off ~len (Read req)
      |`W -> Uring.write_fixed uring ~file_offset fd ~off ~len (Write req)
    )
  in
  if retry then (
    Ctf.label "await-sqe";
    (* wait until an sqe is available *)
    Queue.push (fun st -> submit_rw_req st req) io_q
  )

(* TODO bind from unixsupport *)
let errno_is_retry = function -62 | -11 | -4 -> true |_ -> false

let enqueue_read st action (file_offset,fd,buf,len) =
  let req = { op=`R; file_offset; len; fd; cur_off = 0; buf; action } in
  Ctf.label "read";
  submit_rw_req st req

let rec enqueue_readv args st action =
  let (file_offset,fd,bufs) = args in
  Ctf.label "readv";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.readv st.uring ~file_offset fd bufs (Job action))
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_readv args st action) st.io_q

let rec enqueue_writev args st action =
  let (file_offset,fd,bufs) = args in
  Ctf.label "writev";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.writev st.uring ~file_offset fd bufs (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_writev args st action) st.io_q

let rec enqueue_poll_add fd poll_mask st action =
  Ctf.label "poll_add";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.poll_add st.uring fd poll_mask (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_poll_add fd poll_mask st action) st.io_q

let rec enqueue_poll_add_unix fd poll_mask st action cb =
  Ctf.label "poll_add";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.poll_add st.uring fd poll_mask (Job_fn (action, cb))
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_poll_add_unix fd poll_mask st action cb) st.io_q

let enqueue_write st action (file_offset,fd,buf,len) =
  let req = { op=`W; file_offset; len; fd; cur_off = 0; buf; action } in
  Ctf.label "write";
  submit_rw_req st req

let rec enqueue_splice ~src ~dst ~len st action =
  Ctf.label "splice";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.splice st.uring (Job action) ~src ~dst ~len
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_splice ~src ~dst ~len st action) st.io_q

let rec enqueue_openat2 ((access, flags, perm, resolve, fd, path) as args) st action =
  Ctf.label "openat2";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.openat2 st.uring ~access ~flags ~perm ~resolve ?fd path (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_openat2 args st action) st.io_q

let rec enqueue_unlink ((dir, fd, path) as args) st action =
  Ctf.label "unlinkat";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.unlink st.uring ~dir ~fd path (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_unlink args st action) st.io_q

let rec enqueue_connect fd addr st action =
  Ctf.label "connect";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.connect st.uring fd addr (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_connect fd addr st action) st.io_q

let rec enqueue_send_msg fd ~fds ~dst buf st action =
  Ctf.label "send_msg";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.send_msg st.uring fd ~fds ?dst buf (Job action)
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_send_msg fd ~fds ~dst buf st action) st.io_q

let rec enqueue_recv_msg fd msghdr st action =
  Ctf.label "recv_msg";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.recv_msg st.uring fd msghdr (Job action);
    )
  in
  if retry then (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_recv_msg fd msghdr st action) st.io_q

let rec enqueue_accept fd client_addr st action =
  Ctf.label "accept";
  let retry = with_cancel_hook ~action st (fun () ->
      Uring.accept st.uring fd client_addr (Job action)
    ) in
  if retry then (
    (* wait until an sqe is available *)
    Queue.push (fun st -> enqueue_accept fd client_addr st action) st.io_q
  )

let rec enqueue_noop t action =
  Ctf.label "noop";
  let job = enqueue_job t (fun () -> Uring.noop t.uring (Job_no_cancel action)) in
  if job = None then (
    (* wait until an sqe is available *)
    Queue.push (fun t -> enqueue_noop t action) t.io_q
  )

let submit_pending_io st =
  match Queue.take_opt st.io_q with
  | None -> ()
  | Some fn ->
    Ctf.label "submit_pending_io";
    fn st

(* Switch control to the next ready continuation.
   If none is ready, wait until we get an event to wake one and then switch.
   Returns only if there is nothing to do and no queued operations. *)
let rec schedule ({run_q; sleep_q; mem_q; uring; _} as st) : [`Exit_scheduler] =
  (* This is not a fair scheduler *)
  (* Wakeup any paused fibers *)
  match Lf_queue.pop run_q with
  | None -> assert false    (* We should always have an IO job, at least *)
  | Some Thread (k, v) ->   (* We already have a runnable task *)
    Fiber_context.clear_cancel_fn k.fiber;
    Suspended.continue k v
  | Some Failed_thread (k, ex) ->
    Fiber_context.clear_cancel_fn k.fiber;
    Suspended.discontinue k ex
  | Some IO -> (* Note: be sure to re-inject the IO task before continuing! *)
    (* This is not a fair scheduler: timers always run before all other IO *)
    let now = Mtime_clock.now () in
    match Zzz.pop ~now sleep_q with
    | `Due k ->
      Lf_queue.push run_q IO;                   (* Re-inject IO job in the run queue *)
      Suspended.continue k ()                   (* A sleeping task is now due *)
    | `Wait_until _ | `Nothing as next_due ->
      (* Handle any pending events before submitting. This is faster. *)
      match Uring.get_cqe_nonblocking uring with
      | Some { data = runnable; result } ->
        Lf_queue.push run_q IO;                   (* Re-inject IO job in the run queue *)
        handle_complete st ~runnable result
      | None ->
        ignore (Uring.submit uring : int);
        let timeout =
          match next_due with
          | `Wait_until time ->
            let time = Mtime.to_uint64_ns time in
            let now = Mtime.to_uint64_ns now in
            let diff_ns = Int64.sub time now |> Int64.to_float in
            Some (diff_ns /. 1e9)
          | `Nothing -> None
        in
        if not (Lf_queue.is_empty st.run_q) then (
          Lf_queue.push run_q IO;                   (* Re-inject IO job in the run queue *)
          schedule st
        ) else if timeout = None && Uring.active_ops uring = 0 then (
          (* Nothing further can happen at this point.
             If there are no events in progress but also still no memory available, something has gone wrong! *)
          assert (Queue.length mem_q = 0);
          Lf_queue.close st.run_q;      (* Just to catch bugs if something tries to enqueue later *)
          `Exit_scheduler
        ) else (
          Atomic.set st.need_wakeup true;
          if Lf_queue.is_empty st.run_q then (
            (* At this point we're not going to check [run_q] again before sleeping.
               If [need_wakeup] is still [true], this is fine because we don't promise to do that.
               If [need_wakeup = false], a wake-up event will arrive and wake us up soon. *)
            Ctf.(note_hiatus Wait_for_work);
            let result = Uring.wait ?timeout uring in
            Ctf.note_resume system_thread;
            Atomic.set st.need_wakeup false;
            Lf_queue.push run_q IO;                   (* Re-inject IO job in the run queue *)
            match result with
            | None ->
              (* Woken by a timeout, which is now due, or by a signal. *)
              schedule st
            | Some { data = runnable; result } ->
              handle_complete st ~runnable result
          ) else (
            (* Someone added a new job while we were setting [need_wakeup] to [true].
               They might or might not have seen that, so we can't be sure they'll send an event. *)
            Atomic.set st.need_wakeup false;
            Lf_queue.push run_q IO;                   (* Re-inject IO job in the run queue *)
            schedule st
          )
        )
and handle_complete st ~runnable result =
  submit_pending_io st;                       (* If something was waiting for a slot, submit it now. *)
  match runnable with
  | Read req ->
    complete_rw_req st req result
  | Write req ->
    complete_rw_req st req result
  | Job k ->
    Fiber_context.clear_cancel_fn k.fiber;
    if result >= 0 then Suspended.continue k result
    else (
      match Fiber_context.get_error k.fiber with
        | None -> Suspended.continue k result
        | Some e ->
          (* If cancelled, report that instead. *)
          Suspended.discontinue k e
    )
  | Job_no_cancel k ->
    Suspended.continue k result
  | Cancel_job ->
    begin match result with
      | 0     (* Operation cancelled successfully *)
      | -2    (* ENOENT - operation completed before cancel took effect *)
      | -114  (* EALREADY - operation already in progress *)
        -> ()
      | errno ->
        Log.warn (fun f -> f "Cancel returned unexpected error: %s" (Unix.error_message (Uring.error_of_errno errno)))
    end;
    schedule st
  | Job_fn (k, f) ->
    Fiber_context.clear_cancel_fn k.fiber;
    (* Should we only do this on error, to avoid losing the return value?
       We already do that with rw jobs. *)
    begin match Fiber_context.get_error k.fiber with
      | None -> f result
      | Some e -> Suspended.discontinue k e
    end
and complete_rw_req st ({len; cur_off; action; _} as req) res =
  Fiber_context.clear_cancel_fn action.fiber;
  match res, len with
  | 0, _ -> Suspended.discontinue action End_of_file
  | e, _ when e < 0 ->
    begin match Fiber_context.get_error action.fiber with
      | Some e -> Suspended.discontinue action e        (* If cancelled, report that instead. *)
      | None ->
        if errno_is_retry e then (
          submit_rw_req st req;
          schedule st
        ) else (
          Suspended.continue action e
        )
    end
  | n, Exactly len when n < len - cur_off ->
    req.cur_off <- req.cur_off + n;
    submit_rw_req st req;
    schedule st
  | _, Exactly len -> Suspended.continue action len
  | n, Upto _ -> Suspended.continue action n

module Low_level = struct
  let alloc_buf_or_wait st k =
    match st.mem with
    | None -> Suspended.discontinue k (Failure "No fixed buffer available")
    | Some mem ->
      match Uring.Region.alloc mem with
      | buf -> Suspended.continue k buf
      | exception Uring.Region.No_space ->
        Queue.push k st.mem_q;
        schedule st

  let free_buf st buf =
    match Queue.take_opt st.mem_q with
    | None -> Uring.Region.free buf
    | Some k -> enqueue_thread st k buf

  let noop () =
    let result = enter enqueue_noop in
    if result <> 0 then raise (unclassified_error (Eio_unix.Unix_error (Uring.error_of_errno result, "noop", "")))

  type _ Effect.t += Sleep_until : Mtime.t -> unit Effect.t
  let sleep_until d =
    Effect.perform (Sleep_until d)

  type _ Effect.t += ERead : (file_offset * Unix.file_descr * Uring.Region.chunk * amount) -> int Effect.t

  let read_exactly ?file_offset fd buf len =
    let file_offset = FD.file_offset fd file_offset in
    FD.use_exn "read_exactly" fd @@ fun fd ->
    let res = Effect.perform (ERead (file_offset, fd, buf, Exactly len)) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "read_exactly" ""
    )

  let read_upto ?file_offset fd buf len =
    let file_offset = FD.file_offset fd file_offset in
    FD.use_exn "read_upto" fd @@ fun fd ->
    let res = Effect.perform (ERead (file_offset, fd, buf, Upto len)) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "read_upto" ""
    ) else (
      res
    )

  let readv ?file_offset fd bufs =
    let file_offset =
      match file_offset with
      | Some x -> x
      | None -> FD.uring_file_offset fd
    in
    FD.use_exn "readv" fd @@ fun fd ->
    let res = enter (enqueue_readv (file_offset, fd, bufs)) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "readv" ""
    ) else if res = 0 then (
      raise End_of_file
    ) else (
      res
    )

  let writev_single ?file_offset fd bufs =
    let file_offset =
      match file_offset with
      | Some x -> x
      | None -> FD.uring_file_offset fd
    in
    FD.use_exn "writev" fd @@ fun fd ->
    let res = enter (enqueue_writev (file_offset, fd, bufs)) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "writev" ""
    ) else (
      res
    )

  let rec writev ?file_offset fd bufs =
    let bytes_written = writev_single ?file_offset fd bufs in
    match Cstruct.shiftv bufs bytes_written with
    | [] -> ()
    | bufs ->
      let file_offset =
        let module I63 = Optint.Int63 in
        match file_offset with
        | None -> None
        | Some ofs when ofs = I63.minus_one -> Some I63.minus_one
        | Some ofs -> Some (I63.add ofs (I63.of_int bytes_written))
      in
      writev ?file_offset fd bufs

  let await_readable fd =
    FD.use_exn "await_readable" fd @@ fun fd ->
    let res = enter (enqueue_poll_add fd (Uring.Poll_mask.(pollin + pollerr))) in
    if res < 0 then (
      raise (unclassified_error (Eio_unix.Unix_error (Uring.error_of_errno res, "await_readable", "")))
    )

  let await_writable fd =
    FD.use_exn "await_writable" fd @@ fun fd ->
    let res = enter (enqueue_poll_add fd (Uring.Poll_mask.(pollout + pollerr))) in
    if res < 0 then (
      raise (unclassified_error (Eio_unix.Unix_error (Uring.error_of_errno res, "await_writable", "")))
    )

  type _ Effect.t += EWrite : (file_offset * Unix.file_descr * Uring.Region.chunk * amount) -> int Effect.t

  let write ?file_offset fd buf len =
    let file_offset = FD.file_offset fd file_offset in
    FD.use_exn "write" fd @@ fun fd ->
    let res = Effect.perform (EWrite (file_offset, fd, buf, Exactly len)) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "write" ""
    )

  type _ Effect.t += Alloc : Uring.Region.chunk option Effect.t
  let alloc_fixed () = Effect.perform Alloc

  type _ Effect.t += Alloc_or_wait : Uring.Region.chunk Effect.t
  let alloc_fixed_or_wait () = Effect.perform Alloc_or_wait

  type _ Effect.t += Free : Uring.Region.chunk -> unit Effect.t
  let free_fixed buf = Effect.perform (Free buf)

  let splice src ~dst ~len =
    FD.use_exn "splice-src" src @@ fun src ->
    FD.use_exn "splice-dst" dst @@ fun dst ->
    let res = enter (enqueue_splice ~src ~dst ~len) in
    if res > 0 then res
    else if res = 0 then raise End_of_file
    else raise @@ wrap_error (Uring.error_of_errno res) "splice" ""

  let connect fd addr =
    FD.use_exn "connect" fd @@ fun fd ->
    let res = enter (enqueue_connect fd addr) in
    if res < 0 then (
      let ex =
        match addr with
        | ADDR_UNIX _ -> wrap_error_fs (Uring.error_of_errno res) "connect" ""
        | ADDR_INET _ -> wrap_error (Uring.error_of_errno res) "connect" ""
      in
      raise ex
    )

  let send_msg fd ?(fds=[]) ?dst buf =
    FD.use_exn "send_msg" fd @@ fun fd ->
    FD.use_exn_list "send_msg" fds @@ fun fds ->
    let res = enter (enqueue_send_msg fd ~fds ~dst buf) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "send_msg" ""
    )

  let recv_msg fd buf =
    FD.use_exn "recv_msg" fd @@ fun fd ->
    let addr = Uring.Sockaddr.create () in
    let msghdr = Uring.Msghdr.create ~addr buf in
    let res = enter (enqueue_recv_msg fd msghdr) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "recv_msg" ""
    );
    addr, res

  let recv_msg_with_fds ~sw ~max_fds fd buf =
    FD.use_exn "recv_msg_with_fds" fd @@ fun fd ->
    let addr = Uring.Sockaddr.create () in
    let msghdr = Uring.Msghdr.create ~n_fds:max_fds ~addr buf in
    let res = enter (enqueue_recv_msg fd msghdr) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "recv_msg" ""
    );
    let fds =
      Uring.Msghdr.get_fds msghdr
      |> List.map (fun fd -> FD.of_unix ~sw ~seekable:(FD.is_seekable fd) ~close_unix:true fd)
    in
    addr, res, fds

  let with_chunk ~fallback fn =
    match alloc_fixed () with
    | Some chunk ->
      Fun.protect ~finally:(fun () -> free_fixed chunk) @@ fun () ->
      fn chunk
    | None ->
      fallback ()

  let openat2 ~sw ?seekable ~access ~flags ~perm ~resolve ?dir path =
    let use dir =
      let res = enter (enqueue_openat2 (access, flags, perm, resolve, dir, path)) in
      if res < 0 then (
        Switch.check sw;    (* If cancelled, report that instead. *)
        raise @@ wrap_error_fs (Uring.error_of_errno res) "openat2" ""
      );
      let fd : Unix.file_descr = Obj.magic res in
      let seekable =
        match seekable with
        | None -> FD.is_seekable fd
        | Some x -> x
      in
      FD.of_unix ~sw ~seekable ~close_unix:true fd
    in
    match dir with
    | None -> use None
    | Some dir -> FD.use_exn "openat2" dir (fun x -> use (Some x))

  let openat ~sw ?seekable ~access ~flags ~perm dir path =
    match dir with
    | FD dir -> openat2 ~sw ?seekable ~access ~flags ~perm ~resolve:Uring.Resolve.beneath ~dir path
    | Cwd -> openat2 ~sw ?seekable ~access ~flags ~perm ~resolve:Uring.Resolve.beneath path
    | Fs -> openat2 ~sw ?seekable ~access ~flags ~perm ~resolve:Uring.Resolve.empty path

  let fstat fd = fstat fd

  external eio_mkdirat : Unix.file_descr -> string -> Unix.file_perm -> unit = "caml_eio_mkdirat"

  external eio_renameat : Unix.file_descr -> string -> Unix.file_descr -> string -> unit = "caml_eio_renameat"

  external eio_getrandom : Cstruct.buffer -> int -> int -> int = "caml_eio_getrandom"

  external eio_getdents : Unix.file_descr -> string list = "caml_eio_getdents"

  let getrandom { Cstruct.buffer; off; len } =
    let rec loop n =
      if n = len then
        ()
      else
        loop (n + eio_getrandom buffer (off + n) (len - n))
    in
    loop 0

  (* [with_parent_dir dir path fn] runs [fn parent (basename path)],
     where [parent] is a path FD for [path]'s parent, resolved using [Resolve.beneath]. *)
  let with_parent_dir op dir path fn =
    let dir_path = Filename.dirname path in
    let leaf = Filename.basename path in
    Switch.run (fun sw ->
        let parent =
          match dir with
          | FD d when dir_path = "." -> d
          | _ ->
            openat ~sw ~seekable:false dir dir_path
              ~access:`R
              ~flags:Uring.Open_flags.(cloexec + path + directory)
              ~perm:0
        in
        FD.use_exn op parent @@ fun parent ->
        fn parent leaf
      )

  let mkdir_beneath ~perm dir path =
    (* [mkdir] is really an operation on [path]'s parent. Get a reference to that first: *)
    with_parent_dir "mkdir" dir path @@ fun parent leaf ->
    try eio_mkdirat parent leaf perm
    with Unix.Unix_error (code, name, arg) -> raise @@ wrap_error_fs code name arg

  let unlink ~rmdir dir path =
    (* [unlink] is really an operation on [path]'s parent. Get a reference to that first: *)
    with_parent_dir "unlink" dir path @@ fun parent leaf ->
    let res = enter (enqueue_unlink (rmdir, parent, leaf)) in
    if res <> 0 then raise @@ wrap_error_fs (Uring.error_of_errno res) "unlinkat" ""

  let rename old_dir old_path new_dir new_path =
    with_parent_dir "renameat-old" old_dir old_path @@ fun old_parent old_leaf ->
    with_parent_dir "renameat-new" new_dir new_path @@ fun new_parent new_leaf ->
    try
      eio_renameat
        old_parent old_leaf
        new_parent new_leaf
    with Unix.Unix_error (code, name, arg) -> raise @@ wrap_error_fs code name arg

  let shutdown socket command =
    FD.use_exn "shutdown" socket @@ fun fd ->
    try Unix.shutdown fd command
    with Unix.Unix_error (code, name, arg) -> raise @@ wrap_error code name arg

  let accept ~sw fd =
    Ctf.label "accept";
    FD.use_exn "accept" fd @@ fun fd ->
    let client_addr = Uring.Sockaddr.create () in
    let res = enter (enqueue_accept fd client_addr) in
    if res < 0 then (
      raise @@ wrap_error (Uring.error_of_errno res) "accept" ""
    ) else (
      let unix : Unix.file_descr = Obj.magic res in
      let client = FD.of_unix ~sw ~seekable:false ~close_unix:true unix in
      let client_addr = Uring.Sockaddr.get client_addr in
      client, client_addr
    )

  let open_dir ~sw dir path =
    openat ~sw ~seekable:false dir path
      ~access:`R
      ~flags:Uring.Open_flags.(cloexec + directory)
      ~perm:0

  let read_dir fd =
    FD.use_exn "read_dir" fd @@ fun fd ->
    let rec read_all acc fd =
      match eio_getdents fd with
      | [] -> acc
      | files ->
        let files = List.filter (function ".." | "." -> false | _ -> true) files in
        read_all (files @ acc) fd
    in
    Eio_unix.run_in_systhread (fun () -> read_all [] fd)

  (* https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml *)
  let getaddrinfo ~service node =
    let to_eio_sockaddr_t {Unix.ai_family; ai_addr; ai_socktype; ai_protocol; _ } =
      match ai_family, ai_socktype, ai_addr with
      | (Unix.PF_INET | PF_INET6),
        (Unix.SOCK_STREAM | SOCK_DGRAM),
        Unix.ADDR_INET (inet_addr,port) -> (
          match ai_protocol with
          | 6 -> Some (`Tcp (Eio_unix.Ipaddr.of_unix inet_addr, port))
          | 17 -> Some (`Udp (Eio_unix.Ipaddr.of_unix inet_addr, port))
          | _ -> None)
      | _ -> None
    in
    Eio_unix.run_in_systhread @@ fun () ->
    Unix.getaddrinfo node service []
    |> List.filter_map to_eio_sockaddr_t

    module Process = struct
      external pidfd_open : int -> Unix.file_descr = "caml_eio_pidfd_open" 

      type t = {
        process : FD.t;
        pid : int;
        mutable hook : Switch.hook option;
        mutable status : Unix.process_status option;
      }

      let await_exit t =
        match t.status with
          | Some status -> status
          | None ->
            await_readable t.process;
            let status = Unix.waitpid [] t.pid |> snd in
            Option.iter Switch.remove_hook t.hook;
            t.status <- Some status;
            status

      let resolve_program ~paths prog =
        if not (Filename.is_implicit prog) then Some prog
        else
        let exists path =
          let p = Filename.concat path prog in
          if Sys.file_exists p then Some p else None
        in
        List.find_map exists paths
      
      let spawn ?env ?cwd ?stdin ?stdout ?stderr ~sw prog argv =
        let paths = Option.map (fun v -> String.split_on_char ':' v) (Sys.getenv_opt "PATH") |> Option.value ~default:[ "/usr/bin"; "/usr/local/bin" ] in
        let prog = match resolve_program ~paths prog with
          | Some prog -> prog
          | None -> raise (Eio.Fs.err (Eio.Fs.Not_found (Eio_unix.Unix_error (Unix.ENOENT, "", ""))))
        in
        let pid = Spawn.spawn ?env ?cwd ?stdin ?stdout ?stderr ~prog ~argv () in
        let fd = pidfd_open pid in
        let process = FD.of_unix ~sw ~seekable:false ~close_unix:true fd in
        let t = { process; pid; hook = None; status = None } in
        let hook = Switch.on_release_cancellable sw (fun () ->
          Unix.kill pid Sys.sigkill; ignore (await_exit t)
        ) in
        t.hook <- Some hook;
        t

        let send_signal t i = Unix.kill t.pid i
    end
end

external eio_eventfd : int -> Unix.file_descr = "caml_eio_eventfd"

type has_fd = < fd : FD.t >
type source = < Eio.Flow.source; Eio.Flow.close; has_fd >
type sink   = < Eio.Flow.sink  ; Eio.Flow.close; has_fd >

let get_fd (t : <has_fd; ..>) = t#fd

(* When copying between a source with an FD and a sink with an FD, we can share the chunk
   and avoid copying. *)
let fast_copy src dst =
  let fallback () =
    (* No chunks available. Use regular memory instead. *)
    let buf = Cstruct.create 4096 in
    try
      while true do
        let got = Low_level.readv src [buf] in
        Low_level.writev dst [Cstruct.sub buf 0 got]
      done
    with End_of_file -> ()
  in
  Low_level.with_chunk ~fallback @@ fun chunk ->
  let chunk_size = Uring.Region.length chunk in
  try
    while true do
      let got = Low_level.read_upto src chunk chunk_size in
      Low_level.write dst chunk got
    done
  with End_of_file -> ()

(* Try a fast copy using splice. If the FDs don't support that, switch to copying. *)
let _fast_copy_try_splice src dst =
  try
    while true do
      let _ : int = Low_level.splice src ~dst ~len:max_int in
      ()
    done
  with
  | End_of_file -> ()
  | Eio.Exn.Io (Eio.Exn.X Eio_unix.Unix_error ((EAGAIN | EINVAL), "splice", _), _) -> fast_copy src dst

(* XXX workaround for issue #319, PR #327 *)
let fast_copy_try_splice src dst = fast_copy src dst

(* Copy using the [Read_source_buffer] optimisation.
   Avoids a copy if the source already has the data. *)
let copy_with_rsb rsb dst =
  try
    while true do
      rsb (Low_level.writev_single dst)
    done
  with End_of_file -> ()

(* Copy by allocating a chunk from the pre-shared buffer and asking
   the source to write into it. This used when the other methods
   aren't available. *)
let fallback_copy src dst =
  let fallback () =
    (* No chunks available. Use regular memory instead. *)
    let buf = Cstruct.create 4096 in
    try
      while true do
        let got = Eio.Flow.single_read src buf in
        Low_level.writev dst [Cstruct.sub buf 0 got]
      done
    with End_of_file -> ()
  in
  Low_level.with_chunk ~fallback @@ fun chunk ->
  let chunk_cs = Uring.Region.to_cstruct chunk in
  try
    while true do
      let got = Eio.Flow.single_read src chunk_cs in
      Low_level.write dst chunk got
    done
  with End_of_file -> ()

let udp_socket sock = object
  inherit Eio.Net.datagram_socket

  method close = FD.close sock

  method send sockaddr buf =
    let addr = match sockaddr with
      | `Udp (host, port) ->
        let host = Eio_unix.Ipaddr.to_unix host in
        Unix.ADDR_INET (host, port)
    in
    Low_level.send_msg sock ~dst:addr [buf]

  method recv buf =
    let addr, recv = Low_level.recv_msg sock [buf] in
    match Uring.Sockaddr.get addr with
      | Unix.ADDR_INET (inet, port) ->
        `Udp (Eio_unix.Ipaddr.of_unix inet, port), recv
      | Unix.ADDR_UNIX _ ->
        raise (Failure "Expected INET UDP socket address but got Unix domain socket address.")
end

let flow fd =
  let is_tty = FD.use_exn "isatty" fd Unix.isatty in
  object (_ : <source; sink; ..>)
    method fd = fd
    method close = FD.close fd

    method stat = fstat fd

    method probe : type a. a Eio.Generic.ty -> a option = function
      | FD -> Some fd
      | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd)
      | _ -> None

    method read_into buf =
      if is_tty then (
        (* Work-around for https://github.com/axboe/liburing/issues/354
           (should be fixed in Linux 5.14) *)
        Low_level.await_readable fd
      );
      Low_level.readv fd [buf]

    method pread ~file_offset bufs =
      Low_level.readv ~file_offset fd bufs

    method pwrite ~file_offset bufs =
      Low_level.writev_single ~file_offset fd bufs

    method read_methods = []

    method write bufs = Low_level.writev fd bufs

    method copy src =
      match get_fd_opt src with
      | Some src -> fast_copy_try_splice src fd
      | None ->
        let rec aux = function
          | Eio.Flow.Read_source_buffer rsb :: _ -> copy_with_rsb rsb fd
          | _ :: xs -> aux xs
          | [] -> fallback_copy src fd
        in
        aux (Eio.Flow.read_methods src)

    method shutdown cmd =
      FD.use_exn "shutdown" fd @@ fun fd ->
      Unix.shutdown fd @@ match cmd with
      | `Receive -> Unix.SHUTDOWN_RECEIVE
      | `Send -> Unix.SHUTDOWN_SEND
      | `All -> Unix.SHUTDOWN_ALL

    method unix_fd op = FD.to_unix op fd
  end

let source fd = (flow fd :> source)
let sink   fd = (flow fd :> sink)

let listening_socket fd = object
  inherit Eio.Net.listening_socket

  method! probe : type a. a Eio.Generic.ty -> a option = function
    | Eio_unix.Private.Unix_file_descr op -> Some (FD.to_unix op fd)
    | _ -> None

  method close = FD.close fd

  method accept ~sw =
    Switch.check sw;
    let client, client_addr = Low_level.accept ~sw fd in
    let client_addr = match client_addr with
      | Unix.ADDR_UNIX path         -> `Unix path
      | Unix.ADDR_INET (host, port) -> `Tcp (Eio_unix.Ipaddr.of_unix host, port)
    in
    let flow = (flow client :> <Eio.Flow.two_way; Eio.Flow.close>) in
    flow, client_addr
end

let socket_domain_of = function
  | `Unix _ -> Unix.PF_UNIX
  | `UdpV4 -> Unix.PF_INET
  | `UdpV6 -> Unix.PF_INET6
  | `Udp (host, _)
  | `Tcp (host, _) ->
    Eio.Net.Ipaddr.fold host
      ~v4:(fun _ -> Unix.PF_INET)
      ~v6:(fun _ -> Unix.PF_INET6)

let net = object
  inherit Eio.Net.t

  method listen ~reuse_addr ~reuse_port  ~backlog ~sw listen_addr =
    let socket_type, addr =
      match listen_addr with
      | `Unix path         ->
        if reuse_addr then (
          match Unix.lstat path with
          | Unix.{ st_kind = S_SOCK; _ } -> Unix.unlink path
          | _ -> ()
          | exception Unix.Unix_error (Unix.ENOENT, _, _) -> ()
          | exception Unix.Unix_error (code, name, arg) -> raise @@ wrap_error code name arg
        );
        Unix.SOCK_STREAM, Unix.ADDR_UNIX path
      | `Tcp (host, port)  ->
        let host = Eio_unix.Ipaddr.to_unix host in
        Unix.SOCK_STREAM, Unix.ADDR_INET (host, port)
    in
    let sock_unix = Unix.socket ~cloexec:true (socket_domain_of listen_addr) socket_type 0 in
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
    let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true sock_unix in
    Unix.bind sock_unix addr;
    Unix.listen sock_unix backlog;
    listening_socket sock

  method connect ~sw connect_addr =
    let addr =
      match connect_addr with
      | `Unix path         -> Unix.ADDR_UNIX path
      | `Tcp (host, port)  ->
        let host = Eio_unix.Ipaddr.to_unix host in
        Unix.ADDR_INET (host, port)
    in
    let sock_unix = Unix.socket ~cloexec:true (socket_domain_of connect_addr) Unix.SOCK_STREAM 0 in
    let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true sock_unix in
    Low_level.connect sock addr;
    (flow sock :> <Eio.Flow.two_way; Eio.Flow.close>)

  method datagram_socket ~reuse_addr ~reuse_port ~sw saddr =
    let sock_unix = Unix.socket ~cloexec:true (socket_domain_of saddr) Unix.SOCK_DGRAM 0 in
    let sock = FD.of_unix ~sw ~seekable:false ~close_unix:true sock_unix in
    begin match saddr with
    | `Udp (host, port) ->
      let host = Eio_unix.Ipaddr.to_unix host in
      let addr = Unix.ADDR_INET (host, port) in
      if reuse_addr then
        Unix.setsockopt sock_unix Unix.SO_REUSEADDR true;
      if reuse_port then
        Unix.setsockopt sock_unix Unix.SO_REUSEPORT true;
      Unix.bind sock_unix addr
    | `UdpV4 | `UdpV6 -> ()
    end;
    udp_socket sock

  method getaddrinfo = Low_level.getaddrinfo

  method getnameinfo = Eio_unix.getnameinfo
end

type stdenv = <
  stdin  : source;
  stdout : sink;
  stderr : sink;
  net : Eio.Net.t;
  domain_mgr : Eio.Domain_manager.t;
  clock : Eio.Time.clock;
  mono_clock : Eio.Time.Mono.t;
  fs : Eio.Fs.dir Eio.Path.t;
  cwd : Eio.Fs.dir Eio.Path.t;
  secure_random : Eio.Flow.source;
  debug : Eio.Debug.t;
>

let domain_mgr ~run_event_loop = object
  inherit Eio.Domain_manager.t

  method run_raw fn =
    let domain = ref None in
    enter (fun t k ->
        domain := Some (Domain.spawn (fun () -> Fun.protect fn ~finally:(fun () -> enqueue_thread t k ())))
      );
    Domain.join (Option.get !domain)

  method run fn =
    let domain = ref None in
    enter (fun t k ->
        let cancelled, set_cancelled = Promise.create () in
        Fiber_context.set_cancel_fn k.fiber (Promise.resolve set_cancelled);
        domain := Some (Domain.spawn (fun () ->
            Fun.protect
              (fun () ->
                 let result = ref None in
                 run_event_loop (fun _ -> result := Some (fn ~cancelled));
                 Option.get !result
              )
              ~finally:(fun () -> enqueue_thread t k ())))
      );
    Domain.join (Option.get !domain)
end

let mono_clock = object
  inherit Eio.Time.Mono.t

  method now = Mtime_clock.now ()

  method sleep_until = Low_level.sleep_until
end

let clock = object
  inherit Eio.Time.clock

  method now = Unix.gettimeofday ()

  method sleep_until time =
    (* todo: use the realtime clock directly instead of converting to monotonic time.
       That is needed to handle adjustments to the system clock correctly. *)
    let d = time -. Unix.gettimeofday () in
    Eio.Time.Mono.sleep mono_clock d
end

class dir ~label (fd : dir_fd) = object
  inherit Eio.Fs.dir

  method! probe : type a. a Eio.Generic.ty -> a option = function
    | Dir_fd -> Some fd
    | _ -> None

  method open_in ~sw path =
    let fd = Low_level.openat ~sw fd path
        ~access:`R
        ~flags:Uring.Open_flags.cloexec
        ~perm:0
    in
    (flow fd :> <Eio.File.ro; Eio.Flow.close>)

  method open_out ~sw ~append ~create path =
    let perm, flags =
      match create with
      | `Never            -> 0,    Uring.Open_flags.empty
      | `If_missing  perm -> perm, Uring.Open_flags.creat
      | `Or_truncate perm -> perm, Uring.Open_flags.(creat + trunc)
      | `Exclusive   perm -> perm, Uring.Open_flags.(creat + excl)
    in
    let flags = if append then Uring.Open_flags.(flags + append) else flags in
    let fd = Low_level.openat ~sw fd path
        ~access:`RW
        ~flags:Uring.Open_flags.(cloexec + flags)
        ~perm
    in
    (flow fd :> <Eio.File.rw; Eio.Flow.close>)

  method open_dir ~sw path =
    let fd = Low_level.openat ~sw ~seekable:false fd path
        ~access:`R
        ~flags:Uring.Open_flags.(cloexec + path + directory)
        ~perm:0
    in
    let label = Filename.basename path in
    (new dir ~label (FD fd) :> <Eio.Fs.dir; Eio.Flow.close>)

  method mkdir ~perm path = Low_level.mkdir_beneath ~perm fd path

  method read_dir path =
    Switch.run @@ fun sw ->
    let fd = Low_level.open_dir ~sw fd path in
    Low_level.read_dir fd

  method close =
    match fd with
    | FD x -> FD.close x
    | Cwd | Fs -> failwith "Can't close non-FD directory!"

  method unlink path = Low_level.unlink ~rmdir:false fd path
  method rmdir path = Low_level.unlink ~rmdir:true fd path

  method rename old_path t2 new_path =
    match get_dir_fd_opt t2 with
    | Some fd2 -> Low_level.rename fd old_path fd2 new_path
    | None -> raise (Unix.Unix_error (Unix.EXDEV, "rename-dst", new_path))

  method pp f = Fmt.string f (String.escaped label)
end

let secure_random = object
  inherit Eio.Flow.source
  method read_into buf = Low_level.getrandom buf; Cstruct.length buf
end

let stdenv ~run_event_loop =
  let of_unix fd = FD.of_unix_no_hook ~seekable:(FD.is_seekable fd) ~close_unix:true fd in
  let stdin = lazy (source (of_unix Unix.stdin)) in
  let stdout = lazy (sink (of_unix Unix.stdout)) in
  let stderr = lazy (sink (of_unix Unix.stderr)) in
  let fs = (new dir ~label:"fs" Fs, ".") in
  let cwd = (new dir ~label:"cwd" Cwd, ".") in
  object (_ : stdenv)
    method stdin  = Lazy.force stdin
    method stdout = Lazy.force stdout
    method stderr = Lazy.force stderr
    method net = net
    method domain_mgr = domain_mgr ~run_event_loop
    method clock = clock
    method mono_clock = mono_clock
    method fs = (fs :> Eio.Fs.dir Eio.Path.t)
    method cwd = (cwd :> Eio.Fs.dir Eio.Path.t)
    method secure_random = secure_random
    method debug = Eio.Private.Debug.v
  end

let monitor_event_fd t =
  let buf = Cstruct.create 8 in
  while true do
    let got = Low_level.readv t.eventfd [buf] in
    assert (got = 8);
    (* We just go back to sleep now, but this will cause the scheduler to look
       at the run queue again and notice any new items. *)
  done;
  assert false

let no_fallback (`Msg msg) = failwith msg

(* Don't use [Fun.protect] - it throws away the original exception! *)
let with_uring ~queue_depth ?polling_timeout ?(fallback=no_fallback) fn =
  match Uring.create ~queue_depth ?polling_timeout () with
  | exception Unix.Unix_error(Unix.ENOSYS, _, _) -> fallback (`Msg "io_uring is not available on this system")
  | uring ->
    match fn uring with
    | x -> Uring.exit uring; x
    | exception ex ->
      let bt = Printexc.get_raw_backtrace () in
      begin
        try Uring.exit uring
        with ex2 -> Log.warn (fun f -> f "Uring.exit failed (%a) while handling another error" Fmt.exn ex2)
      end;
      Printexc.raise_with_backtrace ex bt

let rec run : type a.
  ?queue_depth:int -> ?n_blocks:int -> ?block_size:int -> ?polling_timeout:int -> ?fallback:(_ -> a) -> (_ -> a) -> a =
  fun ?(queue_depth=64) ?n_blocks ?(block_size=4096) ?polling_timeout ?fallback main ->
  let n_blocks = Option.value n_blocks ~default:queue_depth in
  let stdenv = stdenv ~run_event_loop:(run ~queue_depth ~n_blocks ~block_size ?polling_timeout ?fallback:None) in
  (* TODO unify this allocation API around baregion/uring *)
  with_uring ~queue_depth ?polling_timeout ?fallback @@ fun uring ->
  let mem =
    let fixed_buf_len = block_size * n_blocks in
    let buf = Bigarray.(Array1.create char c_layout fixed_buf_len) in
    match Uring.set_fixed_buffer uring buf with
    | Ok () ->
      Some (Uring.Region.init ~block_size buf n_blocks)
    | Error `ENOMEM ->
      Log.warn (fun f -> f "Failed to allocate %d byte fixed buffer" fixed_buf_len);
      None
  in
  let run_q = Lf_queue.create () in
  Lf_queue.push run_q IO;
  let sleep_q = Zzz.create () in
  let io_q = Queue.create () in
  let mem_q = Queue.create () in
  let eventfd = FD.of_unix_no_hook ~seekable:false ~close_unix:true (eio_eventfd 0) in
  let st = { mem; uring; run_q; io_q; mem_q; eventfd; need_wakeup = Atomic.make false; sleep_q } in
  let rec fork ~new_fiber:fiber fn =
    let open Effect.Deep in
    Ctf.note_switch (Fiber_context.tid fiber);
    match_with fn ()
      { retc = (fun () -> Fiber_context.destroy fiber; schedule st);
        exnc = (fun ex ->
            Fiber_context.destroy fiber;
            Printexc.raise_with_backtrace ex (Printexc.get_raw_backtrace ())
          );
        effc = fun (type a) (e : a Effect.t) ->
          match e with
          | Enter fn -> Some (fun k ->
              match Fiber_context.get_error fiber with
              | Some e -> discontinue k e
              | None ->
                let k = { Suspended.k; fiber } in
                fn st k;
                schedule st
            )
          | Low_level.ERead args -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              enqueue_read st k args;
              schedule st)
          | Cancel job -> Some (fun k ->
              enqueue_cancel job st;
              continue k ()
            )
          | Low_level.EWrite args -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              enqueue_write st k args;
              schedule st
            )
          | Low_level.Sleep_until time -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              match Fiber_context.get_error fiber with
              | Some ex -> Suspended.discontinue k ex
              | None ->
                let job = Zzz.add sleep_q time k in
                Fiber_context.set_cancel_fn fiber (fun ex ->
                    Zzz.remove sleep_q job;
                    enqueue_failed_thread st k ex
                  );
                schedule st
            )
          | Eio.Private.Effects.Get_context -> Some (fun k -> continue k fiber)
          | Eio.Private.Effects.Suspend f -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              f fiber (function
                  | Ok v -> enqueue_thread st k v
                  | Error ex -> enqueue_failed_thread st k ex
                );
              schedule st
            )
          | Eio.Private.Effects.Fork (new_fiber, f) -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              enqueue_at_head st k ();
              fork ~new_fiber f
            )
          | Eio_unix.Private.Await_readable fd -> Some (fun k ->
              match Fiber_context.get_error fiber with
              | Some e -> discontinue k e
              | None ->
                let k = { Suspended.k; fiber } in
                enqueue_poll_add_unix fd Uring.Poll_mask.(pollin + pollerr) st k (fun res ->
                    if res >= 0 then Suspended.continue k ()
                    else Suspended.discontinue k (Unix.Unix_error (Uring.error_of_errno res, "await_readable", ""))
                  );
                schedule st
            )
          | Eio_unix.Private.Await_writable fd -> Some (fun k ->
              match Fiber_context.get_error fiber with
              | Some e -> discontinue k e
              | None ->
                let k = { Suspended.k; fiber } in
                enqueue_poll_add_unix fd Uring.Poll_mask.(pollout + pollerr) st k (fun res ->
                    if res >= 0 then Suspended.continue k ()
                    else Suspended.discontinue k (Unix.Unix_error (Uring.error_of_errno res, "await_writable", ""))
                  );
                schedule st
            )
          | Eio_unix.Private.Get_monotonic_clock -> Some (fun k -> continue k mono_clock)
          | Eio_unix.Private.Socket_of_fd (sw, close_unix, fd) -> Some (fun k ->
              let fd = FD.of_unix ~sw ~seekable:false ~close_unix fd in
              continue k (flow fd :> Eio_unix.socket)
            )
          | Eio_unix.Private.Socketpair (sw, domain, ty, protocol) -> Some (fun k ->
              let a, b = Unix.socketpair ~cloexec:true domain ty protocol in
              let a = FD.of_unix ~sw ~seekable:false ~close_unix:true a |> flow in
              let b = FD.of_unix ~sw ~seekable:false ~close_unix:true b |> flow in
              continue k ((a :> Eio_unix.socket), (b :> Eio_unix.socket))
            )
          | Eio_unix.Private.Pipe sw -> Some (fun k ->
              let r, w = Unix.pipe ~cloexec:true () in
              (* See issue #319, PR #327 *)
              Unix.set_nonblock r;
              Unix.set_nonblock w;
              let r = (flow (FD.of_unix ~sw ~seekable:false ~close_unix:true r) :> <Eio.Flow.source; Eio.Flow.close; Eio_unix.unix_fd>) in
              let w = (flow (FD.of_unix ~sw ~seekable:false ~close_unix:true w) :> <Eio.Flow.sink; Eio.Flow.close; Eio_unix.unix_fd>) in
              continue k (r, w)
            )
          | Low_level.Alloc -> Some (fun k ->
              match st.mem with
              | None -> continue k None
              | Some mem ->
                match Uring.Region.alloc mem with
                | buf -> continue k (Some buf)
                | exception Uring.Region.No_space -> continue k None
            )
          | Low_level.Alloc_or_wait -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              Low_level.alloc_buf_or_wait st k
            )
          | Low_level.Free buf -> Some (fun k ->
              Low_level.free_buf st buf;
              continue k ()
            )
          | _ -> None
      }
  in
  let result = ref None in
  let `Exit_scheduler =
    let new_fiber = Fiber_context.make_root () in
    fork ~new_fiber (fun () ->
        Switch.run_protected (fun sw ->
            Switch.on_release sw (fun () ->
                FD.close st.eventfd
              );
            result := Some (
                Fiber.first
                  (fun () -> main stdenv)
                  (fun () -> monitor_event_fd st)
              )
          )
      )
  in
  Option.get !result

let run ?queue_depth ?n_blocks ?block_size ?polling_timeout ?fallback main =
  (* SIGPIPE makes no sense in a modern application. *)
  Sys.(set_signal sigpipe Signal_ignore);
  run ?queue_depth ?n_blocks ?block_size ?polling_timeout ?fallback main
