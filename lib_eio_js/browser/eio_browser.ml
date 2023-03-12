(*
 * Copyright (C) 2021-2022 Thomas Leonard
 * Copyright (C) 2022      Patrick Ferris
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

open Brr

module Fiber_context = Eio.Private.Fiber_context
module Run_queue : sig
  type 'a t
  (* A queue that supports pushing to the head of the queue *)

  val create : unit -> 'a t

  val is_empty : 'a t -> bool
  (** Check if the queue is empty *)

  val push : 'a t -> 'a -> unit
  (** [push q v] pushes a new item [v] to the back of the queue [q] *)

  val push_head : 'a t -> 'a -> unit
  (** [push_head q v] pushes a new item [v] to the head of the queue [q] *)

  val pop : 'a t -> 'a option
  (** [pop q] pops the next item of the queue if available. *)

end = struct
  type 'a t = 'a Lwt_dllist.t

  let create () = Lwt_dllist.create ()

  let is_empty t = Lwt_dllist.is_empty t

  let push q v = ignore (Lwt_dllist.add_r v q : 'a Lwt_dllist.node)

  let push_head q v = ignore (Lwt_dllist.add_l v q : 'a Lwt_dllist.node)

  let pop q = Lwt_dllist.take_opt_l q
end

module Ctf = Eio.Private.Ctf

module Suspended = struct
  type 'a t = {
    fiber : Eio.Private.Fiber_context.t;
    k : ('a, unit) Effect.Deep.continuation;
  }

  let tid t = Eio.Private.Fiber_context.tid t.fiber

  let continue t v =
    Ctf.note_switch (tid t);
    Effect.Deep.continue t.k v

  let discontinue t ex =
    Ctf.note_switch (tid t);
    Effect.Deep.discontinue t.k ex
end

(* The Javascript backend scheduler is implemented as an event listener.
   We don't need to worry about multiple domains. Here any time something
   asynchronously enqueues a task to our queue, it also sends a wakeup event to
   the event listener which will run the callback calling the scheduler. *)
module Scheduler = struct
  type job =
    | Runnable of (unit -> unit)
    | IO

  type t = {
    run_q : job Run_queue.t;
    mutable io_queued : bool;
  }

  let rec next t = match Run_queue.pop t.run_q with
    | Some (Runnable fn) ->
      if not t.io_queued then begin
        Run_queue.push t.run_q IO;
        t.io_queued <- true
      end;
      fn ()
    | Some IO ->
      (* We now need to stop scheduling and yield to the JS
         event loop to let some of our outstanding continuations
         run. *)
      t.io_queued <- false;
      (* assert (Run_queue.is_empty t.run_q); *)
      let _timeout = G.set_timeout ~ms:0 (fun () -> next t) in
      ()
    | None -> ()

  let v run_q =
    { run_q; io_queued = false }

  let enqueue_thread t k v =
    Run_queue.push t.run_q (Runnable (fun () -> Suspended.continue k v))

  let enqueue_failed_thread t k v =
    Run_queue.push t.run_q (Runnable (fun () -> Suspended.discontinue k v))

  let enqueue_at_head t k v =
    Run_queue.push_head t.run_q (Runnable (fun () -> Suspended.continue k v))
end

type _ Effect.t += Enter_unchecked : (Scheduler.t -> 'a Suspended.t -> unit) -> 'a Effect.t
let enter_unchecked fn = Effect.perform (Enter_unchecked fn)

module Timeout = struct
  let sleep ~ms =
    enter_unchecked @@ fun st k ->
    let id = G.set_timeout ~ms (fun () ->
        Fiber_context.clear_cancel_fn k.fiber;
        Scheduler.enqueue_thread st k ();
        Scheduler.next st
      ) in
    Fiber_context.set_cancel_fn k.fiber (fun exn -> G.stop_timer id; Scheduler.enqueue_failed_thread st k exn; Scheduler.next st);
    Scheduler.next st
end

let await fut =
  enter_unchecked @@ fun st k ->
  (* There is no way to cancel a Javascript promise (which Fut wraps) so we
     have to leak this memory unfortunately. *)
  let cancelled = ref false in
  Fiber_context.set_cancel_fn k.fiber (fun exn -> cancelled := true; Scheduler.enqueue_failed_thread st k exn; Scheduler.next st);
  Fut.await fut (fun v ->
      if not !cancelled then begin
        Fiber_context.clear_cancel_fn k.fiber;
        Scheduler.enqueue_thread st k v;
        Scheduler.next st
      end
    );
  Scheduler.next st

let next_event : 'a Brr.Ev.type' -> Brr.Ev.target -> 'a Brr.Ev.t = fun typ target ->
  let opts = Brr.Ev.listen_opts ~once:true () in
  let listen fn = Brr.Ev.listen ~opts typ fn target in
  enter_unchecked @@ fun st k ->
  (* If there is a cancellation of the call to next_event, then unlisten
     will be called and so enqueue_thread will never be called even
     if another event arrives. *)
  let v = listen (fun v -> Fiber_context.clear_cancel_fn k.fiber; Scheduler.enqueue_thread st k v; Scheduler.next st) in
  Fiber_context.set_cancel_fn k.fiber (fun exn -> Ev.unlisten v; Scheduler.enqueue_failed_thread st k exn; Scheduler.next st);
  Scheduler.next st

(* Largely based on the Eio_mock.Backend event loop. *)
let run main =
  let run_q = Run_queue.create () in
  let scheduler = Scheduler.v run_q in
  Run_queue.push run_q IO;
  let rec fork ~new_fiber:fiber fn =
    Effect.Deep.match_with fn ()
      { retc = (fun () -> Fiber_context.destroy fiber; Scheduler.next scheduler);
        exnc = (fun ex ->
            let bt = Printexc.get_raw_backtrace () in
            Fiber_context.destroy fiber;
            Printexc.raise_with_backtrace ex bt
          );
        effc = fun (type a) (e : a Effect.t) : ((a, unit) Effect.Deep.continuation -> unit) option ->
          match e with
          | Eio.Private.Effects.Suspend f -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              f fiber (function
                  | Ok v -> (
                    Scheduler.enqueue_thread scheduler k v;
                  )
                  | Error ex -> (
                    Scheduler.enqueue_failed_thread scheduler k ex;
                  )
                );
              Scheduler.next scheduler
            )
          | Enter_unchecked fn -> Some (fun k ->
              fn scheduler { Suspended.k; fiber }
            )
          | Eio.Private.Effects.Fork (new_fiber, f) -> Some (fun k ->
              let k = { Suspended.k; fiber } in
              Scheduler.enqueue_at_head scheduler k ();
              fork ~new_fiber f
            )
          | Eio.Private.Effects.Get_context -> Some (fun k ->
              Effect.Deep.continue k fiber
            )
          | _ -> None
      }
  in
  let new_fiber = Fiber_context.make_root () in
  let result, r = Fut.create () in
  let () = fork ~new_fiber (fun () -> r (main ())) in
  result
