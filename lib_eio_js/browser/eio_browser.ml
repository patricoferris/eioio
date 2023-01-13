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
module Lf_queue = Eio_utils.Lf_queue

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

  let _discontinue t ex =
    Ctf.note_switch (tid t);
    Effect.Deep.discontinue t.k ex
end

type t = {
  (* Suspended fibers waiting to run again.
     [Lf_queue] is like [Stdlib.Queue], but is thread-safe (lock-free) and
     allows pushing items to the head too, which we need. *)
  mutable run_q : (unit -> unit) Lf_queue.t;
  mutable pending_io : int;
  mutable timeout : int option;  (* ID for the setTimeout *)
}

let enqueue_thread t k v =
  Lf_queue.push t.run_q (fun () -> Suspended.continue k v)

type _ Effect.t += Enter_unchecked : (t -> 'a Suspended.t -> unit) -> 'a Effect.t
let enter_unchecked fn = Effect.perform (Enter_unchecked fn)

let enter_io fn =
  enter_unchecked @@ fun st k ->
  st.pending_io <- st.pending_io + 1;
  fn (fun res -> enqueue_thread st k res; st.pending_io <- st.pending_io - 1)

(* Resume the next runnable fiber, if any. *)
let rec schedule t : unit =
  match Lf_queue.pop t.run_q with
  | Some f -> f ()
  | None ->
    if t.pending_io = 0 then begin
      Option.iter G.stop_timer t.timeout;
      t.timeout <- None
    end
    else begin
      match t.timeout with
      | None ->
        let id = G.set_timeout ~ms:0 (fun () -> t.timeout <- None; schedule t) in
        t.timeout <- Some id;
        schedule t
      | Some _id -> ()
    end

module Timeout = struct
  let set_timeout ~ms f = ignore (G.set_timeout ~ms f)

  let sleep ~ms =
    enter_io @@ set_timeout ~ms
end

let await fut =
  enter_io @@ Fut.await fut

(* Largely based on the Eio_mock.Backend event loop. *)
let run main =
  let run_q = Lf_queue.create () in
  let t = { run_q; pending_io = 0; timeout = None } in
  let rec fork ~new_fiber:fiber fn =
    Effect.Deep.match_with fn ()
      { retc = (fun () -> Fiber_context.destroy fiber; schedule t);
        exnc = (fun ex ->
            let bt = Printexc.get_raw_backtrace () in
            Fiber_context.destroy fiber;
            Printexc.raise_with_backtrace ex bt
          );
        effc = fun (type a) (e : a Effect.t) : ((a, unit) Effect.Deep.continuation -> unit) option ->
          match e with
          | Eio.Private.Effects.Suspend f -> Some (fun k ->
              f fiber (function
                  | Ok v -> Lf_queue.push t.run_q (fun () -> Effect.Deep.continue k v)
                  | Error ex -> Lf_queue.push t.run_q (fun () -> Effect.Deep.discontinue k ex)
                );
              schedule t
            )
          | Enter_unchecked fn -> Some (fun k ->
              fn t { Suspended.k; fiber };
              schedule t
            )
          | Eio.Private.Effects.Fork (new_fiber, f) -> Some (fun k ->
              Lf_queue.push_head t.run_q (Effect.Deep.continue k);
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
