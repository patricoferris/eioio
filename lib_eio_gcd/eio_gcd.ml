(*
 * Copyright (C) 2021 Thomas Leonard
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

(* The goal is also to have no dependency on Unix whatsoever... *)

let src = Logs.Src.create "eio_gcd" ~doc:"Eio backend using Grand Central Dispatch"
module Log = (val Logs.src_log src : Logs.LOG)

open Eio.Std

type 'a or_error = ('a, [`Msg of string]) result

let or_raise = function
  | Ok x -> x
  | Error (`Msg m) -> raise (Failure m)

module Suspended = struct
  type 'a t = {
    tid : Ctf.id;
    k : ('a, unit) continuation;
  }

  let continue t v =
    Ctf.note_switch t.tid;
    continue t.k v

  let discontinue t ex =
    Ctf.note_switch t.tid;
    discontinue t.k ex

  let continue_result t = function
    | Ok x -> continue t x
    | Error x -> discontinue t x
end

effect Await : (('a -> unit) -> unit) -> 'a
let await fn = perform (Await fn)

effect Enter : ('a Suspended.t -> unit) -> 'a
let enter fn = perform (Enter fn)

let await_exn fn =
  perform (Await fn) |> or_raise

let enqueue_thread k v = Suspended.continue k v

let enqueue_result_thread k r = Suspended.continue_result k r

let enqueue_failed_thread k ex = Suspended.discontinue k ex

let yield ?sw () =
  Option.iter Switch.check sw;
  enter @@ fun k ->
  enqueue_thread k ()

module Handle = struct
  type 'a t = {
    mutable release_hook : Switch.hook;        (* Use this on close to remove switch's [on_release] hook. *)
    mutable fd : [`Open of Dispatch.Io.t | `Closed]
  }

  let get op = function
    | { fd = `Open fd; _ } -> fd
    | { fd = `Closed ; _ } -> invalid_arg (op ^ ": handle used after calling close!")

  let is_open = function
    | { fd = `Open _; _ } -> true
    | { fd = `Closed; _ } -> false

  let close t =
    Ctf.label "close";
    let fd = get "close" t in
    t.fd <- `Closed;
    Switch.remove_hook t.release_hook;
    enter @@ fun k ->
    Dispatch.Io.close fd 

  let ensure_closed t =
    if is_open t then close t
end

module File = struct
  type t = {
    mutable release_hook : Switch.hook;        (* Use this on close to remove switch's [on_release] hook. *)
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
    Switch.remove_hook t.release_hook;
    Dispatch.Io.close fd

  let ensure_closed t =
    if is_open t then close t

  let to_luv = get "to_gcd"

  let of_luv_no_hook fd =
    { fd = `Open fd; release_hook = Switch.null_hook }

  let of_luv ~sw fd =
    let t = of_luv_no_hook fd in
    t.release_hook <- Switch.on_release_cancellable sw (fun () -> ensure_closed t);
    t

  let open_ typ path q =
    Dispatch.Io.create typ path q

  let read ?sw fd bufs =
    await (Dispatch.Io.with_read (get "read" fd) bufs)

  let rec write ?sw fd bufs =
    let request = Luv.File.Request.make () in
    with_cancel ?sw ~request @@ fun () ->
    let sent = await_exn (Luv.File.write ~request (get "write" fd) bufs) in
    let rec aux = function
      | [] -> ()
      | x :: xs when Luv.Buffer.size x = 0 -> aux xs
      | bufs -> write ?sw fd bufs
    in
    aux @@ Luv.Buffer.drop bufs (Unsigned.Size_t.to_int sent)
end

let run_compute fn =
  match fn () with
  | x -> x
  | effect Eio.Private.Effects.Trace k -> continue k Eunix.Trace.default_traceln

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

    method read_into ?sw buf =
      let buf = Cstruct.to_bigarray buf in
      match File.read ?sw fd [buf] |> or_raise |> Unsigned.Size_t.to_int with
      | 0 -> raise End_of_file
      | got -> got

    method read_methods = []

    method write ?sw src =
      let buf = Luv.Buffer.create 4096 in
      try
        while true do
          let got = Eio.Flow.read_into src (Cstruct.of_bigarray buf) in
          let sub = Luv.Buffer.sub buf ~offset:0 ~length:got in
          File.write ?sw fd [sub]
        done
      with End_of_file -> ()
  end

  let source fd = (flow fd :> source)
  let sink   fd = (flow fd :> sink)

  let socket sock = object
    inherit Eio.Flow.two_way

    method read_into ?sw buf =
      let buf = Cstruct.to_bigarray buf in
      Stream.read_into ?sw sock buf

    method read_methods = []

    method write ?sw src =
      let buf = Luv.Buffer.create 4096 in
      try
        while true do
          let got = Eio.Flow.read_into src (Cstruct.of_bigarray buf) in
          let buf' = Luv.Buffer.sub buf ~offset:0 ~length:got in
          Stream.write ?sw sock [buf']
        done
      with End_of_file -> ()

    method close =
      Handle.close sock

    method shutdown = function
      | `Send -> await_exn @@ Luv.Stream.shutdown (Handle.get "shutdown" sock)
      | `Receive -> failwith "shutdown receive not supported"
      | `All ->
        Log.warn (fun f -> f "shutdown receive not supported");
        await_exn @@ Luv.Stream.shutdown (Handle.get "shutdown" sock)
  end

  class virtual ['a] listening_socket ~backlog sock = object (self)
    inherit Eio.Net.listening_socket

    val ready = Eio.Semaphore.make 0

    method private virtual make_client : 'a Luv.Stream.t
    method private virtual get_client_addr : 'a Stream.t -> Eio.Net.Sockaddr.t

    method close = Handle.close sock

    method accept_sub ~sw ~on_error fn =
      Eio.Semaphore.acquire ~sw ready;
      let client = self#make_client |> Handle.of_luv_no_hook in
      match Luv.Stream.accept ~server:(Handle.get "accept" sock) ~client:(Handle.get "accept" client) with
      | Error e ->
        Handle.close client;
        raise (Luv_error e)
      | Ok () ->
        Fibre.fork_sub_ignore ~sw ~on_error
          (fun sw ->
             let client_addr = self#get_client_addr client in
             fn ~sw (socket client :> <Eio.Flow.two_way; Eio.Flow.close>) client_addr
          )
          ~on_release:(fun () -> Handle.ensure_closed client)

    initializer
      Luv.Stream.listen ~backlog (Handle.get "listen" sock) (fun x ->
          or_raise x;
          Eio.Semaphore.release ready
        )
  end

  type stdenv = <
    stdin : source;
    stdout : sink;
    stderr : sink;
    net : Eio.Net.t;
    domain_mgr : Eio.Domain_manager.t;
    clock : Eio.Time.clock;
    fs : Eio.Dir.t;
    cwd : Eio.Dir.t;
  >

  let domain_mgr = object
    inherit Eio.Domain_manager.t

    method run_compute_unsafe (type a) fn =
      let domain_k : (unit Domain.t * a Suspended.t) option ref = ref None in
      let result = ref None in
      let async = Luv.Async.init (fun async ->
          (* This is called in the parent domain after returning to the mainloop,
             so [domain_k] must be set by then. *)
          let domain, k = Option.get !domain_k in
          Domain.join domain;
          Luv.Handle.close async @@ fun () ->
          Suspended.continue_result k (Option.get !result)
        ) |> or_raise
      in
      enter @@ fun k ->
      let d = Domain.spawn (fun () ->
          result := Some (match run_compute fn with
              | v -> Ok v
              | exception ex -> Error ex
            );
          Luv.Async.send async |> or_raise
        ) in
      domain_k := Some (d, k)
  end

  let clock = object
    inherit Eio.Time.clock

    method now = Unix.gettimeofday ()
    method sleep_until = sleep_until
  end

  (* Full access to the filesystem. *)
  let fs = object
    inherit dir "/"

    (* No checks *)
    method! private resolve ?sw:_ path = path
  end

  let cwd = object
    inherit dir "."
  end

  let stdenv () =
    let stdin = lazy (source (File.of_luv_no_hook Luv.File.stdin)) in
    let stdout = lazy (sink (File.of_luv_no_hook Luv.File.stdout)) in
    let stderr = lazy (sink (File.of_luv_no_hook Luv.File.stderr)) in
    object (_ : stdenv)
      method stdin  = Lazy.force stdin
      method stdout = Lazy.force stdout
      method stderr = Lazy.force stderr
      method net = net
      method domain_mgr = domain_mgr
      method clock = clock
      method fs = (fs :> Eio.Dir.t)
      method cwd = (cwd :> Eio.Dir.t)
    end
end  

let run main =
  Log.debug (fun l -> l "starting run");
  let stdenv = Objects.stdenv () in
  let rec fork ~tid fn =
    Ctf.note_switch tid;
    match fn () with
    | () -> ()
    | effect (Await fn) k ->
      let k = { Suspended.k; tid } in
      fn (Suspended.continue k)
    | effect Eio.Private.Effects.Trace k -> continue k Eunix.Trace.default_traceln
    | effect (Eio.Private.Effects.Fork f) k ->
      let k = { Suspended.k; tid } in
      let id = Ctf.mint_id () in
      Ctf.note_created id Ctf.Task;
      let promise, resolver = Promise.create_with_id id in
      enqueue_thread k promise;
      fork
        ~tid:id
        (fun () ->
           match f () with
           | x -> Promise.fulfill resolver x
           | exception ex ->
             Log.debug (fun f -> f "Forked fibre failed: %a" Fmt.exn ex);
             Promise.break resolver ex
        )
    | effect (Eio.Private.Effects.Fork_ignore f) k ->
      let k = { Suspended.k; tid } in
      enqueue_thread k ();
      let child = Ctf.note_fork () in
      Ctf.note_switch child;
      fork ~tid:child (fun () ->
          match f () with
          | () ->
            Ctf.note_resolved child ~ex:None
          | exception ex ->
            Ctf.note_resolved child ~ex:(Some ex)
        )
    | effect (Enter fn) k -> fn { Suspended.k; tid }
    | effect (Eio.Private.Effects.Suspend fn) k ->
      let k = { Suspended.k; tid } in
      fn tid (enqueue_result_thread k)
  in
  let main_status = ref `Running in
  fork ~tid:(Ctf.mint_id ()) (fun () ->
      match main stdenv with
      | () -> main_status := `Done
      | exception ex -> main_status := `Ex (ex, Printexc.get_raw_backtrace ())
    );
  ignore (Luv.Loop.run () : bool);
  match !main_status with
  | `Done -> ()
  | `Ex (ex, bt) -> Printexc.raise_with_backtrace ex bt
  | `Running -> failwith "Deadlock detected: no events scheduled but main function hasn't returned"
