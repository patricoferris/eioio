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

 let src = Logs.Src.create "eio_node" ~doc:"Eio backend using node.js"
 module Log = (val Logs.src_log src : Logs.LOG)

 module Node = Node
 
 open Eio.Std
 
 module Ctf = Eio.Private.Ctf
 
 module Fiber_context = Eio.Private.Fiber_context
 module Lf_queue = Eio_utils.Lf_queue
 
 (* SIGPIPE makes no sense in a modern application. *)
 let () = Sys.(set_signal sigpipe Signal_ignore)

 
 let () =
   Printexc.register_printer @@ function
   | Node.Node_error e -> Some (Printf.sprintf "Eio_node.Node_error(%s)" e)
   | _ -> None
 
 let wrap_error ~path:_ e =
   let ex = Node.Node_error (Node.Error.message e) in
   match e with
   (* | `EEXIST -> Eio.Fs.Already_exists (path, ex)
   | `ENOENT -> Eio.Fs.Not_found (path, ex) *)
   | _ -> ex
 
 let wrap_flow_error e =
   let ex = Node.Node_error e in
   match e with
   (* | `ECONNRESET
   | `EPIPE -> Eio.Net.Connection_reset ex *)
   | _ -> ex
 
 let or_raise = function
   | Ok x -> x
   | Error e -> raise (Node.Node_error (Node.Error.message e))
 
 let or_raise_path path = function
   | Ok x -> x
   | Error e -> raise (wrap_error ~path e)
 
 let max_luv_buffer_size = 0x7fffffff
 
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
 
   let continue_result t = function
     | Ok x -> continue t x
     | Error x -> discontinue t x
 end
 
 type runnable =
   | IO
   | Thread of (unit -> unit)
 
 type fd_event_waiters = {
   fd : Unix.file_descr;
   read : unit Suspended.t Lwt_dllist.t;
   write : unit Suspended.t Lwt_dllist.t;
 }
 
 module Fd_map = Map.Make(struct type t = Unix.file_descr let compare = Stdlib.compare end)
 
 type t = {
   async : Node.Async.t;                          (* Will process [run_q] when prodded. *)
   run_q : runnable Lf_queue.t;
   mutable fd_map : fd_event_waiters Fd_map.t;   (* Used for mapping readable/writable poll handles *)
 }
 
 type _ Effect.t += Await : (Eio.Private.Fiber_context.t -> ('a -> unit) -> unit) -> 'a Effect.t
 
 type _ Effect.t += Enter : (t -> 'a Suspended.t -> unit) -> 'a Effect.t
 type _ Effect.t += Enter_unchecked : (t -> 'a Suspended.t -> unit) -> 'a Effect.t
 
 let enter fn = Effect.perform (Enter fn)
 let enter_unchecked fn = Effect.perform (Enter_unchecked fn)
 
 let enqueue_thread t k v =
   Lf_queue.push t.run_q (Thread (fun () -> Suspended.continue k v));
   Node.Async.send t.async
 
 let enqueue_result_thread t k r =
   Lf_queue.push t.run_q (Thread (fun () -> Suspended.continue_result k r));
   Node.Async.send t.async
 
 let enqueue_failed_thread t k ex =
   Lf_queue.push t.run_q (Thread (fun () -> Suspended.discontinue k ex));
   Node.Async.send t.async
 
 (* Can only be called from our domain. *)
 let enqueue_at_head t k v =
   Lf_queue.push_head t.run_q (Thread (fun () -> Suspended.continue k v));
   Node.Async.send t.async
  
 module Low_level = struct
   type 'a or_error = ('a, Node.Error.t) result
 
   exception Node_error = Node.Node_error
   let or_raise = or_raise
 
   let await fn =
     Effect.perform (Await fn)
 
   let await_exn fn =
     Effect.perform (Await fn) |> or_raise
 
   let await_with_cancel fn =
     enter (fun st k ->
         let cancel_reason = ref None in
         Eio.Private.Fiber_context.set_cancel_fn k.fiber (fun ex ->
             cancel_reason := Some ex;
             (* match Luv.Request.cancel request with
             | Ok () -> ()
             | Error e -> Log.debug (fun f -> f "Cancel failed: %s" (Luv.Error.strerror e)) *)
           );
         fn (fun v ->
             if Eio.Private.Fiber_context.clear_cancel_fn k.fiber then (
               enqueue_thread st k v
             ) else (
               (* Cancellations always come from the same domain, so we can be sure
                  that [cancel_reason] is set by now. *)
               enqueue_failed_thread st k (Option.get !cancel_reason)
             )
           )
       )
 
   module File = struct
     type t = {
       mutable release_hook : Eio.Switch.hook;        (* Use this on close to remove switch's [on_release] hook. *)
       close_unix : bool;
       mutable fd : [`Open of Node.Fs.fd | `Closed]
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
       enter_unchecked (fun st k ->
          Node.Fs.close fd (enqueue_thread st k)
         ) |> or_raise
 
     let ensure_closed t =
       if is_open t then close t
 
     let to_luv = get "to_luv"
 
     let of_luv_no_hook ~close_unix fd =
       { fd = `Open fd; release_hook = Eio.Switch.null_hook; close_unix }
 
     let of_luv ?(close_unix=true) ~sw fd =
       let t = of_luv_no_hook ~close_unix fd in
       t.release_hook <- Switch.on_release_cancellable sw (fun () -> ensure_closed t);
       t
 
     let open_ ~sw ?mode path flags =
       Printf.printf "OpEn";
       await (fun _ctx -> Node.Fs.open_ ?mode ~flags path)
       |> Result.map (of_luv ~sw)
 
     let read ?file_offset fd bufs =
      await (fun _ctx -> Node.Fs.read ?off:file_offset (get "read" fd) bufs)
 
     let write_single ?file_offset fd bufs =
        await (fun _ctx -> Node.Fs.writev ?off:file_offset (get "write" fd) bufs)

    let drop ts len =
      let rec aux xs n = match xs, n with
        | xs, 0 -> xs
        | x :: xs, n ->
          let size = Brr.Tarray.length x in
          if n >= size then aux xs (n - size) else
          x :: xs
        | _ -> failwith "Err"
      in
        aux ts len
 
     let rec write fd bufs =
       match write_single fd bufs with
       | Error _ as e -> e
       | Ok sent ->
         let rec aux = function
           | [] -> Ok ()
           | x :: xs when Brr.Tarray.length x = 0 -> aux xs
           | bufs -> write fd bufs
         in
         aux @@ drop bufs sent 

  let realpath path = await (fun _ctx -> Node.Fs.realpath path)

  let mkdir ~mode path = await (fun _ctx -> Node.Fs.mkdir ~mode path)

  let unlink path = await (fun _ctx -> Node.Fs.unlink path)

  let rmdir path = await (fun _ctx -> Node.Fs.rmdir path)

  let rename old_path new_path = await (fun _ctx -> Node.Fs.rename old_path new_path)

  let readdir dir = await (fun _ctx -> Node.Fs.readdir dir)
   end
end
 
module File = Low_level.File
 
open Low_level
 
type _ Eio.Generic.ty += FD : Low_level.File.t Eio.Generic.ty

type has_fd = < fd : File.t >
type source = < Eio.Flow.source; Eio.Flow.close; has_fd >
type sink   = < Eio.Flow.sink  ; Eio.Flow.close; has_fd >

let get_fd (t : <has_fd; ..>) = t#fd

let get_fd_opt t = Eio.Generic.probe t FD

let cstruct_to_luv_truncate buf =
  Cstruct.to_bigarray @@
  if Cstruct.length buf <= max_luv_buffer_size then buf
  else Cstruct.sub buf 0 max_luv_buffer_size

let flow fd = object (_ : <source; sink; ..>)
  method fd = fd
  method close = Low_level.File.close fd
  (* method unix_fd op = File.to_unix op fd *)

  method probe : type a. a Eio.Generic.ty -> a option = function
    | FD -> Some fd
    | _ -> None

  method read_into buf =
    match File.read fd (Node.Buffer.of_cstruct buf) |> or_raise with
    | (0, _) -> raise End_of_file
    | (got, _) -> got

  method pread ~file_offset bufs =
    let bufs = List.hd bufs in (* HACK TODO: fix and add proper pread *) 
    let file_offset = Optint.Int63.to_int file_offset in
    match File.read ~file_offset fd (Node.Buffer.of_cstruct bufs) |> or_raise with
    | (0, _) -> raise End_of_file
    | (got, _) -> got

  method pwrite ~file_offset bufs =
    let bufs = List.map (fun v -> Brr.Tarray.of_bigarray1 (Cstruct.to_bigarray v)) bufs in
    let file_offset = Optint.Int63.to_int file_offset in
    File.write_single ~file_offset fd bufs |> or_raise

  method read_methods = []

  method write bufs =
    let bufs = List.map (fun v -> Brr.Tarray.of_bigarray1 (Cstruct.to_bigarray v)) bufs in
    File.write fd bufs |> or_raise

  method copy src =
  let buf = Cstruct.create 4096 in
  try
      while true do
      let got = Eio.Flow.single_read src buf in
      let sub = Cstruct.sub buf 0 got in
      File.write fd [ Brr.Tarray.of_bigarray1 (Cstruct.to_bigarray sub) ] |> or_raise
      done
    with End_of_file -> ()
end

let source fd = (flow fd :> source)
let sink   fd = (flow fd :> sink)

type _ Eio.Generic.ty += Dir_resolve_new : (string -> string) Eio.Generic.ty
let dir_resolve_new x = Eio.Generic.probe x Dir_resolve_new

(* Warning: libuv doesn't provide [openat], etc, and so there is probably no way to make this safe.
  We make a best-efforts attempt to enforce the sandboxing using realpath and [`NOFOLLOW].
  todo: this needs more testing *)
class dir ~label (dir_path : string) = object (self)
  inherit Eio.Fs.dir

  val mutable closed = false

  method! probe : type a. a Eio.Generic.ty -> a option = function
    | Dir_resolve_new -> Some self#resolve_new
    | _ -> None

  (* Resolve a relative path to an absolute one, with no symlinks.
    @raise Eio.Fs.Permission_denied if it's outside of [dir_path]. *)
  method private resolve ?display_path path =
    if closed then Fmt.invalid_arg "Attempt to use closed directory %S" dir_path;
    let display_path = Option.value display_path ~default:path in
    if Filename.is_relative path then (
      let dir_path = File.realpath dir_path |> or_raise_path dir_path in
      Printf.printf "OPEN OUT %s\n" dir_path;
      let full = File.realpath (Filename.concat dir_path path) |> or_raise_path path in
      let prefix_len = String.length dir_path + 1 in
      if String.length full >= prefix_len && String.sub full 0 prefix_len = dir_path ^ Filename.dir_sep then
        full
      else if full = dir_path then
        full
      else
        raise (Eio.Fs.Permission_denied (display_path, Failure (Fmt.str "Path %S is outside of sandbox %S" full dir_path)))
    ) else (
      raise (Eio.Fs.Permission_denied (display_path, Failure (Fmt.str "Path %S is absolute" path)))
    )

  (* We want to create [path]. Check that the parent is in the sandbox. *)
  method private resolve_new path =
    let dir, leaf = Filename.dirname path, Filename.basename path in
    if leaf = ".." then Fmt.failwith "New path %S ends in '..'!" path
    else match self#resolve dir with
      | dir -> Filename.concat dir leaf
      | exception Eio.Fs.Not_found (dir, ex) ->
        raise (Eio.Fs.Not_found (Filename.concat dir leaf, ex))
      | exception Eio.Fs.Permission_denied (dir, ex) ->
        raise (Eio.Fs.Permission_denied (Filename.concat dir leaf, ex))

  method open_in ~sw path =
    let fd = File.open_ ~sw (self#resolve path) "r" |> or_raise_path path in
    (flow fd :> <Eio.File.ro; Eio.Flow.close>)

  method open_out ~sw ~append ~create path =
    let mode, flags =
      match create with
      | `Never            -> 0,    []
      | `If_missing  perm -> perm, [`CREAT]
      | `Or_truncate perm -> perm, [`CREAT; `TRUNC]
      | `Exclusive   perm -> perm, [`CREAT; `EXCL]
    in
    let flags = if append then `APPEND :: flags else flags in
    let _flags = `RDWR :: `NOFOLLOW :: flags in
    let real_path =
      if create = `Never then self#resolve path
      else self#resolve_new path
    in
    let fd = File.open_ ~sw real_path "w" ~mode |> or_raise_path path in
    (flow fd :> <Eio.File.rw; Eio.Flow.close>)

  method open_dir ~sw path =
    Switch.check sw;
    let label = Filename.basename path in
    let d = new dir ~label (self#resolve path) in
    Switch.on_release sw (fun () -> d#close);
    d

  (* libuv doesn't seem to provide a race-free way to do this. *)
  method mkdir ~perm path =
    let real_path = self#resolve_new path in
    File.mkdir ~mode:perm real_path |> or_raise_path path

  (* libuv doesn't seem to provide a race-free way to do this. *)
  method unlink path =
    let dir_path = Filename.dirname path in
    let leaf = Filename.basename path in
    let real_dir_path = self#resolve ~display_path:path dir_path in
    File.unlink (Filename.concat real_dir_path leaf) |> or_raise_path path

  (* libuv doesn't seem to provide a race-free way to do this. *)
  method rmdir path =
    let dir_path = Filename.dirname path in
    let leaf = Filename.basename path in
    let real_dir_path = self#resolve ~display_path:path dir_path in
    File.rmdir (Filename.concat real_dir_path leaf) |> or_raise_path path

  method read_dir path =
    let path = self#resolve path in
    File.readdir path |> or_raise_path path

  method rename old_path new_dir new_path =
    match dir_resolve_new new_dir with
    | None -> invalid_arg "Target is not a luv directory!"
    | Some new_resolve_new ->
      let old_path = self#resolve old_path in
      let new_path = new_resolve_new new_path in
      File.rename old_path new_path |> or_raise_path old_path

  method close = closed <- true

  method pp f = Fmt.string f (String.escaped label)
end

(* Full access to the filesystem. *)
let fs = object
  inherit dir ~label:"fs" "."

  (* No checks *)
  method! private resolve ?display_path:_ path = path
end

let cwd = object
  inherit dir ~label:"cwd" "."
end

type stdenv = <
stdin  : source;
stdout : sink;
stderr : sink;
fs : Eio.Fs.dir Eio.Path.t;
cwd : Eio.Fs.dir Eio.Path.t;
>

let stdenv =
  let stdin = lazy (source (File.of_luv_no_hook Node.Process.stdin ~close_unix:true)) in
  let stdout = lazy (sink (File.of_luv_no_hook Node.Process.stdout ~close_unix:true)) in
  let stderr = lazy (sink (File.of_luv_no_hook Node.Process.stderr ~close_unix:true)) in
  object (_ : stdenv)
    method stdin  = Lazy.force stdin
    method stdout = Lazy.force stdout
    method stderr = Lazy.force stderr
    method fs = (fs :> Eio.Fs.dir), "."
    method cwd = (cwd :> Eio.Fs.dir), "."
  end

let rec wakeup ~io_queued run_q =
  match Lf_queue.pop run_q with
  | Some (Thread f) ->
    if not !io_queued then (
      Lf_queue.push run_q IO;
      io_queued := true;
    );
    f ();
    wakeup ~io_queued run_q
  | Some IO ->
    (* If threads keep yielding they could prevent pending IO from being processed.
      Therefore, we keep an [IO] job on the queue to force us to check from time to time. *)
    io_queued := false;
    if not (Lf_queue.is_empty run_q) then
      wakeup ~io_queued run_q
  | None -> ()

let rec run = fun main ->
  let run_q = Lf_queue.create () in
  let io_queued = ref false in
  let async = Node.Async.init (fun () ->
  try wakeup ~io_queued run_q
  with ex ->
    let bt = Printexc.get_raw_backtrace () in
    Fmt.epr "Uncaught exception in run loop:@,%a@." Fmt.exn_backtrace (ex, bt)
  )
  in
  let st = { async; run_q; fd_map = Fd_map.empty } in
  let rec fork ~new_fiber:fiber fn =
    Ctf.note_switch (Fiber_context.tid fiber);
    let open Effect.Deep in
    match_with fn ()
    { retc = (fun () -> Fiber_context.destroy fiber);
      exnc = (fun e -> Fiber_context.destroy fiber; raise e);
      effc = fun (type a) (e : a Effect.t) ->
        match e with
        | Await fn ->
          Some (fun (k : (a, unit) continuation) ->
            let k = { Suspended.k; fiber } in
            fn fiber (enqueue_thread st k)
        )
        | Eio.Private.Effects.Fork (new_fiber, f) ->
          Some (fun k ->
              let k = { Suspended.k; fiber } in
              enqueue_at_head st k ();
              fork ~new_fiber f
            )
        | Eio.Private.Effects.Get_context -> Some (fun k -> continue k fiber)
        | Enter_unchecked fn -> Some (fun k ->
            fn st { Suspended.k; fiber }
          )
        | Enter fn -> Some (fun k ->
            match Fiber_context.get_error fiber with
            | Some e -> discontinue k e
            | None -> 
            fn st { Suspended.k; fiber }
          )
        | Eio.Private.Effects.Suspend fn ->
          Some (fun k ->
              let k = { Suspended.k; fiber } in
              fn fiber (enqueue_result_thread st k)
            )
        | _ -> None
    }
  in
  let new_fiber = Fiber_context.make_root () in
  let main_status, r = Fut.create () in
  let value = ref None in
  fork ~new_fiber (fun () ->
      begin match main stdenv with
        | v -> 
          value := Some v;
          r (Ok `Done);
        | exception ex -> r (Error (ex, Printexc.get_raw_backtrace ()))
      end;
    );
  let timer = Node.prevent_exit () in
  Fut.await main_status (fun v -> 
    Lf_queue.close st.run_q;
    Brr.G.stop_timer timer;
    match v with
      | Ok `Done -> ()
      | Error (ex, bt) ->
        Printexc.raise_with_backtrace ex bt
  )
