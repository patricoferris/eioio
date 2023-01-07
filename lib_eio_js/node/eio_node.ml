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

type Eio.Exn.Backend.t +=
  | Node_error of Node.Error.t
  | Outside_sandbox of string * string
  | Absolute_path

let () =
  Eio.Exn.Backend.register_pp @@ fun f -> function
  | Node_error e -> Fmt.pf f "Eio_node.Node_error(%s) (* %s *)" (Node.Error.code e) (Node.Error.message e); true
  | Outside_sandbox (path, dir) -> Fmt.pf f "Outside_sandbox (%S, %S)" path dir; true
  | Absolute_path -> Fmt.pf f "Absolute_path"; true
  | _ -> false

let unclassified_error e = Eio.Exn.create (Eio.Exn.X e)

open Eio.Std

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

module Fiber_context = Eio.Private.Fiber_context
module Lf_queue = Eio_utils.Lf_queue

(* SIGPIPE makes no sense in a modern application. *)
let () = Sys.(set_signal sigpipe Signal_ignore)

let or_raise = function
  | Ok x -> x
  | Error e -> raise (Node.Node_error (Node.Error.message e))

type runnable =
  | Thread of (unit -> unit)

type fd_event_waiters = {
  fd : Unix.file_descr;
  read : unit Suspended.t Lwt_dllist.t;
  write : unit Suspended.t Lwt_dllist.t;
}

module Fd_map = Map.Make(struct type t = Unix.file_descr let compare = Stdlib.compare end)

type t = {
  run_q : runnable Lf_queue.t;
  mutable imm : Jv.t option;
  mutable pending_io : int;
  mutable fd_map : fd_event_waiters Fd_map.t;   (* Used for mapping readable/writable poll handles *)
}

type _ Effect.t += Enter_unchecked : (t -> 'a Suspended.t -> unit) -> 'a Effect.t
let enter_unchecked fn = Effect.perform (Enter_unchecked fn)

let enqueue_thread t k v =
  Lf_queue.push t.run_q (Thread (fun () -> Suspended.continue k v))

let enqueue_failed_thread t k ex =
  Lf_queue.push t.run_q (Thread (fun () -> Suspended.discontinue k ex))

let enqueue_result t k = function
  | Ok v -> enqueue_thread t k v
  | Error err ->
    match Node.Error.code err with
    | "EEXIST" ->
      enqueue_failed_thread t k (Eio.Fs.err @@ Eio.Fs.Already_exists (Node_error err))
    | "ENOENT" ->
      enqueue_failed_thread t k (Eio.Fs.err @@ Eio.Fs.Not_found (Node_error err))
    | _ -> enqueue_failed_thread t k (unclassified_error @@ Node_error err)

let enter_io fn =
  enter_unchecked @@ fun st k ->
  st.pending_io <- st.pending_io + 1;
  fn (fun res -> enqueue_result st k res; st.pending_io <- st.pending_io - 1)

(* Can only be called from our domain. *)
let enqueue_at_head t k v =
  Lf_queue.push_head t.run_q (Thread (fun () -> Suspended.continue k v))

module Low_level = struct
  let or_raise = or_raise

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
      enter_io (Node.Fs.close fd)

    let ensure_closed t =
      if is_open t then close t

    let to_node = get "to_node"

    let of_node_no_hook ~close_unix fd =
      { fd = `Open fd; release_hook = Eio.Switch.null_hook; close_unix }

    let of_node ?(close_unix=true) ~sw fd =
      let t = of_node_no_hook ~close_unix fd in
      t.release_hook <- Switch.on_release_cancellable sw (fun () -> ensure_closed t);
      t

    let open_ ~sw ?mode path flags =
      enter_io (Node.Fs.open_ ?mode ~flags path)
      |> of_node ~sw

    let read ?file_offset fd bufs =
      enter_io (Node.Fs.read ?off:file_offset (get "read" fd) bufs)

    let readv ?file_offset fd bufs =
      enter_io (Node.Fs.readv ?position:file_offset (get "readv" fd) bufs)

    let write_single ?file_offset fd bufs =
      enter_io (Node.Fs.writev ?off:file_offset (get "write" fd) bufs)

    let fstat fd =
      enter_io (Node.Fs.fstat (get "fstat" fd))

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
      let sent = write_single fd bufs in
      let rec aux = function
        | [] -> Ok ()
        | x :: xs when Brr.Tarray.length x = 0 -> aux xs
        | bufs -> write fd bufs
      in
      aux @@ drop bufs sent

    let realpath path = enter_io (Node.Fs.realpath path)

    let mkdir ~mode path = enter_io (Node.Fs.mkdir ~mode path)

    let unlink path = enter_io (Node.Fs.unlink path)

    let rmdir path = enter_io (Node.Fs.rmdir path)

    let rename old_path new_path = enter_io (Node.Fs.rename old_path new_path)

    let readdir dir = enter_io (Node.Fs.readdir dir)
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

let node_stat_to_stat stat =
  let open Node.Fs in
  let st_kind : Eio.File.Stat.kind =
    if Stat.is_file stat then `Regular_file
    else if Stat.is_directory stat then `Directory
    else if Stat.is_char_device stat then `Character_special
    else if Stat.is_block_device stat then `Block_device
    else if Stat.is_symbolic_link stat then `Symbolic_link
    else if Stat.is_fifo stat then `Fifo
    else `Socket
  in
  Eio.File.Stat.{
    dev     = Stat.dev stat;
    ino     = Stat.ino stat;
    kind    = st_kind;
    perm    = Stat.mode stat |> Int64.to_int;
    nlink   = Stat.nlink stat;
    uid     = Stat.uid stat;
    gid     = Stat.gid stat;
    rdev    = Stat.rdev stat;
    size    = Stat.size stat |> Optint.Int63.of_int64;
    atime   = Stat.atime stat;
    mtime   = Stat.mtime stat;
    ctime   = Stat.ctime stat;
  }

let flow fd = object (_ : <source; sink; ..>)
  method fd = fd
  method close = Low_level.File.close fd

  method stat = File.fstat fd |> node_stat_to_stat

  method probe : type a. a Eio.Generic.ty -> a option = function
    | FD -> Some fd
    | _ -> None

  method read_into buf =
    match File.read fd (Node.Buffer.of_cstruct buf) with
    | (0, _) -> raise End_of_file
    | (got, _) ->
      got

  method pread ~file_offset bufs =
    let bufs = List.map Node.Buffer.of_cstruct bufs in
    let file_offset = Optint.Int63.to_int file_offset in
    match File.readv ~file_offset fd bufs with
    | (0, _) -> raise End_of_file
    | (got, _) -> got

  method pwrite ~file_offset bufs =
    let bufs = List.map (fun v -> Brr.Tarray.of_bigarray1 (Cstruct.to_bigarray v)) bufs in
    let file_offset = Optint.Int63.to_int file_offset in
    File.write_single ~file_offset fd bufs

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
let sink fd = (flow fd :> sink)

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


  method private resolve path =
    if closed then Fmt.invalid_arg "Attempt to use closed directory %S" dir_path;
    if Filename.is_relative path then (
      let dir_path = File.realpath dir_path in
      let full = File.realpath (Filename.concat dir_path path) in
      let prefix_len = String.length dir_path + 1 in
      if String.length full >= prefix_len && String.sub full 0 prefix_len = dir_path ^ Filename.dir_sep then
        full
      else if full = dir_path then
        full
      else
        raise (Eio.Fs.err @@ Eio.Fs.Permission_denied (Outside_sandbox (full, dir_path)))
    ) else (
      raise (Eio.Fs.err @@ Eio.Fs.Permission_denied Absolute_path)
    )

  (* We want to create [path]. Check that the parent is in the sandbox. *)
  method private resolve_new path =
    let dir, leaf = Filename.dirname path, Filename.basename path in
    if leaf = ".." then Fmt.failwith "New path %S ends in '..'!" path
    else
      let dir = self#resolve dir in
      Filename.concat dir leaf

  method open_in ~sw path =
    let fd = File.open_ ~sw (self#resolve path) "r" in
    (flow fd :> <Eio.File.ro; Eio.Flow.close>)

  method open_out ~sw ~append ~create path =
    let mode, flags =
      (* https://nodejs.org/dist/latest-v18.x/docs/api/fs.html#file-system-flags *)
      match create with
      | `Never            -> 0,    ""
      | `If_missing  perm -> perm, "+"
      | `Or_truncate perm -> perm, "+"
      | `Exclusive   perm -> perm, "x"
    in
    let flags = if append then "a" ^ flags else "w" ^ flags in
    let real_path =
      if create = `Never then self#resolve path
      else self#resolve_new path
    in
    let fd = File.open_ ~sw real_path flags ~mode in
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
    File.mkdir ~mode:perm real_path

  (* libuv doesn't seem to provide a race-free way to do this. *)
  method unlink path =
    let dir_path = Filename.dirname path in
    let leaf = Filename.basename path in
    let real_dir_path = self#resolve dir_path in
    File.unlink (Filename.concat real_dir_path leaf)

  (* libuv doesn't seem to provide a race-free way to do this. *)
  method rmdir path =
    let dir_path = Filename.dirname path in
    let leaf = Filename.basename path in
    let real_dir_path = self#resolve dir_path in
    File.rmdir (Filename.concat real_dir_path leaf)

  method read_dir path =
    let path = self#resolve path in
    File.readdir path

  method rename old_path new_dir new_path =
    match dir_resolve_new new_dir with
    | None -> invalid_arg "Target is not a luv directory!"
    | Some new_resolve_new ->
      let old_path = self#resolve old_path in
      let new_path = new_resolve_new new_path in
      File.rename old_path new_path

  method close = closed <- true

  method pp f = Fmt.string f (String.escaped label)
end

(* Full access to the filesystem. *)
let fs = object
  inherit dir ~label:"fs" "."

  (* No checks *)
  method! private resolve path = path
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
  let stdin = lazy (source (File.of_node_no_hook Node.Process.stdin ~close_unix:true)) in
  let stdout = lazy (sink (File.of_node_no_hook Node.Process.stdout ~close_unix:true)) in
  let stderr = lazy (sink (File.of_node_no_hook Node.Process.stderr ~close_unix:true)) in
  object (_ : stdenv)
    method stdin  = Lazy.force stdin
    method stdout = Lazy.force stdout
    method stderr = Lazy.force stderr
    method fs = (fs :> Eio.Fs.dir), "."
    method cwd = (cwd :> Eio.Fs.dir), "."
  end

let rec schedule t =
  match Lf_queue.pop t.run_q with
  | Some (Thread f) -> f ()
  | None ->
    if t.pending_io = 0 then begin
      (match t.imm with Some imm -> Node.clear_immediate imm | _ -> ());
      ()
    end
    else begin
      match t.imm with
      | None ->
        let imm = Node.set_immediate (fun () -> t.imm <- None; ignore @@ schedule t) in
        t.imm <- Some imm;
        schedule t
      | Some _ -> ()
    end

(* The main loop:

   The main loop makes an assumption that there is only ever one domain.
   It prevents the event loop from exiting by always adding a set_immediate
   callback if there is pending IO. *)

let run main =
  let run_q = Lf_queue.create () in
  let prom, finish = Fut.create () in
  let st = { run_q; fd_map = Fd_map.empty; pending_io = 0; imm = None } in
  let rec fork ~new_fiber:fiber fn : unit =
    Ctf.note_switch (Fiber_context.tid fiber);
    let open Effect.Deep in
    match_with fn ()
      { retc = (fun () -> Fiber_context.destroy fiber; schedule st);
        exnc = (fun e -> Fiber_context.destroy fiber; raise e);
        effc = fun (type a) (e : a Effect.t) ->
          match e with
          | Eio.Private.Effects.Fork (new_fiber, f) ->
            Some (fun (k : (a, _) continuation) ->
                let k = { Suspended.k; fiber } in
                enqueue_at_head st k ();
                fork ~new_fiber f
              )
          | Eio.Private.Effects.Get_context -> Some (fun k -> continue k fiber)
          | Enter_unchecked fn -> Some (fun k ->
              fn st { Suspended.k; fiber };
              schedule st
            )
          | Eio.Private.Effects.Suspend fn ->
            Some (fun k ->
                let k = { Suspended.k; fiber } in
                fn fiber (function Ok v -> enqueue_thread st k v | Error e -> enqueue_failed_thread st k e);
                schedule st
              )
          | _ -> None
      }
  in
  let new_fiber = Fiber_context.make_root () in
  let () = fork ~new_fiber (fun () ->
      finish (main stdenv)
    )
  in
  let imm = Node.set_immediate (fun () -> schedule st) in
  st.imm <- Some imm;
  prom