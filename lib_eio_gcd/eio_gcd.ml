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
      match File.read ~off:(buff.Cstruct.off) ~length:(buff.len) fd data with
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
      method net = failwith "unimplemented"
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
  | _ ->
    Eio_luv.run (fun env -> fn (env :> Eio.Stdenv.t))
