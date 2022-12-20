open Js_of_ocaml

let pure_js_expr = Js_of_ocaml.Js.Unsafe.pure_js_expr

module Async = struct
  type t = Jv.t

  let events = pure_js_expr "require('events')"
  let event_emitter = Jv.get events "EventEmitter"

  let init cb = 
    let emitter = Jv.new' event_emitter [||] in
    ignore (Jv.call emitter "on" [| Jv.of_string "event"; Jv.callback ~arity:1 cb |]);
    emitter

  let send e =
    Jv.call e "emit" [| Jv.of_string "event" |]
    |> ignore
end

module Error = struct
  type t = Jv.t

  let message t = Jv.get t "message" |> Jv.to_string

  let to_result v j =
    if (j == Jv.null || j == Jv.undefined) then Ok v else Error j
end

exception Node_error of string

(* let raise_or_ignore f = or_error f |> ignore *)

module ArrayBuffer = struct
  include Brr.Tarray.Buffer

  let drop ts len =
    let rec aux xs n = match xs, n with
      | xs, 0 -> xs
      | x :: xs, n ->
        let size = byte_length x in
        if n > size then aux xs (n - size) else
        x :: xs
      | _ -> failwith "Err"
    in
      aux ts len
end

module Buffer = struct
  let _buffer = pure_js_expr "require('buffer')"

  let buffer = Jv.get Jv.global "Buffer"

  type t = Jv.t

  let make size : t =
    Jv.call buffer "alloc" [| Jv.of_int size |]

  let sub ~off ~len buf = Jv.call buf "subarray" [| Jv.of_int off; Jv.of_int len |]

  let size buf = Jv.get buf "size" |> Jv.to_int

  let to_array_buffer buf : ArrayBuffer.t = Jv.get buf "buffer" |> ArrayBuffer.of_jv

  (* let of_array_buffer arr = Jv.call buffer "from" [| arr |] *)

  (* TODO: ... better! *)
  let to_cstruct buff =
    Jv.call buff "toString" [||] |>
    Jv.to_string
    |> Cstruct.of_string

  let of_cstruct buff : t =
    let s = Cstruct.to_string buff in
    Jv.call buffer "from" [| Jv.of_string s |]
end


module Fs = struct
  let fs : Jv.t = pure_js_expr "require('node:fs')"

  type fd = Jv.t 
  (** File descriptor *)

  let open_ ?(flags="r") ?(mode=0o644) path cb =
    let cb' err (fd : fd) = cb (Error.to_result fd err) in
    Jv.call fs "open" [| Jv.of_string path; Jv.of_string flags; Jv.of_int mode; Jv.callback ~arity:2 cb' |] |> ignore

  let close (fd : fd) cb =
    let cb' err = cb (Error.to_result () err) in
    Jv.call fs "close" [| fd; Jv.callback ~arity:1 cb' |] |> ignore

  let read ?(buf_off=0) ?(off=0) ?len (fd : fd) buff cb =
    let cb' err bytes_read buff =
      let read = Jv.to_int bytes_read in
      cb (Error.to_result (read, buff) err)
    in
    let len = match len with None -> Jv.undefined | Some len -> Jv.of_int len in
    Jv.call fs "read" [| fd; buff; Jv.of_int buf_off; Jv.of_int off; len; Jv.callback ~arity:3 cb' |] |> ignore

  let write ?(buf_off=0) ?(off=0) ?len (fd : fd) buff cb =
    let cb' err bytes_written buff =
      let read = Jv.to_int bytes_written in
      let buff = Buffer.of_cstruct buff in
      cb (Error.to_result (read, buff) err)
    in
    let len = match len with None -> Jv.undefined | Some len -> Jv.of_int len in
    Jv.call fs "write" [| fd; buff; Jv.of_int buf_off; Jv.of_int off; len; Jv.callback ~arity:3 cb' |] |> ignore

  let writev ?off (fd : fd) buffs cb =
    let cb' err bytes_read _buffs =
      let wrote = Jv.to_int bytes_read in
      cb (Error.to_result wrote err)
    in
    let off = Jv.of_option ~none:Jv.undefined Jv.of_int off in
    Jv.call fs "writev" [| fd; Jv.of_list Brr.Tarray.to_jv buffs; off; Jv.callback ~arity:3 cb' |] |> ignore

  let realpath path cb =
    let cb' err resolved =
      Printf.printf "%s" (Jv.to_string resolved);
      cb (Error.to_result (Jv.to_string resolved) err)
    in
    Jv.call fs "realpath" [| Jv.of_string path; Jv.callback ~arity:2 cb' |]
    |> ignore

  let unlink path cb =
    let cb' err =
      cb (Error.to_result () err)
    in
    Jv.call fs "unlink" [| Jv.of_string path; Jv.callback ~arity:1 cb' |]
    |> ignore

  let mkdir ?(mode=0o777) path cb =
    let cb' err _resolved =
      cb (Error.to_result () err)
    in
    Jv.call fs "mkdir" [| Jv.of_string path; Jv.obj [| "recursive", Jv.false'; "mode", Jv.of_int mode |]; Jv.callback ~arity:2 cb' |]
    |> ignore

  let rmdir path cb =
    let cb' err =
      cb (Error.to_result () err)
    in
    Jv.call fs "rmdir" [| Jv.of_string path; Jv.callback ~arity:1 cb' |]
    |> ignore

  let readdir path cb =
    let cb' err dirs =
      cb (Error.to_result (Jv.to_list Jv.to_string dirs) err)
    in
    Jv.call fs "readdir" [| Jv.of_string path; Jv.callback ~arity:2 cb' |]
    |> ignore

  let rename old new_ cb =
    let cb' err =
      cb (Error.to_result () err)
    in
    Jv.call fs "rename" [| Jv.of_string old; Jv.of_string new_; Jv.callback ~arity:1 cb' |]
    |> ignore
end

module Process = struct
  let process = pure_js_expr "require('node:process')"
  let stdin = Jv.get process "stdin" |> fun v -> Jv.get v "fd"
  let stdout = Jv.get process "stdout" |> fun v -> Jv.get v "fd"
  let stderr = Jv.get process "stderr" |> fun v -> Jv.get v "fd"
end

module Worker = struct
  let worker = pure_js_expr "require('node:worker_threads')"
  
  type t = Jv.t

  let is_main_thread = Jv.get worker "isMainThread" |> Jv.to_bool

  let parent_port_post_message msg =
    let pp = Jv.get worker "parentPort" in
    Jv.call pp "postMessage" [| msg |]
    |> ignore

  let v data = Jv.new' (Jv.get worker "Worker") [| pure_js_expr "__filename" |]

  let on_message t cb = 
    Jv.call t "on" [| Jv.of_string "message"; Jv.callback ~arity:1 cb |]
    |> ignore

  let on_error t cb =
    Jv.call t "on" [| Jv.of_string "error"; Jv.callback ~arity:1 cb |]
    |> ignore
end

let prevent_exit () =
  (* A noop that prevents the nodejs eventloop from
     exiting until the main loops promise is resolved
     by the main program exiting. *)
  Brr.G.set_interval ~ms:10000 (fun () -> ())
