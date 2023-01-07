open Js_of_ocaml

let pure_js_expr = Js.Unsafe.pure_js_expr

module Int32Array = struct
  type t = Jv.t
end

module SharedArrayBuffer = struct
  type t = Jv.t

  let shared_array_buffer = Jv.get Jv.global "SharedArrayBuffer"
  let int32_array = Jv.get Jv.global "Int32Array"

  let v size = Jv.new' shared_array_buffer [| Jv.of_int size |]

  let to_int32_array sab =
    Jv.new' int32_array [| sab |]

  include (Jv.Id : Jv.CONV with type t := t)
end

module Atomics = struct
  type t = Jv.t
  let atomics = Jv.get Jv.global "Atomics"
  let wait arr off v =
    Jv.call atomics "wait" [| arr; Jv.of_int off; Jv.of_int v |]
    |> ignore

  let notify (arr : Int32Array.t) off =
    Jv.call atomics "notify" [| arr; Jv.of_int off |]
    |> ignore
end

module Error = struct
  type t = Jv.t

  let message t = Jv.get t "message" |> Jv.to_string

  let code t = Jv.get t "code" |> Jv.to_string

  let path t = Jv.find t "path" |> Option.map Jv.to_string

  let to_result v j =
    if (j == Jv.null || j == Jv.undefined) then Ok v else Error j
end

exception Node_error of string

module ArrayBuffer = struct
  include Brr.Tarray.Buffer

  let _drop ts len =
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

  type t = Jv.t

  let sub ~off ~len buf = Jv.call buf "subarray" [| Jv.of_int off; Jv.of_int len |]

  let size buf = Jv.get buf "size" |> Jv.to_int

  (* TODO: Is there a safer way to do this? *)
  external caml_ba_create_unsafe :
    ('a, 'b) Bigarray.kind ->
    'c Bigarray.layout ->
    int array ->
    Jv.t ->
    ('a, 'b, 'c) Bigarray.Genarray.t = "caml_ba_create_unsafe"

  let to_cstruct buff =
    let ba = caml_ba_create_unsafe Bigarray.Char Bigarray.C_layout [| size buff |] buff in
    let ba = Bigarray.array1_of_genarray ba in
    Cstruct.of_bigarray ba

  let of_cstruct buff : t =
    Jv.get (Obj.magic @@ Cstruct.to_bigarray buff) "data"
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

  let read ?buf_off ?off ?len (fd : fd) buff cb =
    let cb' err bytes_read buff =
      let read = Jv.to_int bytes_read in
      cb (Error.to_result (read, buff) err)
    in
    let options =
      Jv.obj [|
        "offset", Jv.of_option ~none:Jv.undefined Jv.of_int buf_off;
        "length", Jv.of_option ~none:Jv.undefined Jv.of_int len;
        "position", Jv.of_option ~none:Jv.undefined Jv.of_int off;
      |]
    in
    Jv.call fs "read" [| fd; buff; options; Jv.callback ~arity:3 cb' |] |> ignore

  let readv ?position (fd : fd) buffs cb =
    let cb' err bytes_read buff =
      let read = Jv.to_int bytes_read in
      cb (Error.to_result (read, Jv.to_jv_list buff) err)
    in
    let options = Jv.of_option ~none:Jv.undefined Jv.of_int position in
    Jv.call fs "readv" [| fd; Jv.of_jv_list buffs; options; Jv.callback ~arity:3 cb' |] |> ignore

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

  module Stat = struct
    type t = Jv.t

    let is_block_device stat =
      Jv.call stat "isBlockDevice" [||] |> Jv.to_bool

    let is_char_device stat =
      Jv.call stat "isCharacterDevice" [||] |> Jv.to_bool

    let is_directory stat =
      Jv.call stat "isDirectory" [||] |> Jv.to_bool

    let is_fifo stat =
      Jv.call stat "isFIFO" [||] |> Jv.to_bool

    let is_socket stat =
      Jv.call stat "isSocket" [||] |> Jv.to_bool

    let is_symbolic_link stat =
      Jv.call stat "isSymbolicLink" [||] |> Jv.to_bool

    let is_file stat =
      Jv.call stat "isFile" [||] |> Jv.to_bool

    let get_int64 jv =
      Jv.call jv "toString" [||] |> Jv.to_string |> Big_int.big_int_of_string |> Big_int.int64_of_big_int

    let dev stat =
      Jv.get stat "dev" |> get_int64
    let ino stat = Jv.get stat "ino" |> get_int64
    let mode stat = Jv.get stat "mode" |> get_int64
    let nlink stat = Jv.get stat "nlink" |> get_int64
    let uid stat = Jv.get stat "uid" |> get_int64
    let gid stat = Jv.get stat "gid" |> get_int64
    let rdev stat = Jv.get stat "rdev" |> get_int64
    let size stat = Jv.get stat "size" |> get_int64
    let atime stat = Jv.Float.get stat "atime"
    let mtime stat = Jv.Float.get stat "mtime"
    let ctime stat = Jv.Float.get stat "ctime"

  end

  let fstat fd cb =
    let cb' err v =
      cb (Error.to_result v err)
    in
    let options = Jv.obj [| "bigint", Jv.true' |] in
    Jv.call fs "fstat" [| fd; options; Jv.callback ~arity:2 cb' |]
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

  let worker_data = Jv.get worker "workerData"

  let terminate w = Jv.call w "terminate" [||] |> ignore

  let parent_port_post_message msg =
    let pp = Jv.get worker "parentPort" in
    Jv.call pp "postMessage" [| msg |]
    |> ignore

  let v data =
    let data = Jv.obj [| "workerData", data |] in
    Jv.new' (Jv.get worker "Worker") [| pure_js_expr "__filename"; data |]

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

let _set_immediate = Jv.get Jv.global "setImmediate"
let _clear_immediate = Jv.get Jv.global "clearImmediate"

let set_immediate fn =
  Jv.apply _set_immediate [| Jv.callback ~arity:1 fn |]

let clear_immediate j =
  ignore @@ Jv.apply _clear_immediate [| j |]
