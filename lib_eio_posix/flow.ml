open Eio.Std

module Fd = Eio_unix.Fd

module Impl = struct
  type tag = [`Generic | `Unix]

  type t = Eio_unix.Fd.t

  (* TODO fix rounding *)
  let float_of_time s ns = Int64.to_float s +. (float ns /. 1e9)

  let stat t k =
    let open Eio.File in
    (* TODO pool the buf *)
    let t = Low_level.fstat t in
    try
      let rec fn : type a b. (a, b) stats -> a -> b = fun v acc ->
        match v with
        | Dev :: tl -> fn tl @@ acc (Low_level.dev t)
        | Ino :: tl -> fn tl @@ acc (Low_level.ino t)
        | Kind :: tl -> fn tl @@ acc (Low_level.kind t)
        | Perm :: tl -> fn tl @@ acc (Low_level.perm t)
        | Nlink :: tl -> fn tl @@ acc (Low_level.nlink t)
        | Uid :: tl -> fn tl @@ acc (Low_level.uid t)
        | Gid :: tl -> fn tl @@ acc (Low_level.gid t)
        | Rdev :: tl -> fn tl @@ acc (Low_level.rdev t)
        | Size :: tl -> fn tl @@ acc (Low_level.size t)
        | Atime :: tl -> fn tl @@ acc (float_of_time (Low_level.atime_sec t) (Low_level.atime_nsec t))
        | Mtime :: tl -> fn tl @@ acc (float_of_time (Low_level.mtime_sec t) (Low_level.mtime_nsec t))
        | Ctime :: tl -> fn tl @@ acc (float_of_time (Low_level.ctime_sec t) (Low_level.ctime_nsec t))
        | [] -> acc
      in
      fn k
    with Unix.Unix_error (code, name, arg) -> raise @@ Err.wrap code name arg

  let single_write t bufs =
    try
      Low_level.writev t (Array.of_list bufs)
    with Unix.Unix_error (code, name, arg) ->
      raise (Err.wrap code name arg)

  let write_all t bufs =
    try
      let rec loop = function
        | [] -> ()
        | bufs ->
          let wrote = Low_level.writev t (Array.of_list bufs) in
          loop (Cstruct.shiftv bufs wrote)
      in
      loop bufs
    with Unix.Unix_error (code, name, arg) -> raise (Err.wrap code name arg)

  let copy dst ~src =
    let buf = Cstruct.create 4096 in
    try
      while true do
        let got = Eio.Flow.single_read src buf in
        write_all dst [Cstruct.sub buf 0 got]
      done
    with End_of_file -> ()

  let single_read t buf =
    match Low_level.readv t [| buf |] with
    | 0 -> raise End_of_file
    | got -> got
    | exception (Unix.Unix_error (code, name, arg)) -> raise (Err.wrap code name arg)

  let shutdown t cmd =
    try
      Low_level.shutdown t @@ match cmd with
      | `Receive -> Unix.SHUTDOWN_RECEIVE
      | `Send -> Unix.SHUTDOWN_SEND
      | `All -> Unix.SHUTDOWN_ALL
    with
    | Unix.Unix_error (Unix.ENOTCONN, _, _) -> ()
    | Unix.Unix_error (code, name, arg) -> raise (Err.wrap code name arg)

  let read_methods = []

  let pread t ~file_offset bufs =
    let got = Low_level.preadv ~file_offset t (Array.of_list bufs) in
    if got = 0 then raise End_of_file
    else got

  let pwrite t ~file_offset bufs = Low_level.pwritev ~file_offset t (Array.of_list bufs)

  let send_msg t ~fds data =
    Low_level.send_msg ~fds t (Array.of_list data)

  let recv_msg_with_fds t ~sw ~max_fds data =
    let _addr, n, fds = Low_level.recv_msg_with_fds t ~sw ~max_fds (Array.of_list data) in
    n, fds

  let fd t = t

  let close = Eio_unix.Fd.close
end

let handler = Eio_unix.Pi.flow_handler (module Impl)

let of_fd fd =
  let r = Eio.Resource.T (fd, handler) in
  (r : [`Unix_fd | Eio_unix.Net.stream_socket_ty | Eio.File.rw_ty] r :>
     [< `Unix_fd | Eio_unix.Net.stream_socket_ty | Eio.File.rw_ty] r)

module Secure_random = struct
  type t = unit

  let single_read () buf =
    Low_level.getrandom buf;
    Cstruct.length buf

  let read_methods = []
end

let secure_random =
  let ops = Eio.Flow.Pi.source (module Secure_random) in
  Eio.Resource.T ((), ops)
