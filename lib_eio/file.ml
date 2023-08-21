open Std

module Unix_perm = struct
  type t = int
end

module Stat = struct
  type kind = [
    | `Unknown
    | `Fifo
    | `Character_special
    | `Directory
    | `Block_device
    | `Regular_file
    | `Symbolic_link
    | `Socket
  ]

  let pp_kind ppf = function
    | `Unknown -> Fmt.string ppf "unknown"
    | `Fifo -> Fmt.string ppf "fifo"
    | `Character_special -> Fmt.string ppf "character special file"
    | `Directory -> Fmt.string ppf "directory"
    | `Block_device -> Fmt.string ppf "block device"
    | `Regular_file -> Fmt.string ppf "regular file"
    | `Symbolic_link -> Fmt.string ppf "symbolic link"
    | `Socket -> Fmt.string ppf "socket"

  type 'a f =
    | Dev : int64 f
    | Ino : int64 f
    | Kind : kind f
    | Perm : int f
    | Nlink : int64 f
    | Uid : int64 f
    | Gid : int64 f
    | Rdev : int64 f
    | Size : int64 f
    | Atime : float f
    | Ctime : float f
    | Mtime : float f

  type ('a, 'ty) t =
    | [] : ('ty, 'ty) t
    | (::) : 'a f * ('b, 'ty) t -> ('a -> 'b, 'ty) t

end

type ro_ty = [`File | Flow.source_ty | Resource.close_ty]

type 'a ro = ([> ro_ty] as 'a) r

type rw_ty = [ro_ty | Flow.sink_ty]

type 'a rw = ([> rw_ty] as 'a) r

module Pi = struct
  module type READ = sig
    include Flow.Pi.SOURCE

    val pread : t -> file_offset:Optint.Int63.t -> Cstruct.t list -> int
    val stat : 'a 'b . t -> ('a, 'b) Stat.t -> 'a -> 'b
    val close : t -> unit
  end

  module type WRITE = sig
    include Flow.Pi.SINK
    include READ with type t := t

    val pwrite : t -> file_offset:Optint.Int63.t -> Cstruct.t list -> int
  end

  type (_, _, _) Resource.pi +=
    | Read : ('t, (module READ with type t = 't), [> ro_ty]) Resource.pi
    | Write : ('t, (module WRITE with type t = 't), [> rw_ty]) Resource.pi

  let ro (type t) (module X : READ with type t = t) =
    Resource.handler [
      H (Flow.Pi.Source, (module X));
      H (Read, (module X));
      H (Resource.Close, X.close);
    ]

  let rw (type t) (module X : WRITE with type t = t) =
    Resource.handler (
      H (Flow.Pi.Sink, (module X)) ::
      H (Write, (module X)) ::
      Resource.bindings (ro (module X))
    )
end

let stat (Resource.T (t, ops)) =
  let module X = (val (Resource.get ops Pi.Read)) in
  X.stat t

let kind t  = stat t [Stat.Kind] Fun.id
let perm t  = stat t [Stat.Perm] Fun.id
let uid t   = stat t [Stat.Uid] Fun.id
let gid t   = stat t [Stat.Gid] Fun.id
let size t  = stat t [Stat.Size] (fun s -> Optint.Int63.of_int64 s)
let atime t = stat t [Stat.Atime] Fun.id
let ctime t = stat t [Stat.Ctime] Fun.id
let mtime t = stat t [Stat.Mtime] Fun.id

let pread (Resource.T (t, ops)) ~file_offset bufs =
  let module X = (val (Resource.get ops Pi.Read)) in
  let got = X.pread t ~file_offset bufs in
  assert (got > 0 && got <= Cstruct.lenv bufs);
  got

let pread_exact (Resource.T (t, ops)) ~file_offset bufs =
  let module X = (val (Resource.get ops Pi.Read)) in
  let rec aux ~file_offset bufs =
    if Cstruct.lenv bufs > 0 then (
      let got = X.pread t ~file_offset bufs in
      let file_offset = Optint.Int63.add file_offset (Optint.Int63.of_int got) in
      aux ~file_offset (Cstruct.shiftv bufs got)
    )
  in
  aux ~file_offset bufs

let pwrite_single (Resource.T (t, ops)) ~file_offset bufs =
  let module X = (val (Resource.get ops Pi.Write)) in
  let got = X.pwrite t ~file_offset bufs in
  assert (got > 0 && got <= Cstruct.lenv bufs);
  got

let pwrite_all (Resource.T (t, ops)) ~file_offset bufs =
  let module X = (val (Resource.get ops Pi.Write)) in
  let rec aux ~file_offset bufs =
    if Cstruct.lenv bufs > 0 then (
      let got = X.pwrite t ~file_offset bufs in
      let file_offset = Optint.Int63.add file_offset (Optint.Int63.of_int got) in
      aux ~file_offset (Cstruct.shiftv bufs got)
    )
  in
  aux ~file_offset bufs
