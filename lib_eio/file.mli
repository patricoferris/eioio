open Std

(** Tranditional Unix permissions. *)
module Unix_perm : sig
  type t = int
  (** This is the same as {!Unix.file_perm}, but avoids a dependency on [Unix]. *)
end

(** Portable file stats. *)
module Stat : sig

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
  (** Kind of file from st_mode. **)

  val pp_kind : kind Fmt.t
  (** Pretty printer for {! kind}. *)

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

  val dev   : int64 f
  val ino   : int64 f
  val kind  : kind f
  val perm  : int f
  val nlink : int64 f
  val uid   : int64 f
  val gid   : int64 f
  val rdev  : int64 f
  val size  : int64 f
  val atime : float f
  val ctime : float f
  val mtime : float f
end

type ro_ty = [`File | Flow.source_ty | Resource.close_ty]

type 'a ro = ([> ro_ty] as 'a) r
(** A file opened for reading. *)

type rw_ty = [ro_ty | Flow.sink_ty]

type 'a rw = ([> rw_ty] as 'a) r
(** A file opened for reading and writing. *)

module Pi : sig
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

  val ro : (module READ with type t = 't) -> ('t, ro_ty) Resource.handler

  val rw : (module WRITE with type t = 't) -> ('t, rw_ty) Resource.handler
end

val stat : _ ro -> ('a, 'b) Stat.t -> 'a -> 'b
(** [stat t fields fn] will retrieve the file statistics for the specified
    [fields] and apply them as arguments to [fn]. *)

val size : _ ro -> Optint.Int63.t
(** [size t] returns the size of [t].
    Equivalent to [stat t [File.size] Fun.id]. *)

val pread : _ ro -> file_offset:Optint.Int63.t -> Cstruct.t list -> int
(** [pread t ~file_offset bufs] performs a single read of [t] at [file_offset] into [bufs].

    It returns the number of bytes read, which may be less than the space in [bufs],
    even if more bytes are available. Use {!pread_exact} instead if you require
    the buffer to be filled.

    To read at the current offset, use {!Flow.single_read} instead. *)

val pread_exact : _ ro -> file_offset:Optint.Int63.t -> Cstruct.t list -> unit
(** [pread_exact t ~file_offset bufs] reads from [t] into [bufs] until [bufs] is full.

    @raise End_of_file if the buffer could not be filled. *)

val pwrite_single : _ rw -> file_offset:Optint.Int63.t -> Cstruct.t list -> int
(** [pwrite_single t ~file_offset bufs] performs a single write operation, writing
    data from [bufs] to location [file_offset] in [t].

    It returns the number of bytes written, which may be less than the length of [bufs].
    In most cases, you will want to use {!pwrite_all} instead. *)

val pwrite_all : _ rw -> file_offset:Optint.Int63.t -> Cstruct.t list -> unit
(** [pwrite_all t ~file_offset bufs] writes all the data in [bufs] to location [file_offset] in [t]. *)
