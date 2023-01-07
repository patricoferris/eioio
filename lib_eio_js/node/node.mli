(* As needed nodejs API bindings *)

module Int32Array : sig
  type t
end

module SharedArrayBuffer : sig
  type t

  val v : int -> t

  val to_int32_array : t -> Int32Array.t

  include Jv.CONV with type t := t
end

module Atomics : sig
  type t

  val wait : Int32Array.t -> int -> int -> unit
  val notify : Int32Array.t -> int -> unit
end

module Error : sig
  type t
  (** Node errors *)

  val message : t -> string
  (** A description of the error *)

  val code : t -> string
  (** The error code, see {{: https://docs.libuv.org/en/v1.x/errors.html} the libuv docs}. *)

  val path : t -> string option
  (** If present, the relevant invalid pathname. *)
end

exception Node_error of string

module Buffer : sig
  type t
  (** A Node buffer *)

  val sub : off:int -> len:int -> t -> t
  (** [sub ~off ~len t] returns a new buffer that starts from [off]
      and continues for [len] elements. *)

  val size : t -> int
  (** [size t] is the size of the buffer. *)

  val to_cstruct : t -> Cstruct.t
  (** [to_cstruct t] converts [t] to a cstruct sharing the data. *)

  val of_cstruct : Cstruct.t -> t
  (** [of_cstruct buff] converts [buff] to a buffer sharing the underlying data. *)
end

module Fs : sig
  type fd

  val open_ : ?flags:string -> ?mode:int -> string -> ((fd, Error.t) result -> 'b) -> unit
  val close : fd -> ((unit, Error.t) result -> 'b) -> unit

  val read :
    ?buf_off:int ->
    ?off:int ->
    ?len:int -> fd -> Buffer.t -> ((int * Buffer.t, Error.t) result -> 'b) -> unit

  val readv :
    ?position:int ->
    fd ->
    Buffer.t list ->
    ((int * Buffer.t list, Error.t) result -> 'b) -> unit

  val write :
    ?buf_off:int ->
    ?off:int ->
    ?len:int -> fd -> Buffer.t -> ((int * Buffer.t, Error.t) result -> 'b) -> unit

  val writev :
    ?off:int -> fd -> ('a, 'c) Brr.Tarray.t list -> ((int, Error.t) result -> 'b) -> unit

  val realpath : string -> ((string, Error.t) result -> 'b) -> unit
  val mkdir : ?mode:int -> string -> ((unit, Error.t) result -> 'b) -> unit
  val unlink : string -> ((unit, Error.t) result -> 'b) -> unit
  val rmdir : string -> ((unit, Error.t) result -> 'b) -> unit
  val readdir : string -> ((string list, Error.t) result -> 'b) -> unit
  val rename : string -> string -> ((unit, Error.t) result -> 'b) -> unit

  module Stat : sig
    type t

    val is_block_device : t -> bool
    val is_char_device : t -> bool
    val is_directory : t -> bool
    val is_fifo : t -> bool
    val is_file : t -> bool
    val is_socket : t -> bool
    val is_symbolic_link : t -> bool

    val dev : t -> int64
    val ino : t -> int64
    val mode : t -> int64
    val nlink : t -> int64
    val uid : t -> int64
    val gid : t -> int64
    val rdev : t -> int64
    val size : t -> int64
    val atime : t -> float
    val mtime : t -> float
    val ctime : t -> float
  end
  val fstat : fd -> ((Stat.t, Error.t) result -> 'b) -> unit
end

module Process : sig
  val stdin : Fs.fd
  val stdout : Fs.fd
  val stderr : Fs.fd
end

module Worker : sig
  type t

  val is_main_thread : bool

  val worker_data : Jv.t

  val terminate : t -> unit

  val v : Jv.t -> t

  val parent_port_post_message : Jv.t -> unit

  val on_message : t -> ('a -> 'b) -> unit
  val on_error : t -> ('a -> 'b) -> unit
end

val prevent_exit : unit -> int

val set_immediate : (unit -> 'a) -> Jv.t

val clear_immediate : Jv.t -> unit