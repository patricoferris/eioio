(* As needed nodejs API bindings *)

module Error : sig
  type t

  val message : t -> string

  val to_result : 'a -> Jv.t -> ('a, t) result
end

exception Node_error of string

module ArrayBuffer : sig
  type t = Brr.Tarray.Buffer.t

  val byte_length : t -> int

  val drop : t list -> int -> t list
end

module Buffer : sig
  type t

  val make : int -> t
  val sub : off:int -> len:int -> t -> t
  val size : t -> int

  val to_cstruct : t -> Cstruct.t
  val of_cstruct : Cstruct.t -> t

  val to_array_buffer : t -> ArrayBuffer.t
end

module Async : sig
  type t

  val init : (unit -> unit) -> t

  val send : t -> unit
end

module Fs : sig
  type fd

  val open_ : ?flags:string -> ?mode:int -> string -> ((fd, Error.t) result -> 'b) -> unit
  val close : fd -> ((unit, Error.t) result -> 'b) -> unit
  val read : 
    ?buf_off:int ->
    ?off:int ->
    ?len:int -> fd -> Buffer.t -> ((int * Buffer.t, Error.t) result -> 'b) -> unit

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
end

module Process : sig
  val stdin : Fs.fd
  val stdout : Fs.fd
  val stderr : Fs.fd
end

module Worker : sig
  type t

  val is_main_thread : bool

  val v : string -> t

  val parent_port_post_message : Jv.t -> unit

  val on_message : t -> ('a -> 'b) -> unit
  val on_error : t -> ('a -> 'b) -> unit
end

val prevent_exit : unit -> int