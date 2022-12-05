(* Portable eventfd(2) like value *)
type t

val create : int -> t

val send : t -> unit

val monitor : read:(Unix.file_descr -> Cstruct.t -> int) -> t -> unit

val close : t -> unit