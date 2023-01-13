module Timeout : sig
  val sleep : ms:int -> unit
  (** Non-blocking timeout that waits for [ms] millseconds. *)
end

val await : 'a Fut.t -> 'a
(** [await fut] blocks on the promise [fut] and allows other fibers to do work. *)

(** {1 Main loop} *)

val run : (unit -> 'a) -> 'a Fut.t
(** [run main] runs [main] whose result is returned as a promise. *)