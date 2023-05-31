module Timeout : sig
  val sleep : ms:int -> unit
  (** Non-blocking timeout that waits for [ms] millseconds. *)
end

val await : 'a Fut.t -> 'a
(** [await fut] blocks on the promise [fut] and allows other fibers to do work. *)

val next_event : 'a Brr.Ev.type' -> Brr.Ev.target -> 'a Brr.Ev.t
(** [next_event typ target] blocks until an event of type [typ] arrives
    on the [target]. *)

(** {1 Main loop} *)

val run : (unit -> 'a) -> 'a Fut.t
(** [run main] runs [main] whose result is returned as a promise. *)

(** {1 Callbacks} *)

val run_callbacks : (unit -> unit)
(** [run_callbacks] is a never-ending loop that will handle all callbacks
    wrapped by [wrap_callback].
    It must be called at the end of your main loop
    if you want to use Eio inside event handlers. *)

val listen :
    ?opts:Brr.Ev.listen_opts ->
    'a Brr.Ev.type' -> ('a Brr.Ev.t -> unit) -> Brr.Ev.target -> Brr.Ev.listener
(** Identical to {! Brr.Ev.listen} except the callback for the event listener
    can perform asynchronous actions (i.e. perform effects) and they will be
    handled properly. *)

val wrap_callback : ('a -> unit) -> 'a -> bool Js_of_ocaml.Js.t
(** Callbacks must be wrapped with [wrap_callback]
    if you want to use Eio from inside.
*)
