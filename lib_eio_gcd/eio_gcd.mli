(*
 * Copyright (C) 2021 Thomas Leonard
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Eio.Std

(* TODO: Wrap dispatch errors *)
type 'a or_error = ('a, [`Msg of string]) result

val or_raise : 'a or_error -> 'a
(** [or_error (Error e)] raises [Luv_error e]. *)

val await : (('a -> unit) -> unit) -> 'a
(** [await fn] converts a function using a gcd-style callback to one using effects.
    Use it as e.g. [await (Dispatch.async f)]. *)

(** {1 Time functions} *)

val sleep_until : ?sw:Switch.t -> float -> unit
(** [sleep_until time] blocks until the current time is [time].
    @param sw Cancel the sleep if [sw] is turned off. *)

val yield : ?sw:Switch.t -> unit -> unit

(** {1 Low-level wrappers for Luv functions} *)

module File : sig
  type t

  val is_open : t -> bool
  (** [is_open t] is [true] if {!close t} hasn't been called yet. *)

  val close : t -> unit
  (** [close t] closes [t].
      @raise Invalid_arg if [t] is already closed. *)

  val open_ :
    sw:Switch.t ->
    string -> t or_error

  val read : ?sw:Switch.t -> t -> Dispatch.Data.t -> int or_error

  val write : ?sw:Switch.t -> t -> Dispatch.Data.t -> unit
end

module Handle : sig
  type 'a t

  val is_open : 'a t -> bool
  (** [is_open t] is [true] if {!close t} hasn't been called yet. *)

  val close : 'a t -> unit
  (** [close t] closes [t].
      @raise Invalid_arg if [t] is already closed. *)
end

(** {1 Eio API} *)

module Objects : sig
  type has_fd = < fd : File.t >
  type source = < Eio.Flow.source; Eio.Flow.close; has_fd >
  type sink   = < Eio.Flow.sink  ; Eio.Flow.close; has_fd >

  type stdenv = <
    stdin  : source;
    stdout : sink;
    stderr : sink;
    net : Eio.Net.t;
    domain_mgr : Eio.Domain_manager.t;
    clock : Eio.Time.clock;
    fs : Eio.Dir.t;
    cwd : Eio.Dir.t;
  >

  val get_fd : <has_fd; ..> -> File.t
  val get_fd_opt : #Eio.Generic.t -> File.t option
end

(** {1 Main Loop} *)

val run : (Objects.stdenv -> unit) -> unit
