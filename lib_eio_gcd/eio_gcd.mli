(*
 * Copyright (C) 2021 Thomas Leonard
 * Copyright (C) 2021 Patrick Ferris
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

 (* A work-in-progress macOS, Grand Central Dispatch (GCD) backend for Eio.
    Whilst parts are not implemented, this backend uses the libuv backend as
    a base and unhandled effects fall through to luv when using Eio_main. *)

 open Eio.Std

 module Buffer : sig
  type t
  (** A wrapper around {! Dispatch.Data.t} *)

  val empty : unit -> t
  (** A new, empty buffer *)

  val to_string : t -> string
  (** Convert a buffer to a string, this can be quite expensive.  *)
end

 (** {1 Low-level wrappers for Gcd functions} *)

 module File : sig
   type t

   val is_open : t -> bool
   (** [is_open t] is [true] if {!close t} hasn't been called yet. *)

   val close : t -> unit
   (** [close t] closes [t].
       @raise Invalid_arg if [t] is already closed. *)

   val of_gcd : sw:Switch.t -> Dispatch.Io.t -> t
   (** [of_gcd ~sw fd] wraps [fd] as an open file descriptor.
       This is unsafe if [fd] is closed directly (before or after wrapping it).
       @param sw The FD is closed when [sw] is released, if not closed manually first. *)

   val to_gcd : t -> Dispatch.Io.t
   (** [to_gcd t] returns the wrapped descriptor.
       This allows unsafe access to the FD.
       @raise Invalid_arg if [t] is closed. *)

   val open_ :
     sw:Switch.t ->
     ?mode:int ->
     string -> int -> (t, [`Msg of string]) result
   (** Wraps {!Dispatch.Io.create_with_path}, note that GCD does not open the file descriptor
       when this returns. It will not be opened until it is used. *)

   val read : off:int -> length:int -> t -> Buffer.t -> int
   (** Wraps {!Dispatch.Io.with_read} *)

   val write : t -> Buffer.t -> unit
   (** [write t buf] writes all the data in [buf]. *)

   val realpath : string -> string Eio_luv.or_error
   (** Wraps {!Luv.File.realpath} *)

   val mkdir : mode:Luv.File.Mode.t list -> string -> unit Eio_luv.or_error
   (** Wraps {!Luv.File.mkdir} *)
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

 val run : (Eio.Stdenv.t -> unit) -> unit
 (** The main loop uses {!Eio_luv.run} *)
