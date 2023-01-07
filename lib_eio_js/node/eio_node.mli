module File : sig
  type t
  (** A file *)

  val to_node : t -> Node.Fs.fd
  (** Convert a file to a Node file descriptor. *)

  val of_node : ?close_unix:bool -> sw:Eio.Switch.t -> Node.Fs.fd -> t
  (** [of_node ~sw fd] convert a [fd] to a file. This file will be closed
      when [sw] is released. *)
end

type has_fd = < fd : File.t >
type source = < Eio.Flow.source; Eio.Flow.close; has_fd >
type sink   = < Eio.Flow.sink  ; Eio.Flow.close; has_fd >

val get_fd : <has_fd; ..> -> File.t
val get_fd_opt : #Eio.Generic.t -> File.t option

type stdenv = <
  stdin  : source;
  stdout : sink;
  stderr : sink;
  fs : Eio.Fs.dir Eio.Path.t;
  cwd : Eio.Fs.dir Eio.Path.t;
>

(** {1 Main Loop} *)

val run : (stdenv -> 'a) -> 'a Fut.t
(** [run main] runs the function [main] using a nodejs event loop.
    Because Javascript is inherently single-threaded, it returns a
    promise for the result. *)