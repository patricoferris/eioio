type source = < Eio.Flow.source; Eio.Flow.close >
type sink   = < Eio.Flow.sink  ; Eio.Flow.close >

type stdenv = <
  stdin  : source;
  stdout : sink;
  stderr : sink;
  net : Eio.Net.t;
  domain_mgr : Eio.Domain_manager.t;
  clock : Eio.Time.clock;
  fs : Eio.Dir.t;
  cwd : Eio.Dir.t;
  secure_random : Eio.Flow.source;
>

val run : (stdenv -> unit) -> unit