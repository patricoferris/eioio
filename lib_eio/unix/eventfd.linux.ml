type t = Unix.file_descr

external eio_eventfd : int -> Unix.file_descr = "caml_eio_unix_eventfd"

let create = eio_eventfd

let wake_buffer =
  let b = Bytes.create 8 in
  Bytes.set_int64_ne b 0 1L;
  b

let send t =
  let sent = Unix.single_write t wake_buffer 0 8 in
  assert (sent = 8)

let monitor ~read t =
  let buf = Cstruct.create 8 in
  while true do
    let got = read t buf in
    assert (got = 8);
    (* We just go back to sleep now, but this will cause the scheduler to look
        at the run queue again and notice any new items. *)
  done;
  assert false

