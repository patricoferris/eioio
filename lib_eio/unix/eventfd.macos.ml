type t = { 
  read : Unix.file_descr;
  write : Unix.file_descr;
}

let create _ = 
  let read, write = Unix.pipe ~cloexec:true () in
  { read; write }

let wake_buffer =
  let b = Bytes.create 8 in
  Bytes.set_int64_ne b 0 1L;
  b

let send t =
  let sent = Unix.single_write t.write wake_buffer 0 8 in
  assert (sent = 8)

let monitor ~read t =
  let buf = Cstruct.create 8 in
  while true do
    let got = read t.read buf in
    assert (got = 8);
    (* We just go back to sleep now, but this will cause the scheduler to look
        at the run queue again and notice any new items. *)
  done;
  assert false

let close t = Unix.close t.read; Unix.close t.write

