# Set up the test environment

```ocaml
# #require "eio_gcd";;
# open Eio.Std;;
```

```ocaml
let rec read_exactly ~length fd buf =
  Eio_gcd.File.read ~off:0 ~length fd buf
```

# Hello, world

```ocaml
# Eio_gcd.run @@ fun env ->
  Eio.Flow.copy_string "Hello, world!\n" (Eio.Stdenv.stdout env);;
Hello, world!
- : unit = ()
```

# Read a few bytes from /dev/zero

```ocaml
let main _stdenv =
  Switch.run @@ fun sw ->
  let fd = Eio_gcd.File.open_ ~sw "/dev/zero" 0 |> Result.get_ok in
  let buf = Eio_gcd.Buffer.empty () in
  let num_bytes = 10 in
  let got : int = read_exactly ~length:num_bytes fd buf in
  assert (got = num_bytes);
  traceln "Read %i bytes: %S" got (Eio_gcd.Buffer.to_string buf);
  Eio_gcd.File.close fd
```

```ocaml
# Eio_gcd.run main;;
+Read 10 bytes: "\000\000\000\000\000\000\000\000\000\000"
- : unit = ()
```
