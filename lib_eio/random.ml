class virtual generator = object
  method virtual fill : Cstruct.t -> unit
end

let fill (g : #generator) buf = g#fill buf

let generate (g : #generator) n =
  let buf = Cstruct.create n in
  g#fill buf;
  buf
