  type t = {
    buffer : bytes;
    off : int;
    len : int;
  }

  let empty = { buffer = Bytes.empty; off = 0; len = 0 }

  let create size = { buffer = Bytes.make size '\000'; off = 0; len = size }

  let of_bytes ?(off=0) ?len buffer = { buffer; off; len = match len with None -> Bytes.length buffer | Some len -> len }

  let pp_t ppf t =
    Format.fprintf ppf "[%d,%d](%d)" t.off t.len (Bytes.length t.buffer)

  let err_sub t = Fmt.failwith "Cstruct.sub: %a off=%d len=%d" pp_t t

  let is_empty t = Int.equal t.len 0

  let sub t off len =
    let new_start = t.off + off in
    let new_end = new_start + len in
    let old_end = t.off + t.len in
    if new_start >= t.off && new_end <= old_end && new_start <= new_end then
      { t with off = new_start ; len }
    else
      err_sub t off len

  let length t = t.len

  let copy_to_string src srcoff len =
    if len < 0 || srcoff < 0 || src.len - srcoff < len then
      failwith "Error copying to string!"
    else
      let b = Bytes.create len in
      Bytes.blit src.buffer (src.off+srcoff) b 0 len;
      (* The following call is safe, since b is not visible elsewhere. *)
      Bytes.unsafe_to_string b

  let blit_from_string src srcoff dst dstoff len =
    if len < 0 || srcoff < 0 || String.length src - srcoff < len then
      invalid_arg "Bstruct blit"
    else if dstoff < 0 || dst.len - dstoff < len then
      invalid_arg "Bstruct blit"
    else
      String.blit src srcoff dst.buffer
        (dst.off+dstoff) len

  let to_string ?(off=0) ?len t =
    let len = match len with None -> length t - off | Some l -> l in
    copy_to_string t off len
  
  let of_string s = { buffer = Bytes.of_string s; off = 0; len = String.length s }
    
  let pp_t ppf t =
    Format.fprintf ppf "[%d,%d](%d)" t.off t.len (Bytes.length t.buffer)
  let string_t ppf str =
    Format.fprintf ppf "[%d]" (String.length str)
  let bytes_t ppf str =
    Format.fprintf ppf "[%d]" (Bytes.length str)

  let err fmt =
    let b = Buffer.create 20 in                         (* for thread safety. *)
    let ppf = Format.formatter_of_buffer b in
    let k ppf = Format.pp_print_flush ppf (); invalid_arg (Buffer.contents b) in
    Format.kfprintf k ppf fmt

  let check_bounds t len =
    len >= 0 && Bytes.length t.buffer >= len

  let err_shift t = err "Bstruct.shift %a %d" pp_t t
  let err_shiftv n = err "Bstruct.shiftv short by %d" n

  let shift t amount =
    let off = t.off + amount in
    let len = t.len - amount in
    if amount < 0 || amount > t.len || not (check_bounds t (off+len)) then
      err_shift t amount
    else { t with off; len }

  let rec skip_empty = function
    | t :: ts when t.len = 0 -> skip_empty ts
    | x -> x

  let rec shiftv ts = function
    | 0 -> skip_empty ts
    | n ->
      match ts with
      | [] -> err_shiftv n
      | t :: ts when n >= t.len -> shiftv ts (n - t.len)
      | t :: ts -> shift t n :: ts

let blit src srcoff dst dstoff len =
  if len < 0 || srcoff < 0 || src.len - srcoff < len then
    failwith "BLIT"
  else if dstoff < 0 || dst.len - dstoff < len then
    failwith "BLIT"
  else
    Bytes.blit src.buffer (src.off+srcoff) dst.buffer
      (dst.off+dstoff) len

let fillv ~src ~dst =
  let rec aux dst n = function
    | [] -> n, []
    | hd::tl ->
        let avail = length dst in
        let first = length hd in
        if first <= avail then (
          blit hd 0 dst 0 first;
          aux (shift dst first) (n + first) tl
        ) else (
          blit hd 0 dst 0 avail;
          let rest_hd = shift hd avail in
          (n + avail, rest_hd :: tl)
        ) in
  aux dst 0 src

let rec sum_lengths_aux ~caller acc = function
  | [] -> acc
  | h :: t ->
     let sum = length h + acc in
     if sum < acc then invalid_arg caller
     else sum_lengths_aux ~caller sum t

let sum_lengths ~caller l = sum_lengths_aux ~caller 0 l

let lenv l = sum_lengths ~caller:"Bstruct.lenv" l

let concat = function
  | []   -> create 0
  | [cs] -> cs
  | css  ->
      let result = create (sum_lengths ~caller:"Cstruct.concat" css) in
      let aux off cs =
        let n = length cs in
        blit cs 0 result off n ;
        off + n in
      ignore @@ List.fold_left aux 0 css ;
      result
