open Brr

let get_element_by_id s =
  Document.find_el_by_id G.document (Jstr.v s) |> Option.get

let eio_callback =
  let t = ref 0 in
  fun _ ->
    incr t;
    let rec aux t =
      let p = El.p [ El.txt' (string_of_int t) ] in
      El.append_children (Document.body G.document) [ p ];
      Eio_browser.Timeout.sleep ~ms:(t*1000);
      aux t
    in
    aux !t

let () =
  let other = get_element_by_id "other" in
  let clickable = get_element_by_id "clickable" in
  let _ : Ev.listener = Eio_browser.listen Ev.click eio_callback (El.as_target clickable) in
  Console.(log [ str "Running main event loop" ]);
  let main = Eio_browser.run @@ fun () ->
    while true do
      El.append_children other [ El.txt' "Append" ];
      Eio_browser.Timeout.sleep ~ms:1000
    done
  in
  Fut.await main (fun _ -> Console.(log [ str "Should not happen" ]))
