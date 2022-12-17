open Effect

type parcap = int

class virtual t = object
  method virtual register : name:string -> parcap
  method virtual submit : 'a 'b. parcap -> (unit -> 'a) -> 'a
  method virtual run : 'a. (unit -> 'a) -> 'a
  method virtual run_raw : 'a. (unit -> 'a) -> 'a
end

module type Op = sig
  type t

  type value

  val pp : t Fmt.t

  val name : string

  val handle : t -> value
end

(* module Par (Op : Op) = struct
  type _ Effect.t += Par : Op.t -> Op.value Effect.t
  (* type _ Effect.t += Seq : (('a -> 'b) * 'a) -> 'b Effect.t *)
  (* let pipe f v = Effect.perform (Seq (f, v)) *)

  let task op = Effect.perform (Par op)

  let run : type a. #t -> (unit -> a) -> a = fun t fn ->
    let open Effect.Deep in
    let par_cap = t#register ~name:Op.name in
      let fork fn =
      match_with fn () {
        retc = Fun.id;
        exnc = raise;
        effc = fun (type a) (e : a Effect.t) -> match e with
        | Par op -> Some (fun (k : (a, _) continuation) ->
          t#submit par_cap (fun () -> Op.handle op) k
        )
        | _ -> None
      }
    in
    let res = ref None in
    let `Exit_scheduler = 
      fork (fun () -> 
        let v = fn () in
        res := Some v;
        `Exit_scheduler)
    in 
    Option.get !res
end *)

let submit (t : #t) = t#submit

let register (t : #t) = t#register

let run_raw (t : #t) = t#run_raw

let run (t : #t) fn =
  let ctx = perform Private.Effects.Get_context in
  Cancel.check (Private.Fiber_context.cancellation_context ctx);
  let cancelled, set_cancelled = Promise.create () in
  Private.Fiber_context.set_cancel_fn ctx (Promise.resolve set_cancelled);
  (* If the spawning fiber is cancelled, [cancelled] gets set to the exception. *)
  match
    t#run @@ fun () ->
    Fiber.first
      (fun () ->
         match Promise.await cancelled with
         | Cancel.Cancelled ex -> raise ex    (* To avoid [Cancelled (Cancelled ex))] *)
         | ex -> raise ex (* Shouldn't happen *)
      )
      fn
  with
  | x ->
    ignore (Private.Fiber_context.clear_cancel_fn ctx : bool);
    x
  | exception ex ->
    ignore (Private.Fiber_context.clear_cancel_fn ctx : bool);
    match Promise.peek cancelled with
    | Some (Cancel.Cancelled ex2 as cex) when ex == ex2 ->
      (* We unwrapped the exception above to avoid a double cancelled exception.
         But this means that the top-level reported the original exception,
         which isn't what we want. *)
      raise cex
    | _ -> raise ex
