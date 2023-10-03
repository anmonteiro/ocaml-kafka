open Eio.Std

let default_poll_interval = 0.050

module Async = struct
  exception Local

  let default_start = Promise.create_resolved ()

  let every ~clock ~sw ?(start = default_start) ?stop interval f =
    Promise.await start;
    let stop =
      match stop with
      | None ->
        let never, _ = Promise.create () in
        never
      | Some stop -> stop
    in
    Fiber.fork_promise ~sw (fun () ->
        try
          while not (Promise.is_resolved stop) do
            match Promise.is_resolved stop with
            | true -> raise Local
            | false ->
              f ();
              Eio.Time.sleep clock interval
          done
        with
        | Local -> ())
end

module Stream : sig
  type push
  type from
  type ('a, 'kind) t

  val empty : unit -> ('a, push) t
  val from : f:(unit -> 'a option) -> ('a, from) t
  val create : int -> ('a, push) t
  val push : ('a, push) t -> 'a -> unit
  val close : _ t -> unit
  val closed : _ t -> unit Eio.Promise.t
  val when_closed : f:(unit -> unit) -> _ t -> unit
  val is_closed : _ t -> bool
  val take : ('a, _) t -> 'a option
  val of_list : 'a list -> ('a, push) t
  val to_list : ('a, _) t -> 'a list
  val map : f:('a -> 'b) -> ('a, _) t -> ('b, from) t
  val iter : f:('a -> unit) -> ('a, _) t -> unit
  val iter_p : sw:Eio.Switch.t -> f:('a -> unit) -> ('a, _) t -> unit
  val fold : f:('acc -> 'a -> 'acc) -> init:'acc -> ('a, _) t -> 'acc
  val drain : _ t -> unit
  val drain_available : _ t -> unit
end = struct
  open Eio.Std

  type push = |
  type from = |

  type ('a, _) kind =
    | From : (unit -> 'a option) -> ('a, from) kind
    | Push :
        { stream : 'a Eio.Stream.t
        ; capacity : int
        }
        -> ('a, push) kind

  type ('a, 'b) t =
    { stream : ('a, 'b) kind
    ; is_closed : bool Atomic.t
    ; closed : unit Promise.t * unit Promise.u
    }

  let unsafe_eio_stream { stream; _ } =
    match stream with Push { stream; _ } -> stream

  let is_closed { is_closed; _ } = Atomic.get is_closed

  let close t =
    if not (is_closed t)
    then (
      let { closed = _, u; _ } = t in
      Atomic.set t.is_closed true;
      Promise.resolve u ())

  let push t item =
    assert (not (is_closed t));
    let stream = unsafe_eio_stream t in
    Eio.Stream.add stream item

  let create capacity =
    let stream = Eio.Stream.create capacity in
    let t =
      { stream = Push { stream; capacity }
      ; is_closed = Atomic.make false
      ; closed = Promise.create ()
      }
    in
    t

  let empty () =
    let t = create 0 in
    close t;
    t

  let from ~f =
    { stream = From f
    ; is_closed = Atomic.make false
    ; closed = Promise.create ()
    }

  let closed t =
    let { closed = p, _; _ } = t in
    p

  let when_closed ~f t =
    Promise.await (closed t);
    f ()

  let of_list xs =
    let stream = create (List.length xs) in
    List.iter (Eio.Stream.add (unsafe_eio_stream stream)) xs;
    (* TODO(anmonteiro): should this return a closed stream? *)
    stream

  let take : type kind. ('a, kind) t -> 'a option =
   fun t ->
    match t.stream with
    | From f ->
      (match f () with
      | Some _ as item -> item
      | None ->
        close t;
        None)
    | Push { stream; _ } ->
      Fiber.first
        (fun () -> Some (Eio.Stream.take stream))
        (fun () ->
          let { closed = p, _; _ } = t in
          Promise.await p;
          None)

  let take_nonblocking : type kind. ('a, kind) t -> 'a option =
   fun t ->
    match t.stream with
    | From _f -> None
    | Push { stream; _ } -> Eio.Stream.take_nonblocking stream

  let map ~f t =
    from ~f:(fun () ->
        match take t with Some item -> Some (f item) | None -> None)

  let rec iter : type kind. f:('a -> unit) -> ('a, kind) t -> unit =
   fun ~f t ->
    match t.stream with
    | Push { capacity = 0; _ } when is_closed t -> ()
    | Push _ | From _ ->
      (match take t with
      | Some item ->
        f item;
        iter ~f t
      | None -> ())

  let rec iter_p :
      type kind. sw:Switch.t -> f:('a -> unit) -> ('a, kind) t -> unit
    =
   fun ~sw ~f t ->
    match t.stream with
    | Push { capacity = 0; _ } when is_closed t -> ()
    | Push _ | From _ ->
      (match take t with
      | Some item ->
        let result = Fiber.fork_promise ~sw (fun () -> f item)
        and rest = Fiber.fork_promise ~sw (fun () -> iter_p ~sw ~f t) in
        Promise.await_exn result;
        Promise.await_exn rest
      | None -> ())

  let fold ~f ~init t =
    let rec loop ~f ~acc t =
      match take t with Some item -> loop ~f ~acc:(f acc item) t | None -> acc
    in
    loop ~f ~acc:init t

  let to_list t =
    let lst = fold ~f:(fun acc item -> item :: acc) ~init:[] t in
    List.rev lst

  let drain t = iter ~f:ignore t

  let rec drain_available t =
    match take_nonblocking t with Some _ -> drain_available t | None -> ()
end

let pending_table () = Hashtbl.create (8 * 1024)

type 'a response = ('a, Kafka.error * string) result

module Producer = struct
  type t =
    { handle : Kafka.handler
    ; pending_msg : (int, (unit, Kafka.error * string) result Promise.t * (unit, Kafka.error * string) result Promise.u) Hashtbl.t
    ; stop_poll : unit Promise.t * unit Promise.u
    }

  let handle t = t.handle

  external poll' : Kafka.handler -> int = "ocaml_kafka_eio_poll"

  external new_producer' :
     delivery_cb:(Kafka.msg_id -> Kafka.error option -> unit)
    -> (string * string) list
    -> Kafka.handler response
    = "ocaml_kafka_eio_new_producer"

let delivery_callback pending_msg_tbl msg_id error =
  match Hashtbl.find pending_msg_tbl msg_id with
  | (_, u) ->
    Hashtbl.remove pending_msg_tbl msg_id;
    (match error with
    | None ->
      Promise.resolve_ok u ()
    | Some error ->
      Promise.resolve_error u (error, "Failed to produce message"))
  | exception Not_found -> ()

  let create ~clock ~sw ?(poll_interval = default_poll_interval) xs =
    let pending_msg = pending_table () in
    let stop_poll, resolve_stop_poll = Promise.create () in
    match new_producer' ~delivery_cb:(delivery_callback pending_msg) xs with
    | Ok handle ->
      Fiber.fork ~sw (fun () ->
          match
            Promise.await
              (Async.every ~clock ~sw ~stop:stop_poll poll_interval (fun () ->
                   ignore (poll' handle)))
          with
          | Ok () -> Kafka.destroy_handler handle
          | Error _ -> assert false);
      Ok { handle; pending_msg; stop_poll = stop_poll, resolve_stop_poll }
    | Error _ as e -> e

  let destroy t =
    let _, resolve_stop_poll = t.stop_poll in
    Promise.resolve resolve_stop_poll ()
end

module Consumer = struct
  type t =
    { handle : Kafka.handler
    ; start_poll : unit Promise.t * unit Promise.u
    ; stop_poll : unit Promise.t * unit Promise.u
    ; subscriptions : (string, (Kafka.message, Stream.push) Stream.t) Hashtbl.t
    }

  let handle t = t.handle

  external new_consumer' :
     ?rebalance_callback:(op:Kafka.Rebalance.op -> Kafka.partition_list -> unit)
    -> (string * string) list
    -> Kafka.handler response
    = "ocaml_kafka_eio_new_consumer"

  external poll' :
     Kafka.handler
    -> Kafka.message option response
    = "ocaml_kafka_eio_consumer_poll"

  let handle_incoming_message subscriptions = function
    | None | Some (Kafka.PartitionEnd _) -> ()
    | Some (Kafka.Message { topic; _ } as msg) ->
      let topic_name = Kafka.topic_name topic in
      (match Hashtbl.find_opt subscriptions topic_name with
      | None -> ()
      | Some writer -> Stream.push writer msg)

  let create
      ~clock
      ~sw
      ?rebalance_callback
      ?(poll_interval = default_poll_interval)
      xs
    =
    let subscriptions = Hashtbl.create (8 * 1024) in
    let stop_poll, resolve_stop_poll = Promise.create () in
    let start_poll, resolve_start_poll = Promise.create () in
    match new_consumer' ?rebalance_callback xs with
    | Ok handle ->
      Fiber.fork ~sw (fun () ->
          match
            Promise.await
            @@ Async.every
                 ~clock
                 ~sw
                 ~start:start_poll
                 ~stop:stop_poll
                 poll_interval
                 (fun () ->
                   match poll' handle with
                   | Error _ -> traceln "Issue with polling"
                   | Ok maybe_message ->
                     handle_incoming_message subscriptions maybe_message)
          with
          | Ok () -> ()
          | Error _ -> assert false);
      Ok
        { handle
        ; subscriptions
        ; start_poll = start_poll, resolve_start_poll
        ; stop_poll = stop_poll, resolve_stop_poll
        }
    | Error _ as e -> e

  let destroy t =
    let _, resolve_stop_poll = t.stop_poll in
    Promise.resolve resolve_stop_poll ()
end

let next_msg_id =
  let n = Atomic.make 1 in
  fun () -> Atomic.fetch_and_add n 1

let produce (t : Producer.t) topic ?partition ?key msg =
  let msg_id = next_msg_id () in
  let p, u = Promise.create () in
  Hashtbl.replace t.pending_msg msg_id (p, u);
  Kafka.produce topic ?partition ?key ~msg_id msg;
  p

external subscribe' :
   Kafka.handler
  -> topics:string list
  -> unit response
  = "ocaml_kafka_eio_subscribe"

let consume ~sw ~topic ?(capacity = 256) (consumer : Consumer.t) =
  match Hashtbl.mem consumer.subscriptions topic with
  | true -> Error (Kafka.FAIL, "Already subscribed to this topic")
  | false ->
    assert (not (Promise.is_resolved (fst consumer.start_poll)));
    Promise.resolve (snd consumer.start_poll) ();
    let subscribe_error = ref None in
    let reader =
      let stream = Stream.create capacity in
      Hashtbl.add consumer.subscriptions topic stream;
      stream
    in
    Fiber.fork ~sw (fun () ->
        let topics =
          Hashtbl.to_seq_keys consumer.subscriptions |> List.of_seq
        in
        match subscribe' consumer.handle ~topics with
        | Ok () ->
          Promise.await (fst consumer.stop_poll);
          Hashtbl.remove consumer.subscriptions topic;
          let remaining_subs =
            Hashtbl.to_seq_keys consumer.subscriptions |> List.of_seq
          in
          subscribe' consumer.handle ~topics:remaining_subs |> Result.get_ok;
          Stream.close reader;
          Kafka.destroy_handler consumer.handle
        | Error e -> subscribe_error := Some e);
    (match Stream.is_closed reader with
    | false -> Ok reader
    | true ->
      (match !subscribe_error with
      | None -> Error (Kafka.FAIL, "Programmer error, subscribe_error unset")
      | Some e -> Error e))

let new_topic (producer : Producer.t) ?partitioner_callback name opts =
  match Kafka.new_topic ?partitioner_callback producer.handle name opts with
  | v -> Ok v
  | exception Kafka.Error (e, msg) -> Error (e, msg)
