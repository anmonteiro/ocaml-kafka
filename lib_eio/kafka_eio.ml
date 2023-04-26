open Eio.Std

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
    Fiber.fork ~sw (fun () ->
        try
          while true do
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

  let rec iter_p
      : type kind. sw:Switch.t -> f:('a -> unit) -> ('a, kind) t -> unit
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

type producer =
  { handler : Kafka.handler
  ; pending_msg : (int, unit Promise.t * unit Promise.u) Hashtbl.t
  ; stop_poll : unit Promise.t * unit Promise.u
  }

type consumer =
  { handler : Kafka.handler
  ; start_poll : unit Promise.t * unit Promise.u
  ; stop_poll : unit Promise.t * unit Promise.u
  ; subscriptions : (string, (Kafka.message, Stream.push) Stream.t) Hashtbl.t
  }

let next_msg_id =
  let n = ref 1 in
  fun () ->
    let id = !n in
    n := id + 1;
    id

let poll_interval = 0.050

external produce'
  :  Kafka.topic
  -> ?partition:Kafka.partition
  -> ?key:string
  -> msg_id:Kafka.msg_id
  -> string
  -> unit response
  = "ocaml_kafka_produce"

external poll' : Kafka.handler -> int = "ocaml_kafka_async_poll"

let produce (t : producer) topic ?partition ?key msg =
  let msg_id = next_msg_id () in
  let p, u = Promise.create () in
  Hashtbl.replace t.pending_msg msg_id (p, u);
  match produce' topic ?partition ?key ~msg_id msg with
  | Error _ as e ->
    Hashtbl.remove t.pending_msg msg_id;
    Promise.create_resolved e
  | Ok () ->
    let ret = Promise.await p in
    Promise.create_resolved (Ok ret)

external new_producer'
  :  (Kafka.msg_id -> Kafka.error option -> unit)
  -> (string * string) list
  -> Kafka.handler response
  = "ocaml_kafka_async_new_producer"

let handle_producer_response pending_msg msg_id _maybe_error =
  match Hashtbl.find_opt pending_msg msg_id with
  | Some (_, u) ->
    Hashtbl.remove pending_msg msg_id;
    Promise.resolve u ()
  | None -> ()

let new_producer ~clock ~sw xs =
  let pending_msg = pending_table () in
  let stop_poll, resolve_stop_poll = Promise.create () in
  match new_producer' (handle_producer_response pending_msg) xs with
  | Ok handler ->
    Async.every ~clock ~sw ~stop:stop_poll poll_interval (fun () ->
        ignore (poll' handler));
    Ok { handler; pending_msg; stop_poll = stop_poll, resolve_stop_poll }
  | Error _ as e -> e

external new_consumer'
  :  (string * string) list
  -> Kafka.handler response
  = "ocaml_kafka_async_new_consumer"

external consumer_poll'
  :  Kafka.handler
  -> Kafka.message option response
  = "ocaml_kafka_async_consumer_poll"

let handle_incoming_message subscriptions = function
  | None | Some (Kafka.PartitionEnd _) -> ()
  | Some (Kafka.Message (topic, _, _, _, _) as msg) ->
    let topic_name = Kafka.topic_name topic in
    (match Hashtbl.find_opt subscriptions topic_name with
    | None -> ()
    | Some writer -> Stream.push writer msg)

let new_consumer ~clock ~sw xs =
  let subscriptions = Hashtbl.create (8 * 1024) in
  let stop_poll, resolve_stop_poll = Promise.create () in
  let start_poll, resolve_start_poll = Promise.create () in
  match new_consumer' xs with
  | Ok handler ->
    Async.every
      ~clock
      ~sw
      ~start:start_poll
      ~stop:stop_poll
      poll_interval
      (fun () ->
        match consumer_poll' handler with
        | Error _ -> traceln "Issue with polling"
        | Ok success -> handle_incoming_message subscriptions success);
    Ok
      { handler
      ; subscriptions
      ; start_poll = start_poll, resolve_start_poll
      ; stop_poll = stop_poll, resolve_stop_poll
      }
  | Error _ as e -> e

external subscribe'
  :  Kafka.handler
  -> topics:string list
  -> unit response
  = "ocaml_kafka_async_subscribe"

let consume ~sw ~topic (consumer : consumer) =
  match Hashtbl.mem consumer.subscriptions topic with
  | true -> Error (Kafka.FAIL, "Already subscribed to this topic")
  | false ->
    if not (Promise.is_resolved (fst consumer.start_poll))
    then Promise.resolve (snd consumer.start_poll) ();
    let subscribe_error = ref None in
    let reader =
      let stream = Stream.create 10 in
      Hashtbl.add consumer.subscriptions topic stream;
      stream
    in
    Fiber.fork ~sw (fun () ->
        let topics =
          Hashtbl.to_seq_keys consumer.subscriptions |> List.of_seq
        in
        match subscribe' consumer.handler ~topics with
        | Ok () ->
          Promise.await (fst consumer.stop_poll);
          Stream.close reader
        | Error e -> subscribe_error := Some e);
    Fiber.fork ~sw (fun () ->
        Promise.await (Stream.closed reader);
        Hashtbl.remove consumer.subscriptions topic;
        let remaining_subs =
          Hashtbl.to_seq_keys consumer.subscriptions |> List.of_seq
        in
        ignore @@ subscribe' consumer.handler ~topics:remaining_subs);
    (match Stream.is_closed reader with
    | false -> Ok reader
    | true ->
      (match !subscribe_error with
      | None -> Error (Kafka.FAIL, "Programmer error, subscribe_error unset")
      | Some e -> Error e))

let new_topic (producer : producer) name opts =
  match Kafka.new_topic producer.handler name opts with
  | v -> Ok v
  | exception Kafka.Error (e, msg) -> Error (e, msg)

let destroy_consumer consumer =
  Promise.resolve (snd consumer.stop_poll) ();
  Kafka.destroy_handler consumer.handler

let destroy_producer (producer : producer) =
  Promise.resolve (snd producer.stop_poll) ();
  Kafka.destroy_handler producer.handler
