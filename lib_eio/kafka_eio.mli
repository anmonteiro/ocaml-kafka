open Eio.Std

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
end

module Producer : sig
  type t

  val create :
     clock:_ Eio.Time.clock
    -> sw:Switch.t
    -> ?poll_interval:float
    -> (string * string) list
    -> (t, Kafka.Error.t) result

  val handle : t -> Kafka.handler
  val destroy : t -> unit
end

module Consumer : sig
  type t

  val create :
     clock:_ Eio.Time.clock
    -> sw:Switch.t
    -> ?rebalance_callback:
         (op:Kafka.Rebalance.op -> Kafka.partition_list -> unit)
    -> ?poll_interval:float
    -> (string * string) list
    -> (t, Kafka.Error.t) result

  val handle : t -> Kafka.handler
  val destroy : t -> unit
end

val produce :
   Producer.t
  -> Kafka.topic
  -> ?partition:Kafka.partition
  -> ?key:string
  -> string
  -> (unit, Kafka.Error.t) result

val new_topic :
   Producer.t
  -> ?partitioner_callback:(int -> string -> int option)
  -> string
  -> (string * string) list
  -> (Kafka.topic, Kafka.Error.t) result

val consume :
   sw:Switch.t
  -> topic:string
  -> ?capacity:int
  -> Consumer.t
  -> ((Kafka.message, Stream.push) Stream.t, Kafka.Error.t) result
