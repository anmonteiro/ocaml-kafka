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

type producer
type consumer
type 'a response = ('a, Kafka.error * string) result

val produce
  :  producer
  -> Kafka.topic
  -> ?partition:Kafka.partition
  -> ?key:string
  -> string
  -> unit response Promise.t

val new_producer
  :  clock:Eio.Time.clock
  -> sw:Switch.t
  -> (string * string) list
  -> producer response

val new_consumer
  :  clock:Eio.Time.clock
  -> sw:Switch.t
  -> (string * string) list
  -> consumer response

val new_topic
  :  producer
  -> string
  -> (string * string) list
  -> Kafka.topic response

val consume
  :  sw:Switch.t
  -> topic:string
  -> consumer
  -> (Kafka.message, Stream.push) Stream.t response

val destroy_consumer : consumer -> unit
val destroy_producer : producer -> unit
