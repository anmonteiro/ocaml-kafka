open Eio

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

val consume : consumer -> topic:string -> Kafka.message Stream.t response
val destroy_consumer : consumer -> unit
val destroy_producer : producer -> unit
