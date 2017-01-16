open System
open System.Text
open System.Collections.Generic
open Confluent.Kafka
open Confluent.Kafka.Serialization

[<EntryPoint>]
let main argv =
    let conf = new Dictionary<string, Object>()
    conf.Add("bootstrap.servers", argv.[0])
    use producer = new Producer<Null, string>(conf, null, new StringSerializer(Encoding.UTF8))
    let dr = producer.ProduceAsync(argv.[1], null, argv.[2])
    dr.Wait()
    0
