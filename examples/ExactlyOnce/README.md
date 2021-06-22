# Exactly Once Processing (WordCount)

This example demonstrates how to count the number of occurrences of each word in a given input set of lines, where the amount of data can potentially be large enough that the computation needs to be distributed across many machines. This is an example of a streaming [map-reduce](https://en.wikipedia.org/wiki/MapReduce) calculation, with **exactly once processing** semantics.

Refer to comments in the [code](Program.cs) for commentary on the implementation. 

### Running the example:

For simplicity, the instructions below assume that you're using a single Kafka broker with the default configuration running on `localhost`.

To remove all topics used by this example application:

```
dotnet run localhost:9092 del
```

To write some lines of text into the topic `lines` (auto-created if it doesn't exist), rate limited to one per second for demonstration purposes:

```
dotnet run localhost:9092 gen
```

To run the "map" part of the calculation, which splits the input line text into words and writes them in the partitioned Kafka topic `words`:

```
dotnet run localhost:9092 map map_client_id_1
```

You can run multiple instances of this stage, specifying different client id's for each. If you use the same client id, the new process
will "fence" the old one with the same id.

This is an example of a stateless stream processor.

To run the "reduce" part of the calculation, which counts the number of occurrences of each word and writes updates to the compacted, partitiond Kafka topic `counts`:

```
dotnet run localhost:9092 reduce reduce_client_id_1
```

You can parallelize this stage as well by running multiple instances with different client id's.

This is an example of a stateful stream processor, where the working state is materialized into a local FASTER store.

You can dynamically start and stop the map and reduce processing instances at any time and watch as the workload rebalances. Both the map and reduce stages make use of [incremental rebalancing](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/), which minimizes the impact of rebalances on processing.
