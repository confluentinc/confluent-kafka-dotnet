# Exactly Once Processing (WordCount)

**Note:** This example demonstrates how to achieve exactly once processing prior to KIP-447 (Apache Kafka <= 2.4.0, Confluent.Kafka <= 1.6.0). KIP-447 makes this significantly easier. 

The example demonstrates how to count the number of occurrences of each word in a given input set, where the input set can potentially be large enough that the computation needs to be distributed across many machines. This is an example of a streaming [map-reduce](https://en.wikipedia.org/wiki/MapReduce) calculation, with exactly once processing semantics.

Refer to comments in the code for commentary on the implementation. 


### Running the example:

For simplicity, the below instructions assume you're using a single Kafka broker with the default configuration running on `localhost`.

To remove all topics used by this example application:

```
dotnet run localhost:9092 del
```

To write some lines of text into the topic `lines`, rate limited to one per second for demonstration purposes:

```
dotnet run localhost:9092 gen
```

To process the lines of text in `lines`:

```
dotnet run localhost:9092 proc id1
```

This runs both the 'map' and 'reduce' steps of the calculation in separate threads:
- MAP: Read from a subset of the partitions of the `lines` topic (unordered), splits the lines of text into words and writes these to the `words` topic (partitioned by word).
- REDUCE: Read from a subset of the the partitions of the `words` topic and keeps a running tally of word counts. This state is stored permanently in the `counts` topic, but cached locally in RocksDb (to allow enable the aggregation, and ad-hoc lookup).

You can run multiple processing instances simultaneously with different id's to spread the load. e.g.:

```
dotnet run localhost:9092 proc id2
```
