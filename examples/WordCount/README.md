# Exactly Once Processing Example - WordCount

## Running:

The following assumes you're using a Kafka broker with the default configuration running on `localhost`.

To remove all topics used by this example application:

```
dotnet run localhost:9092 del
```

To write lines of text into the topic `line-input`, rate limited to one per second for demonstration purposes:

```
dotnet run localhost:9092 gen
```

To process the lines of text in `line-input`:

```
dotnet run localhost:9092 proc id1
```

You can run multiple processing instances simultaneously with different id's to spread the load. e.g.

```
dotnet run localhost:9092 proc id2
```

The processing instances run three threads concurrently:

1. Read from topic `line-input`, split into words, and write to topic `words`.
2. Read from topic `words`, write counts to topic `counts` (a compacted topic). This state is also cached locally in RocksDb to enable aggregation.
3. Read from the local RocksDb store and display the words with the highest counts known to the instance periodically.

## Notes:

- This example demonstrates 'streaming map reduce' with exactly once processing semantics.
    - MAP: Processor #1 reads from a subset of the partitions of the `line-data` topic (unordered), splits the lines of text into words writes these to the `words` topic, (partitioned by word).
    - REDUCE: Processor #2 reads from a subset of the the partitions of the `words` topic and keeps a running tally. This state is stored permanently in the `counts` topic, but cached locally in RocksDb (to allow enable the aggregation, and ad-hoc lookup).
- Consumer groups are utilized to enable dynamic scaling of processing.
- The topic `counts` is compacted.
