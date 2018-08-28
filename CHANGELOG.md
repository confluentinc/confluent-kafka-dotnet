
# 1.0.0-experimental

## New Features
- Added an AdminClient, providing `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `DescribeConfigs` and `AlterConfigs`.
- Can now produce / consume message headers.
- Can now produce user defined timestamps.
- Added `IClient`, `IProducer` and `IConsumer` interfaces (useful for dependency injection and mocking when writing tests).
- Added a `Handle` property to all clients classes:
  - Producers can utilize the underlying librdkafka handle from other Producers (replaces the 0.11.x `GetSerializingProducer` method on the `Producer` class).
  - `AdminClient` can utilize the underlying librdkafka handle from other `AdminClient`s, `Producer`s or `Consumer`s.
- `IDeserializer` now exposes message data via `ReadOnlySpan<byte>`, directly referencing librdkafka allocated memory. This results in considerable (up to 2x) performance and reduced memory.
- Most blocking operations now accept a `CancellationToken` parameter. 
  - TODO: in some cases there is no backing implementation yet.
- .NET Specific configuration parameters are all specified/documented in the `ConfigPropertyNames` class.


## Major Breaking API Changes
- The `Message` class has been re-purposed and now encapsulates specifically the message payload only.
  - `ProduceAsync` / `BeginProduce` now return a `DeliveryReport` object and `Consumer.Consume` returns a `ConsumeResult` object.
- The methods used to produce messages have changed:
  - Methods that accept a callback are now named `BeginProduce` (not `ProduceAsync`), analogous to similar methods in the standard library.
  - Callbacks are now specified as `Action<DeliveryReport<TKey, TValue>>` delegates, not implementations of `IDeliveryHandler`.
  - The `IDeliveryHandler` interface has been depreciated.
  - There are two variants of `ProduceAsync` and `BeginProduce`, the first takes a topic name and a `Message`. The second takes a `TopicPartition` and a message.
    - i.e. when producing, there is now clear separation between what is produced and where it is produced to.
  - The new API is more future proof.
  - `ProduceAsync` now calls `SetException` instead of `SetResult` on the returned `Task`, making error checking more convenient and less prone to developer mistakes.
- The feature to block `ProduceAsync` calls on local queue full has been removed (result in `Local_QueueFull` error). This should be implemented at the application layer if required.
- The non-serializing `Producer` and non-deserializing `Consumer` types have been removed (use generic types with `byte[]` instead), considerably reducing API surface area.
- The `ISerializingProducer` interface has been removed - you can achieve the same functionality by sharing client handles instead.
- The `Consumer.Poll` method and corresponding `OnMessage` event have been removed. You should use `Consumer` or `ConsumerAsync` instead.
- The `Consumer.OnPartitionEOF` event has been removed. You should inspect the `ConsumeResult` instance returned from `Consumer.Consume` instead.
- The `Consumer.OnConsumeError` has been removed. Consume errors are now exposed via a `ConsumeException`.
- The `Consumer.Consume` method now returns a `ConsumeResult` object, rather than a `Message` via an out parameter.
- Added `Consumer.ConsumeAsync` methods.
  - TODO: this is not finalized. there is an ongoing discussion relating to rebalence semantics.
- `CommitAsync` errors are now reported via an exception and method return values have correspondingly changed.
- `ListGroups`, `ListGroup`, `GetWatermarkOffsets`, `QueryWatermarkOffsets`, and `GetMetadata` have been removed from `Producer` and `Consumer` and exposed only via `AdminClient`.
  - TODO: these method signatures need work / extra capability and will be depreciated. we should try to update before 1.0.
- Added `Consumer.Close` and the `Consumer.Dispose` method no longer blocks.
  - TODO: yes it does! this needs fixing.
- Log handlers are now specified via configuration properties, not `OnLog` handlers. This allows logging to be enabled during client construction.
- Various methods that formerly returned `TopicPartitionOffsetError` / `TopicPartitionError` now return `TopicPartitionOffset` / `TopicPartition` and throw an exception in 
  case of error (with a `Result` property of type `TopicPartitionOffsetError` / `TopicPartitionError`).


## Minor Breaking API Changes
- Removed cast from `Error` to `bool`.
- `Consumer.OffsetsForTimes` if provided an empty collection will return an empty collection (not throw an exception).
- `manualPoll` argument has been removed from the `Producer` constructor and is now a configuration option.
- `enableDeliveryReports` argument has been removed from the `Producer` constructor and is now a configuration option.
- Removed methods with a `millisecondsTimeout` parameter (always preferring a `TimeSpan` parameter).
- Added `Consume` and `ConsumeAsync` variants without a timeout parameter (but with a `CancellationToken` parameter that is observed).
- Added A `Producer.Flush` method variant without a timeout parameter (but with a `CancellationToken` parameter that is observed).
- Added the `SyslogLevel` enumeration, which is used by the log handler delegate.


## Minor Enhancements / Fixes
- When delivery reports are disabled, `ProduceAsync` will return completed `Task`s rather than `Task`s that will never complete.
- Avro serializers / deserializer now handle `null` values.
- Examples upgraded to target 2.1.
- Changed name of `HasError` to `IsError`
- Configuration options have been added to allow fine-grained control over of marshalling of values to/from librdkafka (for high performance usage).
  - headers, message keys and values, timestamps and the topic name.
- Improved XML API documentation.


# 0.11.5 and previous

refer to the [release notes](https://github.com/confluentinc/confluent-kafka-dotnet/releases)
