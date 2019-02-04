# 1.0.0-beta3

## New Features

- Revamped producer and consumer serialization functionality.
  - All producer functionality except the public methods used to produce messages is now provided by `ProducerBase`.
  - There are two producer classes deriving from `ProducerBase`: `Producer` and `Producer<TKey, TValue>`.
    - `Producer` is specialized for the case of producing messages with `byte[]` keys and values.
    - `Producer<TKey, TValue>` provides flexible integration with serialization functionality.
  - On the consumer side, there are analogous classes: `ConsumerBase`, `Consumer` and `Consumer<TKey, TValue>`.
  - There are two types of serializer and deserializer: `ISerializer<T>` / `IAsyncSerializer<T>` and `IDeserializer<T>` / `IAsyncDeserializer<T>`.
    - `ISerializer<T>`/`IDeserializer<T>` are appropriate for most use cases.
    - `IAsyncSerializer<T>`/`IAsyncDeserializer<T>` are more general, but less performant (they return `Task`s).
    - The generic producer and consumer can be used with both types of serializer.
  - Changed the name of `Confluent.Kafka.Avro` to `Confluent.SchemaRegistry.Serdes` (Schema Registry may support other serialization formats in the future).
  - Added a example demonstrating working with protobuf serialized data.
- Avro serdes no longer make blocking calls to `ICachedSchemaRegistryClient` - everything is `await`ed.
- References librdkafka.redist [1.0.0-RC5](https://github.com/edenhill/librdkafka/releases/tag/v1.0.0-RC5)
  - Note: End of partition notification is now disabled by default (enable using the `EnablePartitionEof` config property).
- Removed `Consumer.OnPartitionEOF` in favor of `ConsumeResult.IsPartitionEOF`.
- Removed `ErrorEvent` class and added `IsFatal` to `Error` class. 
  - The `IsFatal` flag is now set appropriately for all errors (previously it was always set to `false`).
- Added `PersistenceStatus` property to `DeliveryResult`, which provides information on the persitence status of the message.

## Fixes

- Added `Close` method to `IConsumer` interface.
- Changed the name of `ProduceException.DeliveryReport` to `ProduceException.DeliveryResult`.


# 1.0.0-beta2

## New Features

- References librdkafka [1.0.0-PRE1](https://github.com/edenhill/librdkafka/tree/v1.0.0-PRE1). Highlights:
  - Idempotent producer.
  - Sparse connections (broker connections are only held open when in use).

## Enhancements / Fixes

- Fixed a memory leak in `ProduceAsync` [#640](https://github.com/confluentinc/confluent-kafka-dotnet/pull/640) (regression from 0.11.x).


# 1.0.0-beta

## New Features
- Added an AdminClient, providing `CreateTopics`, `DeleteTopics`, `CreatePartitions`, `DescribeConfigs` and `AlterConfigs`.
- Can now produce / consume message headers.
- Can now produce user defined timestamps.
- Added `IClient`, `IProducer` and `IConsumer` interfaces (useful for dependency injection and mocking when writing tests).
- Added a `Handle` property to all clients classes:
  - Producers can utilize the underlying librdkafka handle from other Producers (replaces the 0.11.x `GetSerializingProducer` method on the `Producer` class).
  - `AdminClient` can utilize the underlying librdkafka handle from other `AdminClient`s, `Producer`s or `Consumer`s.
- `IDeserializer` now exposes message data via `ReadOnlySpan<byte>`, directly referencing librdkafka allocated memory. This results in a considerable (up to 2x) performance increase and reduced memory.
- Most blocking operations now accept a `CancellationToken` parameter. 
  - TODO: in some cases there is no backing implementation yet.
- .NET Specific configuration parameters are all specified/documented in the `ConfigPropertyNames` class.


## Major Breaking API Changes
- The `Message` class has been re-purposed and now encapsulates specifically the message payload only.
  - `ProduceAsync` / `BeginProduce` now return a `DeliveryReport` object and `Consumer.Consume` returns a `ConsumeResult` object.
- The methods used to produce messages have changed:
  - Methods that accept a callback are now named `BeginProduce` (not `ProduceAsync`), analogous to similar methods in the standard library.
  - Callbacks are now specified as `Action<DeliveryReportResult<TKey, TValue>>` delegates, not implementations of `IDeliveryHandler`.
  - The `IDeliveryHandler` interface has been depreciated.
  - There are two variants of `ProduceAsync` and `BeginProduce`, the first takes a topic name and a `Message`. The second takes a `TopicPartition` and a message.
    - i.e. when producing, there is now clear separation between what is produced and where it is produced to.
  - The new API is more future proof.
  - `ProduceAsync` now calls `SetException` instead of `SetResult` on the returned `Task`, making error checking more convenient and less prone to developer mistakes.
- The feature to block `ProduceAsync` calls on local queue full has been removed (result in `Local_QueueFull` error). This should be implemented at the application layer if required.
- The non-serializing `Producer` and non-deserializing `Consumer` types have been removed (use generic types with `byte[]` instead), considerably reducing API surface area.
- The `ISerializingProducer` interface has been removed - you can achieve the same functionality by sharing client handles instead.
- The `Consumer.Poll` method and corresponding `OnMessage` event have been removed. You should use `Consumer.Consume` instead.
- The `Consumer.OnConsumeError` has been removed. Consume errors are now exposed via a `ConsumeException`.
- The `Consumer.Consume` method now returns a `ConsumeResult` object, rather than a `Message` via an out parameter.
- `CommitAsync` has been removed (use `Commit` instead).
- `Commit` errors are reported via an exception and method return values have correspondingly changed.
- `ListGroups`, `ListGroup`, `GetWatermarkOffsets`, `QueryWatermarkOffsets`, and `GetMetadata` have been removed from `Producer` and `Consumer` and exposed only via `AdminClient`.
- Added `Consumer.Close`.
- Various methods that formerly returned `TopicPartitionOffsetError` / `TopicPartitionError` now return `TopicPartitionOffset` / `TopicPartition` and throw an exception in 
  case of error (with a `Result` property of type `TopicPartitionOffsetError` / `TopicPartitionError`).


## Minor Breaking API Changes
- Removed cast from `Error` to `bool`.
- `Consumer.OffsetsForTimes` if provided an empty collection will return an empty collection (not throw an exception).
- `manualPoll` argument has been removed from the `Producer` constructor and is now a configuration option.
- `enableDeliveryReports` argument has been removed from the `Producer` constructor and is now a configuration option.
- Removed methods with a `millisecondsTimeout` parameter (always preferring a `TimeSpan` parameter).
- Added `Consumer.Consume` variants with a `CancellationToken` parameter.
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
