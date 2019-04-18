# 1.0.0-RC5

## Changes

- Reverted RC4 changes.
- Renamed AvroSerializer to AsyncAvroSerializer and AvroDeserializer to AsyncAvroDeserializer
- Added SyncOverAsyncSerializer and SyncOverAsyncDeserializer adapter classes.
- Added AsSyncOverAsync factory method to AsyncAvroSerializer and AsyncAvroDeserializer.
- Removed IAsyncDeserializer setter overloads from the ConsumerBuilder class.
- Renamed Producer.BeginProduce to Producer.Produce.
- Produce throws an exception if used when async serializers are configured.


# 1.0.0-RC4

## Changes

- Removed `SerializationContext` from non-async serde interfaces.
- Replaced `ISerializer` interface with `Serializer` delegate.
- Replaced `IDeserializer` interface with `Deserializer` delegate.


# 1.0.0-RC3

## New Features

- `Producer.Poll` can now be used with producer instances that are in background polling mode.
  - Typically use: Block for a minimal period of time following a `ErrorCode.Local_QueueFull` error.

## Changes

- Removed the `Confluent.Kafka.Serdes` namespace.

## Fixes

- Added `CompressionType` property to `ProducerConfig` class.


# 1.0.0-RC2

## New Features

- References librdkafka.redist [v1.0.0](https://github.com/edenhill/librdkafka/releases/tag/v1.0.0)

## Changes

- Moved API docs from the client classes to their respective interfaces.
- Update formatting of client API docs so they display well in Visual Studio Code intellisense.


# 1.0.0-RC1

## New Features

- Added GET subject versions to the cached schema registry client.
- References librdkafka.redist [1.0.0-RC9](https://github.com/edenhill/librdkafka/releases/tag/v1.0.0-RC9)
  - supports apline linux out-of-the-box.
  - fallback support (that excludes security features) for most linux distributions previously unsuppored out-of-the-box.
  - fixed a dependency issue on MacOS

## Changes

- A new rebalance API.
  - `SetRebalanceHandler` has been split into `SetPartitionsAssignedHandler` and `SetPartitionsRevokedHandler`.
  - Calling of `Assign`/`Unassign` in these handlers is prohibited.
  - Partitions to read from / start offsets can be optionally specified manually via the return value from these handlers.
- The `Message.PersistenceStatus` property name has changed to `Message.Status`.
- Moved the `GetWatermarkOffsets` and `QueryWatermarkOffsets` methods from admin client to consumer.
- Context is now provided to serdes via a `SerializationContext` class instance.

## Fixes

- Corrected an error in the `rd_kafka_event_type` method signature which was causing incompatibility with mono.
- Audited exception use across the library and made changes in various places where appropriate.
- Removed unused `CancellationToken` parameters (we will add them back when implemented).
- Builder classes now return interfaces, not concrete classes.
- Removed the dependency on `CompilerServices.Unsafe` which was causing `ProduceAsync` to hang in some scenarios.
- Fixed a deadlock-on-dispose issue in `AdminClient`.
- Made `Producer.ProduceAsync` async.


# 1.0.0-beta3

## New Features

- Revamped producer and consumer serialization functionality.
  - There are now two types of serializer and deserializer: `ISerializer<T>` / `IAsyncSerializer<T>` and `IDeserializer<T>` / `IAsyncDeserializer<T>`.
    - `ISerializer<T>`/`IDeserializer<T>` are appropriate for most use cases. 
    - `IAsyncSerializer<T>`/`IAsyncDeserializer<T>` are async friendly, but less performant (they return `Task`s).
  - Changed the name of `Confluent.Kafka.Avro` to `Confluent.SchemaRegistry.Serdes` (Schema Registry may support other serialization formats in the future).
  - Added an example demonstrating working with protobuf serialized data.
- `Consumer`s, `Producer`s and `AdminClient`s are now constructed using builder classes.
  - This is more verbose, but provides a sufficiently flexible and future proof API for specifying serdes and other configuration information.
  - All `event`s on the client classes have been replaced with corresponding `Set...Handler` methods on the builder classes.
    - This allows (enforces) handlers are set on librdkafka initialization (which is important for some handlers, particularly the log handler).
    - `event`s allow for more than one handler to be set, but this is often not appropriate (e.g. `OnPartitionsAssigned`), and never necessary. This is no longer possible.
    - `event`s are also not async friendly (handlers can't return `Task`). The Set...Handler appropach can be extend in such a way that it is.
- Avro serdes no longer make blocking calls to `ICachedSchemaRegistryClient` - everything is `await`ed.
  - Note: The `Consumer` implementation still calls async deserializers synchronously because the `Consumer` API is still otherwise fully synchronous.
- Reference librdkafka.redist [1.0.0-RC7](https://github.com/edenhill/librdkafka/releases/tag/v1.0.0-RC7)
  - Notable features: idempotent producer, sparse connections, KIP-62 (max.poll.interval.ms).
  - Note: End of partition notification is now disabled by default (enable using the `EnablePartitionEof` config property).
- Removed the `Consumer.OnPartitionEOF` event in favor notifying of partition eof via `ConsumeResult.IsPartitionEOF`.
- Removed `ErrorEvent` class and added `IsFatal` to `Error` class. 
  - The `IsFatal` flag is now set appropriately for all errors (previously it was always set to `false`).
- Added `PersistenceStatus` property to `DeliveryResult`, which provides information on the persitence status of the message.

## Fixes

- Added `Close` method to `IConsumer` interface.
- Changed the name of `ProduceException.DeliveryReport` to `ProduceException.DeliveryResult`.
- Fixed bug where enum config property couldn't be read after setting it.
- Added `SchemaRegistryBasicAuthCredentialsSource` back into `SchemaRegistryConfig` (#679).
- Fixed schema registry client failover connection issue (#737).
- Improvements to librdkafka dependnecy discovery (#743).


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
