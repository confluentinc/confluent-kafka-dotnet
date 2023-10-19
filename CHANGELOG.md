# 2.3.0

## Enhancements

- References librdkafka.redist 2.3.0. Refer to the [librdkafka v2.3.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.3.0) for more information.
- [KIP-430](https://cwiki.apache.org/confluence/display/KAFKA/KIP-430+-+Return+Authorized+Operations+in+Describe+Responses):
  Return authorized operations in describe responses (#2021, @jainruchir).
- [KIP-396](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=97551484): Added support for ListOffsets Admin API (#2086).
- Add `Rack` to the `Node` type, so AdminAPI calls can expose racks for brokers (currently, all Describe 
  Responses) (#2021, @jainruchir).
- Added support for external JSON schemas in `JsonSerializer` and `JsonDeserializer` (#2042).
- Added compatibility methods to CachedSchemaRegistryClient ([ISBronny](https://github.com/ISBronny), #2097).
- Add support for AdminAPI `DescribeCluster()` and `DescribeTopics()` (#2021, @jainruchir).


# 2.2.0

## Enhancements

- References librdkafka.redist 2.2.0. Refer to the [librdkafka v2.2.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.2.0) for more information.
- [KIP-339](https://cwiki.apache.org/confluence/display/KAFKA/KIP-339%3A+Create+a+new+IncrementalAlterConfigs+API)
  IncrementalAlterConfigs API (#2005).
- [KIP-554](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554%3A+Add+Broker-side+SCRAM+Config+API):
    User SASL/SCRAM credentials alteration and description (#2070).


## Fixes

- Fix backwards compatability of TopicPartitionOffset constructor. ([drinehimer](https://github.com/drinehimer), #2066)
- Fix IConsumer breaking change. ([ttd2089](https://github.com/ttd2089), #2071)


# 2.1.1

## Enhancements

- References librdkafka.redist 2.1.1. Refer to the [librdkafka v2.1.1 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.1.1) for more information.
- Less heap allocations when calling Produce ([bjornbouetsmith](https://github.com/bjornbouetsmith), #2020)

# 2.1.0

## Enhancements

- References librdkafka.redist 2.1.0. Refer to the [librdkafka v2.1.0 release notes](https://github.com/confluentinc/librdkafka/releases/tag/v2.1.0) for more information.
- Added SetSaslCredentials. This new method (on the Producer, Consumer, and AdminClient) allows modifying the stored
  SASL PLAIN/SCRAM credentials that will be used for subsequent (new) connections to a broker (#1980).
- Changed the way the `_SCHEMA` filed is accessed internally from reflecting the static field to accessing it from the instance ([AlexeyRaga](https://github.com/AlexeyRaga)).
- [KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation): add offset leader epoch fields to the TopicPartitionOffset,
  TopicPartitionOffsetError and ConsumeResult classes (#2027).

## Fixes

- Fixed `OverflowException` thrown intermittently when using the `ListGroup` method (#2003).


# 2.0.2

## Upgrade considerations

OpenSSL 3.0.x upgrade in librdkafka requires a major version bump, as some legacy ciphers need to be explicitly configured to continue working, but it is highly recommended NOT to use them. The rest of the API remains backward compatible.

## Enhancements

- References librdkafka.redist 2.0.2. Refer to the [librdkafka v2.0.0 release notes](https://github.com/edenhill/librdkafka/releases/tag/v2.0.0) and later ones for more information.
- Upgraded `NJsonSchema` to v10.6.3
- Added `LatestCompatibilityStrict` configuration property to JsonSerializerConfig to check the compatibility with latest schema
  when `UseLatestVersion` is set to true.
- Added DeleteConsumerGroupOffset to AdminClient.
- [KIP-222](https://cwiki.apache.org/confluence/display/KAFKA/KIP-222+-+Add+Consumer+Group+operations+to+Admin+API)
   Finish remaining implementation: Add Consumer Group operations to Admin API (`DeleteGroups` is already present).
- [KIP-518](https://cwiki.apache.org/confluence/display/KAFKA/KIP-518%3A+Allow+listing+consumer+groups+per+state)
   Allow listing consumer groups per state.
- [KIP-396](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=97551484)
   Partially implemented: support for AlterConsumerGroupOffsets.
- As result of the above KIPs, added (#1981)
   - `ListConsumerGroups` Admin operation. Supports listing by state.
   - `DescribeConsumerGroups` Admin operation. Supports multiple groups.
   - `ListConsumerGroupOffsets` Admin operation. Currently, only supports
      1 group with multiple partitions. Supports the `requireStable` option.
   - `AlterConsumerGroupOffsets` Admin operation. Currently, only supports
      1 group with multiple offsets.

## Fixes

- During a group rebalance, partitions are now always revoked as a side effect of a call to Consume, whether or not a partitions revoked handler has been specified. Previously, if no handler was specified, the timing of when the consumer lost ownership of partitions during a rebalance was arbitrarily, frequently resulting in an erroneous state exception when committing or storing offsets.
- Fixed 100% CPU usage with `DependentAdminClientBuilder`.


**Note: There were no 2.0.0 and 2.0.1 releases.**


# 1.9.3 

## Enhancements

- Added `NormalizeSchemas` configuration property to the Avro, Json and Protobuf serdes.

## Fixes

- Schema Registry authentication now works with passwords that contain the ':' character ([luismedel](https://github.com/luismedel)). 
- Added missing librdkafka internal and broker error codes to the `ErrorCode` enum.


# 1.9.2

## Enhancements

- References librdkafka.redist 1.9.2 which includes an Apple M1 librdkafka build.
- Added ACL AdminClient operations (CreateAcls, DescribeAcls, DeleteAcls) ([emasab](https://github.com/emasab)).
- Added DeleteGroups to AdminClient ([3schwartz](https://github.com/3schwartz)).
- Enhanced the Avro Specific Deserializer to ignore the type namespace ([sergemat](https://github.com/sergemat)).
- Improved efficiency of the statistics handler ([VladimirTyrin](https://github.com/VladimirTyrin)).


## Fixes

- The AdminClient poll loop no longer terminates when a request results in an error ([emasab](https://github.com/emasab)).
- Upgraded Newtonsoft.Json to 13.0.1 to address a security vulnerability in 9.0.1.


# 1.9.1

There was no 1.9.1 release of the .NET Client.


# 1.9.0

## Enhancements

- References librdkafka.redist 1.9.0. Refer to the [librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.9.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.
- References Apache.Avro 1.11.0. Refer to the [release notes](https://github.com/apache/avro/releases/tag/release-1.11.0) for further information ([JanReimerD](https://github.com/JanReimerD)).
- Added support for serializing and deserializing null in Avro serdes ([YairHalberstadt](https://github.com/YairHalberstadt)).
- Enhanced CachedSchemaRegistryClient to allow for user-implemented authentication schemes ([henrydaly](https://github.com/henrydaly)).
- Reduced memory use when producing with delivery reports disabled ([TrickyCat](https://github.com/TrickyCat)).

## Fixes

- Resolved incompatibility with Apple M1 processors ([dkaukov](https://github.com/dkaukov)).
- No longer crashes on Alpine Linux when MUSL is installed ([shurivich](https://github.com/shurivich)).
- JSON validation exception messages now properly include failing paths ([drinehimer](https://github.com/drinehimer)).
- Upgraded Google.Protobuf dependency to 3.15.0 (CVE-2021-22570).
- Resolved memory leak in AdminClient response handler ([emasab](https://github.com/emasab)).
- Resolved memory leak in Producer.SendOffsetsToTransaction.


## Upgrade Considerations

- The earliest supported .NET Framework version is now 4.6.2, previously this was 4.5 ([bjornbouetsmith](https://github.com/bjornbouetsmith)).


# 1.8.2

# Enhancements and Fixes

- References librdkafka.redist 1.8.2. Refer to the [librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.8.2)
for a complete list of changes, enhancements, fixes and upgrade considerations.
- Added the SslCaPem configuration property to specify a CA certificate using a PEM string.


# 1.8.1

## Enhancements 

- Updated `NJsonSchema` to v10.5.2.


# 1.8.0

- References librdkafka.redist 1.8.0. Refer to the [librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.8.0)
for a complete list of changes, enhancements, fixes and upgrade considerations.
- Added the `UseLatestVersion` configuration property to the Protobuf, JSON Schema and Avro serdes ([rayokota](https://github.com/rayokota)).

## Fixes

- **Breaking Change:** Updated the message framing format used by the Protobuf serdes (`ProtobufSerializer` and `ProtobufDeserializer`) to be
compatible with the Java Protobuf serdes (message indices now use zigzag encoding). Note: This framing encodes schema metadata, enabling
integration with Confluent Schema Registry. To disable, set the `UseDeprecatedFormat` configuration property to `true`.
([rayokota](https://github.com/rayokota)).

## Security

- Upgraded the bundled zlib version from 1.2.8 to 1.2.11 in the librdkafka.redist NuGet package. The updated zlib version fixes
CVEs: CVE-2016-9840, CVE-2016-9841, CVE-2016-9842, CVE-2016-9843 See https://github.com/edenhill/librdkafka/issues/2934 for more information.



# 1.7.0

## Enhancements

- References librdkafka.redist 1.7.0. Refer to the [librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.7.0) for a complete
list of changes, enhancements, fixes and upgrade considerations.
- Added OAuth support to AdminClient ([jerive](https://github.com/jerive))


## Fixes

- Resolved a schema caching bug ([#1587](https://github.com/confluentinc/confluent-kafka-dotnet/pull/1587)) in `CachedSchemaRegistryClient.GetSchemaIdAsync` ([jeremy001181](https://github.com/jeremy001181)).
- Fixed a configuration error in the Web example ([cjgalione](https://github.com/cjgalione)).

## Security

- Updated `System.Net.Http` dependency to v4.3.4 ([CVE-2018-8292](https://github.com/advisories/GHSA-7jgj-8wvc-jh57))


# 1.6.3

## Fixes

- References Apache.Avro v1.10.2, which resolves an issue with large string deserialization [AVRO-3005](https://issues.apache.org/jira/browse/AVRO-3005).


# 1.6.2

## Enhancements

- References librdkafka.redist 1.6.1. Refer to the [1.6.0](https://github.com/edenhill/librdkafka/releases/tag/v1.6.0) and [1.6.1](https://github.com/edenhill/librdkafka/releases/tag/v1.6.1) release notes for more information. Headline features:
  - KIP-429: Incremental rebalancing.
  - KIP-447: Producer scalability for exactly once semantics.
  - KIP-480: Sticky partitioner.
- KIP-22: Support for custom partitioners.
- Confluent.Kafka can now be used with Mono on Linux and MacOS. **Note**: Mono is not a supported runtime.
- The debian9-librdkafka.so build of librdkafka has been replaced with a more portable one: centos6-librdkafka.so (note: Debian 9 is still supported).
- Exceptions thrown by `Producer.Produce` now include an inner exception with additional context on the error ([joostas](https://github.com/joostas)).
- Added `ConfigureAwait(false)` to async methods in the Avro Serdes.
- Added `IsInvalid` property to `Handle` class ([volgunin](https://github.com/volgunin)).

## Fixes

- Fixed race condition in `ProtobufSerializer` ([yurii-hunter](https://github.com/yurii-hunter)).


# 1.6.0, 1.6.1

Version 1.6.0 and 1.6.1 were not released.


# 1.5.3

## Enhancements

- References librdkafka 1.5.3. Refer to the [release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.5.3) for more information.
- References Apache.Avro v1.10.1, which adds support for enum defaults [AVRO-2750](https://issues.apache.org/jira/browse/AVRO-2750).


# 1.5.2

## Enhancements

- References librdkafka 1.5.2. Refer to the [release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.5.2) for more information.
- Avro serializer now supports generic parameter `ISpecificRecord`. In this case, data is serialized according to the per-message concrete type (@[ni-mi](https://github.com/ni-mi)).


# 1.5.1

## Enhancements

- Added support for OAuth Authentication via SASL/OAUTHBEARER (KIP-255) ([thtp](https://github.com/thtp)).


# 1.5.0

## Enhancements

- References librdkafka 1.5.0 which brings many small improvements and bug fixes (and no new large features). Refer to the [release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.5.0) for more information.
- Added support for Schema Registry SSL Authentication ([@dinegri](https://github.com/dinegri)).


# 1.4.4

## Fixes

- Resolved a stack overflow issue in `Error(IntPtr error)` [#1249](https://github.com/confluentinc/confluent-kafka-dotnet/issues/1249) (@[midnightriot](https://github.com/midnightriot))
- References librdkafka 1.4.4. Refer to the [release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.4.4) for more information.


# 1.4.3

## Fixes

- Subject names are now URL encoded when used in Schema Registry URLs.
- Fixed a memory leak that occured when passing a `CancellationToken` to `Producer.ProduceAsync`.


# 1.4.2

## Fixes

- Maintenance release. Refer to the [librdkafka release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.4.2) for more information.
- Fixed incorrect content-type header in Schema Registry HTTP requests ([@jeremy001181](https://github.com/jeremy001181)).


# 1.4.0

## Enhancements

- References librdkafka v1.4.0. Refer to the [release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.4.0) for more information. Headline features:
  - KIP-98: Producer support for transactions ([@edenhill](https://github.com/edenhill)). This is the final piece in the puzzle required to enable exactly once stream processing (EOS) in .NET.
  - KIP-345: Static consumer group membership ([@rnpridgeon](https://github.com/rnpridgeon)).
  - KIP-511: Client name and version are now provided to brokers.
- Added Protobuf and JSON serdes including integration with Schema Registry.
- Switched to the official Apache Avro [nuget package](https://www.nuget.org/packages/Confluent.Apache.Avro/), which includes support for [logical types](https://avro.apache.org/docs/current/spec.html#Logical+Types), and all fixes from the [Confluent fork](https://github.com/confluentinc/avro/tree/confluent-fork), which has now been discontinued.
- Message headers are now exposed to serdes via `SerializationContext` ([@pascalconfluent](https://github.com/pascalconfluent)).
- Added a `CancellationToken` parameter to the `ProduceAsync` methods.
- Uncaught exceptions thrown in handler methods are now propagated to the initiating function, or in the case of error or log events, ignored. Previously, they would cause the application to terminate.
- Added a [WordCount](https://github.com/confluentinc/confluent-kafka-dotnet/tree/master/examples/Transactions) example demonstrating a streaming map-reduce application with exactly-once processing.

## Changes

- Some internal improvements to the `Consmer` (thanks to [@andypook](https://github.com/AndyPook)).
- BREAKING CHANGE: `net452` is no longer a target framework of `Confluent.SchemaRegistry` or `Confluent.SchemaRegistry.Serdes` due to the switch to the official Apache Avro package which only targets `netstandard2.0`. 
- Marked properties on `ConsumeResult` that simply delegate to the corresponding properties on `ConsumeResult.Message` as obsolete.

## Fixes

- Fixed an `ArgumentNullException` regression in `ListGroups` (thanks to [@andypook](https://github.com/AndyPook)).


# 1.3.0

## Enhancements

- Added support for [Subject Name Strategies](https://www.confluent.io/blog/put-several-event-types-kafka-topic/) to `Confluent.SchemaRegistry` (thanks to [@fipil](https://github.com/fipil), [@alexpedrero](https://github.com/alexpedrero) and [@eroyal](https://github.com/eroyal) for their input).
- `ConsumeResult` now throws `MessageNullException`, not `NullReferenceException` when a message property is accessed but no message exists (thanks to [@enzian](https://github.com/enzian) for this change).
- References librdkafka v1.3.0. Refer to the [release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.3.0) for more information. Headline feature is support for fetch from follower (KIP-392).

## Changes

- Deprecated properties of `SchemaRegistryConfig` with the (superfluous) prefix `SchemaRegistry`. Added corresponding properties without this prefix.

## Fixes

- Resolved issue [993](https://github.com/confluentinc/confluent-kafka-dotnet/issues/993) whereby `RestService` was unable to communicate with Schema Registry hosted on a non-root path. Thanks to [@jonathansant](https://github.com/jonathansant) for this fix.


# 1.2.2

- References librdkafka v1.2.2 which upgrades the lz4 dependency to v1.9.2.


# 1.2.1

## Fixes

- References librdkafka v1.2.1 which resolves an issue that broke GSSAPI authentication on Windows.


# 1.2.0

## Bugs

  **WARNING: There is an issue with SASL GSSAPI authentication on Windows with this release. This is resolved in v1.2.1.**
	
## Enhancements

- References librdkafka v1.2.0. Refer to the [release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.2.0) for more information. Headline feature is consumer side support for transactions.
- Added `IDictionary` overload to `Config` constructors (contribution by @AndyPook).
- `Confluent.Kafka`, `Confluent.SchemaRegistry` and `Confluent.SchemaRegistry.Serdes` are now all signed, and `Confluent.Kafka.StrongName` deprecated.

## Fixes

- Updated the librdkafka build load order so that the most featureful version is used on any given platform.


# 1.1.0

## Enhancements

- References librdkafka v1.1.0. Refer to the [release notes](https://github.com/edenhill/librdkafka/releases/tag/v1.1.0) for more informtion. Notable improvement for Windows SSL users: You no longer need to specify a CA certificate file/directory (SslCaLocation) - librdkafka will load the CA certs by default from the Windows Root Certificate Store.


# 1.0.1.1

## Changes

- Applied `ConfigureAwait(false)` to all internal `await`ed calls, which resolves deadlock issues in
synchronization contexts with limited numbers of threads [#967](https://github.com/confluentinc/confluent-kafka-dotnet/pull/967).


# 1.0.1

## Enhancements

- Support for Alpine Linux.
- New LogLevelType enum and functions to convert between different log level type levels.
- Added netstandard20 as a target.
- References librdkafka [1.0.1](https://github.com/edenhill/librdkafka/releases/tag/v1.0.1).


# 1.0.0

## Summary

1.0.0 is a major update of the API, introducing many new features and enhancements.
Note: The 1.0 API is not compatible with earlier versions of the library.

Feature highlights:

- Inherits all of the new features in librdkafka [v1.0.0](https://github.com/edenhill/librdkafka/releases/tag/v1.0.0)
- General improvements to client classes:
  - Strongly typed configuration.
  - Construction is via builder classes:
    - Allows/enforces that event handlers are specified at construction time.
    - More extensible.
  - Header support.
  - New Message class abstraction and related changes.
  - Consistency in error reporting across library (via exceptions).
  - Support for fatal errors.
- Added AdminClient:
  - CreateTopics, DeleteTopics, CreatePartitions, AlterConfigs, and DescribeConfigs methods.
  - Moved ListGroups and GetMetadata methods from the Producer and Consumer classes to AdminClient.
- Producer specific improvements:
  - New serialization interface:
    - Non-blocking support for async serializers.
    - Very flexible:
      - e.g. can be easily extended to support header serialization.
  - Capability to specify custom timestamps when producing messages. 
  - Message persistence status support.
  - Renamed ProduceAsync variants with a callback to Produce.
- Consumer improvements:
  - A new rebalance API.
  - New deserialization API analogous to the new serialization API.
  - PartitionEOF notification is via ConsumeResult, not events.
    - EOF notification is now disabled by default. To enable, set the EnablePartitionEof config property to true.
- Confluent Schema Registry integration
  - Added support for basic authentication.
  - Added GET subject versions to the cached schema registry client.
  - Renamed Confluent.Kafka.Avro to Confluent.SchemaRegistry.Serdes in preparation for support for additional serialization formats.


# 1.0.0-RC7

## Changes

- Moved SyncOverAsync functionality to the Confluent.Kafka.SyncOverAsync namespace.
- Marked DependentProducerBuilder as API-SUBJECT-TO-CHANGE.
- No-op handlers are no longer registered with librdkafka if corresponding handlers are not specified in client builder classes.
- Renamed AsyncAvroSerializer to AvroSerializer and AsyncAvroDeserializer to AvroDeserializer


# 1.0.0-RC6

## New Features

- Added DependentAdminClientBuilder class.


## Changes

- Reverted RC4 changes.
- Renamed AvroSerializer to AsyncAvroSerializer and AvroDeserializer to AsyncAvroDeserializer
- Added SyncOverAsyncSerializer and SyncOverAsyncDeserializer adapter classes.
- Added AsSyncOverAsync factory method to AsyncAvroSerializer and AsyncAvroDeserializer.
- Removed IAsyncDeserializer setter overloads from the ConsumerBuilder class.
- Renamed Producer.BeginProduce to Producer.Produce.
- Produce throws an exception if used when async serializers are configured.
- Made AdminClient, Producer, and Consumer classes internal.


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
  - The `IDeliveryHandler` interface has been deprecated.
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
