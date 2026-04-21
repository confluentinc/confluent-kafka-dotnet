# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Big picture

`Confluent.Kafka` is a thin managed wrapper around **librdkafka** (C client). The interop layer — [src/Confluent.Kafka/Impl/LibRdKafka.cs](src/Confluent.Kafka/Impl/LibRdKafka.cs) and the `Safe*Handle` classes next to it — is the boundary; everything above it is idiomatic .NET (builders, `IProducer<K,V>`, `IConsumer<K,V>`, `IAdminClient`). The native library is supplied by the `librdkafka.redist` NuGet package pinned in [src/Confluent.Kafka/Confluent.Kafka.csproj](src/Confluent.Kafka/Confluent.Kafka.csproj). **This `librdkafka.redist` version and the `VersionPrefix` in [src/Directory.Build.props](src/Directory.Build.props) must stay in sync** — they are the product version, and bumping one without the other will ship a mismatched client.

The solution is a monorepo of independently-packed NuGets (see [README.md](README.md) for the full list):

- **Confluent.Kafka** — the only package that targets `netstandard2.0;net462;net6.0;net8.0`. Core producer/consumer/admin API.
- **Confluent.SchemaRegistry** — REST client for Confluent Schema Registry (`CachedSchemaRegistryClient`).
- **Confluent.SchemaRegistry.Serdes.{Avro,Protobuf,Json}** — serializer/deserializer implementations that plug into `ProducerBuilder.SetKeySerializer` / `ConsumerBuilder.SetValueDeserializer`. They depend on `Confluent.SchemaRegistry` but not on each other.
- **Confluent.SchemaRegistry.Encryption** + **.Aws / .Azure / .Gcp / .HcVault** — client-side field-level encryption (CSFLE). Each KMS lives in its own package so users only take the cloud SDK they need.
- **Confluent.SchemaRegistry.Rules** — CEL data-quality + JSONata migration rules.

All non-`Confluent.Kafka` packages target `net6.0;net8.0` only (set in [src/Directory.Build.props](src/Directory.Build.props)); tests target the same. `LangVersion` is pinned to 12 in the root [Directory.Build.props](Directory.Build.props).

### Generated config

[src/Confluent.Kafka/Config_gen.cs](src/Confluent.Kafka/Config_gen.cs) (`ProducerConfig`, `ConsumerConfig`, `AdminClientConfig`, enums, etc.) is **auto-generated** by the [src/ConfigGen/](src/ConfigGen/) tool, which scrapes the matching `librdkafka` `CONFIGURATION.md`. Don't hand-edit `Config_gen.cs` — regenerate via ConfigGen and commit the output when bumping the librdkafka version. `Config.cs` contains the hand-written additions.

### Test infrastructure

- [test/Confluent.Kafka.TestsCommon/](test/Confluent.Kafka.TestsCommon/) provides `TestConsumerBuilder` / `TestProducerBuilder` / `TestConsumerGroupProtocol`. Integration tests should build clients through these wrappers — they honor the `TEST_CONSUMER_GROUP_PROTOCOL` env var (`classic` or `consumer`) and strip config properties that are incompatible with the KIP-848 `consumer` protocol. See [doc/kip-848-migration-guide.md](doc/kip-848-migration-guide.md).
- Integration tests read a sibling `testconf.json` (copied to output) for `bootstrapServers`, SASL settings, and OAuth bootstrap. Fixture setup is in [test/Confluent.Kafka.IntegrationTests/Tests/Tests.cs](test/Confluent.Kafka.IntegrationTests/Tests/Tests.cs) (`GlobalFixture`), which also creates/deletes shared topics per test run.
- The docker stack in [test/docker/](test/docker/) brings up Kafka on `9092` (PLAINTEXT) and `9093` (SASL PLAINTEXT), plus two Schema Registry instances: `8081` (no auth) and `8082` (basic auth).

## Common commands

Build / restore (from repo root):

```bash
dotnet restore
dotnet build Confluent.Kafka.sln -c Release   # matches CI
make build                                     # builds every examples/ and test/ csproj individually
```

Unit tests (no Kafka required) — single project:

```bash
dotnet test test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj
dotnet test test/Confluent.SchemaRegistry.UnitTests/Confluent.SchemaRegistry.UnitTests.csproj
dotnet test test/Confluent.SchemaRegistry.Serdes.UnitTests/Confluent.SchemaRegistry.Serdes.UnitTests.csproj
```

Run all unit test suites (matches CI):

```bash
make test             # runs every test/*UnitTests project on the default framework set
make test-latest      # pins to net8.0 (DEFAULT_TEST_FRAMEWORK)
```

Run a single test / class (xUnit):

```bash
dotnet test test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj \
  --filter "FullyQualifiedName~Confluent.Kafka.UnitTests.HeadersTests"
dotnet test ... --filter "DisplayName=ThrowOnCommitForUnassignedPartition"
```

Integration tests (require the docker stack):

```bash
cd test/docker && docker-compose up -d        # or docker-compose-kraft.yaml for KRaft
dotnet test test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj
# KIP-848 consumer protocol:
TEST_CONSUMER_GROUP_PROTOCOL=consumer dotnet test test/Confluent.Kafka.IntegrationTests/...
```

Pack a single NuGet locally:

```bash
dotnet pack src/Confluent.Kafka/Confluent.Kafka.csproj -c Release
```

Regenerate `Config_gen.cs` after bumping librdkafka:

```bash
dotnet run --project src/ConfigGen
```

## Conventions worth knowing

- New public API on `IProducer`/`IConsumer`/`IAdminClient` must flow through the corresponding `*Builder` and typically needs a P/Invoke addition in `Impl/LibRdKafka.cs` plus a safe wrapper in `Impl/SafeKafkaHandle.cs`.
- Shared utility code that needs to compile into both `Confluent.Kafka` and the SchemaRegistry packages lives in [src/Shared/](src/Shared/) and is `<Compile Include=...>`-linked by each csproj rather than referenced as a project.
- `netstandard2.0` / `net462` targets of `Confluent.Kafka` pull in `System.Memory` and `System.Buffers` explicitly (see csproj) — keep that conditional `ItemGroup` when editing the csproj.
- CI (AppVeyor for releases, Semaphore for PR builds — see [appveyor.yml](appveyor.yml), [.semaphore/semaphore.yml](.semaphore/semaphore.yml)) only runs unit tests, not integration tests. Integration test failures won't show up in PR checks; run them locally when touching client behavior.
