# Developer Notes

This document provides information useful to developers working on confluent-kafka-dotnet.

## Creating Pull Request

1. Make sure to format your code. We use [format](https://github.com/dotnet/format) to format the code through [pre-commit](https://pre-commit.com/) hook.
    * Install pre-commit following the steps at https://pre-commit.com/.
    * Install dotnet-format `dotnet tool install -g dotnet-format`.
    * Run `pre-commit install` to set up the git hook, which will run `format` on changed files in any commits moving forward.
2. Also verify no regression in integration tests. Add new tests, if needed.

## Building

Nuget packages are built automatically by appveyor corresponding to every commit to a PR or master branch as well as release tags. For further details, inspect the [appveyor.yml](appveyor.yml) file.


## Tests

### Unit Tests

There are unit test suites corresponding to each nuget package. These are [Confluent.Kafka.UnitTests](test/Confluent.Kafka.UnitTests),
[Confluent.SchemaRegistry.UnitTests](test/Confluent.SchemaRegistry.UnitTests) and
[Confluent.SchemaRegistry.Serdes.UnitTests](test/Confluent.SchemaRegistry.Serdes.UnitTests). To execute, enter the
relevant directory and run:

```
dotnet test
```

### Integration Tests

From the test/docker directory bring up the Kafka cluster with two schema registry instances (one with basic auth enabled, one without).

```
docker-compose up
```

There are integration test suites corresponding to each nuget package. These are [Confluent.Kafka.IntegrationTests](test/Confluent.Kafka.IntegrationTests),
[Confluent.SchemaRegistry.IntegrationTests](test/Confluent.SchemaRegistry.IntegrationTests) and
[Confluent.SchemaRegistry.Serdes.IntegrationTests](test/Confluent.SchemaRegistry.Serdes.IntegrationTests).

To execute, enter the relevant directory and run:

```
dotnet test
```
