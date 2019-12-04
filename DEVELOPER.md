# Developer Notes

This document provides information useful to developers working on confluent-kafka-dotnet.


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

From within the [Confluent Platform](https://www.confluent.io/product/compare/) (or Apache Kafka) distribution directory,
run the following two commands (in separate terminal windows) to set up a single broker test Kafka cluster:

```
./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties

./bin/kafka-server-start ./etc/kafka/server.properties
```

Now use the `bootstrap-topics.sh` script in the test/Confleunt.Kafka.IntegrationTests directory to set up the
prerequisite topics:

```
./bootstrap-topics.sh <confluent platform path> <zookeeper>
```

Now bring up two schema registry instances (one with basic auth enabled, one without) using docker compose from inside the tests/Confluent.SchemaRegistry.IntegrationTests directory:

```
docker-compose up
```

There are integration test suites corresponding to each nuget package. These are [Confluent.Kafka.IntegrationTests](test/Confluent.Kafka.IntegrationTests), 
[Confluent.SchemaRegistry.IntegrationTests](test/Confluent.SchemaRegistry.IntegrationTests) and
[Confluent.SchemaRegistry.Serdes.IntegrationTests](test/Confluent.SchemaRegistry.Serdes.IntegrationTests).

