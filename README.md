Confluent's .NET Client for Apache Kafka<sup>TM</sup>
=====================================================

**confluent-kafka-dotnet** is Confluent's .NET client for [Apache Kafka](http://kafka.apache.org/) and the
[Confluent Platform](https://www.confluent.io/product/).

Features:

- **High performance** - confluent-kafka-dotnet is a lightweight wrapper around
[librdkafka](https://github.com/edenhill/librdkafka), a finely tuned C
client.

- **Reliability** - There are a lot of details to get right when writing an Apache Kafka
client. We get them right in one place (librdkafka) and leverage this work
across all of our clients (also [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python)
and [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)).

- **Supported** - Commercial support is offered by
[Confluent](https://confluent.io/).

- **Future proof** - Confluent, founded by the
creators of Kafka, is building a [streaming platform](https://www.confluent.io/product/)
with Apache Kafka at its core. It's high priority for us that client features keep
pace with core Apache Kafka and components of the [Confluent Platform](https://www.confluent.io/product/).

confluent-kafka-dotnet is derived from Andreas Heider's [rdkafka-dotnet](https://github.com/ah-/rdkafka-dotnet).
We're fans of his work and were very happy to have been able to leverage rdkafka-dotnet as the basis of this
client. Thanks Andreas!

## Usage

Reference the [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka/) and 
[librdkafka.redist](https://www.nuget.org/packages/librdkafka.redist/) NuGet packages (version 0.11.1).

**Important Note:** The librdkafka.redist package dependency is no longer implicit. You must now reference the 
librdkafka.redist package explicitly in addition to the Confluent.Kafka package. In the future we plan to provide
a variety of different librdkafka nuget packages suitable for use on specific platforms. We will also continue to 
distribute the general purpose librdkafka.redist package and this will continue to work seamlessly on the most 
common platforms.

To install confluent-kafka-dotnet from within Visual Studio, search for Confluent.Kafka and librdkafka.redist in the 
NuGet Package Manager UI, or run the following command in the Package Manager Console:

```
Install-Package librdkafka.redist -Version 0.11.1
Install-Package Confluent.Kafka -Version 0.11.1
```

To reference in a dotnet core project, explicitly add a package reference to your .csproj file:

```
<ItemGroup>
  ...
  <PackageReference Include="librdkafka.redist" Version="0.11.1" />
  <PackageReference Include="Confluent.Kafka" Version="0.11.1" />
  ...
</ItemGroup>
```

## Examples

Take a look in the [examples](examples) directory. The [integration tests](test/Confluent.Kafka.IntegrationTests/Tests) also serve as good examples.


## Build

To build the library or any test or example project, run the following from within the relevant project directory:

```
dotnet restore
dotnet build
```

To run an example project, run the following from within the example's project directory:

```
dotnet run <args>
```

## Tests

### Unit Tests

From within the test/Confluent.Kafka.UnitTests directory, run:

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

then:

```
dotnet test
```

Copyright (c) 2016-2017 [Confluent Inc.](https://www.confluent.io), 2015-2016, [Andreas Heider](mailto:andreas@heider.io)
