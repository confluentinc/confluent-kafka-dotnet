Confluent's .NET Client for Apache Kafka<sup>TM</sup>
=====================================================

[![Travis Build Status](https://travis-ci.org/confluentinc/confluent-kafka-dotnet.svg?branch=master)](https://travis-ci.org/confluentinc/confluent-kafka-dotnet)
[![Build status](https://ci.appveyor.com/api/projects/status/kux83eykufuv16cn/branch/master?svg=true)](https://ci.appveyor.com/project/ConfluentClientEngineering/confluent-kafka-dotnet/branch/master)
[![Chat on Slack](https://img.shields.io/badge/chat-on%20slack-7A5979.svg)](https://confluentcommunity.slack.com/messages/clients)

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

Reference the [Confluent.Kafka NuGet package](https://www.nuget.org/packages/Confluent.Kafka/) (version 0.11.0).

To install confluent-kafka-dotnet from within Visual Studio, search for Confluent.Kafka in the NuGet Package Manager 
UI, or run the following command in the Package Manager Console:

```
Install-Package Confluent.Kafka -Version 0.11.0
```

To reference in a dotnet core project, explicitly add a package reference to your .csproj file:

```
<ItemGroup>
  ...
  <PackageReference Include="Confluent.Kafka" Version="0.11.0" />
  ...
</ItemGroup>
```

Pre-release nuget packages are available from the following nuget package source:
[https://ci.appveyor.com/nuget/confluent-kafka-dotnet](https://ci.appveyor.com/nuget/confluent-kafka-dotnet). 

The version suffix of these nuget packages matches the appveyor build number. You can see which commit a 
particular build number corresponds to by looking at the 
[AppVeyor build history](https://ci.appveyor.com/project/ConfluentClientEngineering/confluent-kafka-dotnet/history)


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
