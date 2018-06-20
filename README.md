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

## Referencing

confluent-kafka-dotnet is distributed via NuGet. We provide three packages:

- [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka/) *[net45, netstandard1.3]* - The core client library.
- [Confluent.Kafka.Avro](https://www.nuget.org/packages/Confluent.Kafka.Avro/) *[net452, netstandard2.0]* - Provides a serializer and deserializer for working with Avro serialized data with Confluent Schema Registry integration.
- [Confluent.SchemaRegistry](https://www.nuget.org/packages/Confluent.SchemaRegistry/) *[net452, netstandard1.4]* - Confluent Schema Registry client (a dependency of Confluent.Kafka.Avro).

To install Confluent.Kafka from within Visual Studio, search for Confluent.Kafka in the NuGet Package Manager UI, or run the following command in the Package Manager Console:

```
Install-Package Confluent.Kafka -Version 0.11.4
```

To add a reference to a dotnet core project, execute the following at the command line:

```
dotnet add package -v 0.11.4 Confluent.Kafka
```

### Development Branch

We have started working towards a 1.0 release of the library which will occur after we add idempotence and transaction features. In order to best accomodate these and other changes,
we will be making breaking changes to the API in that release. You can track our progress on the [1.0-experimental](https://github.com/confluentinc/confluent-kafka-dotnet/tree/1.0-experimental) 
branch (as well as corresponding packages on [nuget.org](https://www.nuget.org/packages/Confluent.Kafka/)). We have already added an **AdminClient** as well as support for **message headers** 
and **custom timestamps** amongst other things. Note that all work on this branch is subject to change and should not be considered production ready. All feedback is very welcome!

Also, nuget packages corresponding to all release branch commits are available from the following nuget package source (Note: this is not a web url - you should specify it in the nuget package manger):
[https://ci.appveyor.com/nuget/confluent-kafka-dotnet](https://ci.appveyor.com/nuget/confluent-kafka-dotnet). The version suffix of these nuget packages matches the appveyor build number. You can see which commit a particular build number corresponds to by looking at the 
[AppVeyor build history](https://ci.appveyor.com/project/ConfluentClientEngineering/confluent-kafka-dotnet/history)


## Usage

Take a look in the [examples](examples) directory for example usage. The [integration tests](test/Confluent.Kafka.IntegrationTests/Tests) also serve as good examples.

For an overview of configuration properties, refer to the [librdkafka documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). 

API documentation is available on the [Confluent website](https://docs.confluent.io/current/clients/confluent-kafka-dotnet/api/Confluent.Kafka.html). Note that there is currently an issue with the build process that is preventing some of this documentation from being generated. For missing information, please refer instead to the XML doc comments in the source code.

### Basic Producer Example

```csharp
using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

public class Program
{
  public static void Main()
  {
    var config = new Dictionary<string, object> 
    { 
        { "bootstrap.servers", "localhost:9092" } 
    };

    using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
    {
      var dr = producer.ProduceAsync("my-topic", null, "test message text").Result;
      Console.WriteLine($"Delivered '{dr.Value}' to: {dr.TopicPartitionOffset}");
    }
  }
}
```

### Basic Consumer Example

```csharp
using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

public class Program
{
  public static void Main()
  {
    var conf = new Dictionary<string, object> 
    { 
      { "group.id", "test-consumer-group" },
      { "bootstrap.servers", "localhost:9092" },
      { "auto.commit.interval.ms", 5000 },
      { "auto.offset.reset", "earliest" }
    };

    using (var consumer = new Consumer<Null, string>(conf, null, new StringDeserializer(Encoding.UTF8)))
    {
      consumer.OnMessage += (_, msg)
        => Console.WriteLine($"Read '{msg.Value}' from: {msg.TopicPartitionOffset}");

      consumer.OnError += (_, error)
        => Console.WriteLine($"Error: {error}");

      consumer.OnConsumeError += (_, msg)
        => Console.WriteLine($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

      consumer.Subscribe("my-topic");

      while (true)
      {
        consumer.Poll(TimeSpan.FromMilliseconds(100));
      }
    }
  }
}
```

### AvroGen tool

The Avro serializer and deserializer provided by `Confluent.Kafka.Avro` can be used with the `GenericRecord` class
or with specific classes generated using the `avrogen` tool, available via Nuget (.NET Core 2.1 required):

```
dotnet tool install -g Confluent.Apache.Avro.AvroGen
```

Usage:

```
avrogen -s your_schema.asvc .
```

### Confluent Cloud

The [Confluent Cloud example](examples/ConfluentCloud) demonstrates how to configure the .NET client for use with [Confluent Cloud](https://www.confluent.io/confluent-cloud/).


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
