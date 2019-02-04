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
- [Confluent.SchemaRegistry.Serdes](https://www.nuget.org/packages/Confluent.SchemaRegistry.Serdes/) *[net452, netstandard2.0]* - Provides a serializer and deserializer for working with Avro serialized data with Confluent Schema Registry integration.
- [Confluent.SchemaRegistry](https://www.nuget.org/packages/Confluent.SchemaRegistry/) *[net452, netstandard1.4]* - Confluent Schema Registry client (a dependency of Confluent.SchemaRegistry.Serdes).

To install Confluent.Kafka from within Visual Studio, search for Confluent.Kafka in the NuGet Package Manager UI, or run the following command in the Package Manager Console:

```
Install-Package Confluent.Kafka -Version 1.0-beta2
```

To add a reference to a dotnet core project, execute the following at the command line:

```
dotnet add package -v 1.0-beta2 Confluent.Kafka
```

**Note:** We recommend using the `1.0-beta2` version of Confluent.Kafka for new projects in preference to the most recent stable release (0.11.5).
The 1.0 API provides more features, is considerably improved and is more performant than 0.11.x releases. In choosing the label 'beta',
we are signaling that we do not anticipate making any high impact changes to the API before the 1.0 release, however be warned that some 
breaking changes are still planned. You can track progress and provide feedback on the new 1.0 API
[here](https://github.com/confluentinc/confluent-kafka-dotnet/issues/614).

### Branch builds

Nuget packages corresponding to all commits to release branches are available from the following nuget package source (Note: this is not a web URL - you 
should specify it in the nuget package manger):
[https://ci.appveyor.com/nuget/confluent-kafka-dotnet](https://ci.appveyor.com/nuget/confluent-kafka-dotnet). The version suffix of these nuget packages 
matches the appveyor build number. You can see which commit a particular build number corresponds to by looking at the 
[AppVeyor build history](https://ci.appveyor.com/project/ConfluentClientEngineering/confluent-kafka-dotnet/history)


## Usage

Take a look in the [examples](examples) directory for example usage. The [integration tests](test/Confluent.Kafka.IntegrationTests/Tests) also serve as good examples.

For an overview of configuration properties, refer to the [librdkafka documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). 

### Basic Producer Examples

You should use the `ProduceAsync` method if you would like to wait for the result of your produce
requests before proceeding. You might typically want to do this in highly concurrent scenarios,
for example in the context of handling web requests. Behind the scenes, the client will manage 
optimizing communication with the Kafka brokers for you, batching requests as appropriate.

```csharp
using System;
using System.Threading.Tasks;
using Confluent.Kafka;

class Program
{
    public static async Task Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        // If serializers are not specified as constructor arguments, default
        // serializers from `Confluent.Kafka.Serializers` will be automatically
        // used where available. Note: by default strings are encoded as UTF8.
        using (var p = new Producer<Null, string>(config))
        {
            try
            {
                var dr = await p.ProduceAsync("test-topic", new Message<Null, string> { Value="test" });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (KafkaException e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}
```

Note that a server round-trip is slow (3ms at a minimum; actual latency depends on many factors).
In highly concurrent scenarios you will achieve high overall throughput out of the producer using 
the above approach, but there will be a delay on each `await` call. In stream processing 
applications, where you would like to process many messages in rapid succession, you would typically
make use the `BeginProduce` method instead:

```csharp
using System;
using Confluent.Kafka;

class Program
{
    public static void Main(string[] args)
    {
        var conf = new ProducerConfig { BootstrapServers = "localhost:9092" };

        Action<DeliveryReportResult<Null, string>> handler = r => 
            Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery Error: {r.Error.Reason}");

        using (var p = new Producer<Null, string>(conf))
        {
            for (int i=0; i<100; ++i)
            {
                p.BeginProduce("my-topic", new Message<Null, string> { Value = i.ToString() }, handler);
            }

            // wait for up to 10 seconds for any inflight messages to be delivered.
            p.Flush(TimeSpan.FromSeconds(10));
        }
    }
}
```

### Basic Consumer Example

```csharp
using System;
using Confluent.Kafka;

class Program
{
    public static void Main(string[] args)
    {
        var conf = new ConsumerConfig
        { 
            GroupId = "test-consumer-group",
            BootstrapServers = "localhost:9092",
            // Note: The AutoOffsetReset property determines the start offset in the event
            // there are not yet any committed offsets for the consumer group for the
            // topic/partitions of interest. By default, offsets are committed
            // automatically, so in this example, consumption will only start from the
            // earliest message in the topic 'my-topic' the first time you run the program.
            AutoOffsetReset = AutoOffsetResetType.Earliest
        };

        using (var c = new Consumer<Ignore, string>(conf))
        {
            c.Subscribe("my-topic");

            bool consuming = true;
            // The client will automatically recover from non-fatal errors. You typically
            // don't need to take any action unless an error is marked as fatal.
            c.OnError += (_, e) => consuming = !e.IsFatal;

            while (consuming)
            {
                try
                {
                    var cr = c.Consume();
                    Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
            
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            c.Close();
        }
    }
}
```

### Working with Apache Avro

The `Confluent.SchemaRegistry.Serdes` nuget package provides an Avro serializer and deserializer that integrate with [Confluent
Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html). The `Confluent.SchemaRegistry` 
nuget package provides a client for interfacing with Schema Registry's REST API.

You can use the Avro serializer and deserializer with the `GenericRecord` class or with specific classes generated
using the `avrogen` tool, available via Nuget (.NET Core 2.1 required):

```
dotnet tool install -g Confluent.Apache.Avro.AvroGen
```

Usage:

```
avrogen -s your_schema.asvc .
```

For more information about working with Avro in .NET, refer to the the blog post [Decoupling Systems with Apache Kafka, Schema Registry and Avro](https://www.confluent.io/blog/decoupling-systems-with-apache-kafka-schema-registry-and-avro/)


### Error Handling

Errors raised via a client's `OnError` event should be considered informational except when the `IsFatal` flag
is set to `true`, indicating that the client is in an un-recoverable state. Currently, this can only happen on
the producer, and only when `enable.itempotence` has been set to `true`. In all other scenarios, clients are
able to recover from all errors automatically.

Although calling most methods on the clients will result in a fatal error if the client is in an un-recoverable
state, you should generally only need to explicitly check for fatal errors in your `OnError` handler, and handle
this scenario there.

#### Producer

When using `BeginProduce`, to determine whether a particular message has been successfully delivered to a cluster,
check the `Error` field of the `DeliveryReport` during the delivery handler callback.

When using `ProduceAsync`, any delivery result other than `NoError` will cause the returned `Task` to be in the
faulted state, with the `Task.Exception` field set to a `ProduceException` containing information about the message
and error via the `DeliveryResult` and `Error` fields. Note: if you `await` the call, this means a `ProduceException`
will be thrown.

#### Consumer

If you are using the deserializing version of the `Consumer`, any error encountered during deserialization (which
happens during your call to `Consume`) will throw a `DeserializationException`. All other `Consume` errors will
result in a `ConsumeException` with further information about the error and context available via the `Error` and
`ConsumeResult` fields.


### Confluent Cloud

The [Confluent Cloud example](examples/ConfluentCloud) demonstrates how to configure the .NET client for use with
[Confluent Cloud](https://www.confluent.io/confluent-cloud/).


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
