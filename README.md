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

confluent-kafka-dotnet is distributed via NuGet. We provide five packages:

- [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka/) *[net45, netstandard1.3, netstandard2.0]* - The core client library.
- [Confluent.SchemaRegistry.Serdes.Avro](https://www.nuget.org/packages/Confluent.SchemaRegistry.Serdes.Avro/) *[netstandard2.0]* - Provides a serializer and deserializer for working with Avro serialized data with Confluent Schema Registry integration.
- [Confluent.SchemaRegistry.Serdes.Protobuf](https://www.nuget.org/packages/Confluent.SchemaRegistry.Serdes.Protobuf/) *[netstandard2.0]* - Provides a serializer and deserializer for working with Protobuf serialized data with Confluent Schema Registry integration.
- [Confluent.SchemaRegistry.Serdes.Json](https://www.nuget.org/packages/Confluent.SchemaRegistry.Serdes.Json/) *[netstandard2.0]* - Provides a serializer and deserializer for working with Json serialized data with Confluent Schema Registry integration.
- [Confluent.SchemaRegistry](https://www.nuget.org/packages/Confluent.SchemaRegistry/) *[netstandard1.4, netstandard2.0]* - Confluent Schema Registry client (a dependency of the Confluent.SchemaRegistry.Serdes packages).

To install Confluent.Kafka from within Visual Studio, search for Confluent.Kafka in the NuGet Package Manager UI, or run the following command in the Package Manager Console:

```
Install-Package Confluent.Kafka -Version 1.5.2
```

To add a reference to a dotnet core project, execute the following at the command line:

```
dotnet add package -v 1.5.2 Confluent.Kafka
```

Note: `Confluent.Kafka` depends on the `librdkafka.redist` package which provides a number of different builds of `librdkafka` that are compatible with [common platforms](https://github.com/edenhill/librdkafka/wiki/librdkafka.redist-NuGet-package-runtime-libraries). If you are on one of these platforms this will all work seamlessly (and you don't need to explicitly reference `librdkafka.redist`). If you are on a different platform, you may need to [build librdkafka](https://github.com/edenhill/librdkafka#building) manually (or acquire it via other means) and load it using the [Library.Load](https://docs.confluent.io/current/clients/confluent-kafka-dotnet/api/Confluent.Kafka.Library.html#Confluent_Kafka_Library_Load_System_String_) method.

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

        // If serializers are not specified, default serializers from
        // `Confluent.Kafka.Serializers` will be automatically used where
        // available. Note: by default strings are encoded as UTF8.
        using (var p = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                var dr = await p.ProduceAsync("test-topic", new Message<Null, string> { Value="test" });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
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
use the `Produce` method instead:

```csharp
using System;
using Confluent.Kafka;

class Program
{
    public static void Main(string[] args)
    {
        var conf = new ProducerConfig { BootstrapServers = "localhost:9092" };

        Action<DeliveryReport<Null, string>> handler = r => 
            Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery Error: {r.Error.Reason}");

        using (var p = new ProducerBuilder<Null, string>(conf).Build())
        {
            for (int i=0; i<100; ++i)
            {
                p.Produce("my-topic", new Message<Null, string> { Value = i.ToString() }, handler);
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
using System.Threading;
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
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
        {
            c.Subscribe("my-topic");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }
        }
    }
}
```

### Schema Registry Integration

The three "Serdes" packages provide serializers and deserializers for Avro, Protobuf and JSON with [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html) integration. The `Confluent.SchemaRegistry` nuget package provides a client for interfacing with
Schema Registry's REST API.

**Note:** All three serialization formats are supported across Confluent Platform. They each make different tradeoffs, and you should use the one that best matches to your requirements. Avro is well suited to the streaming data use-case, but the maturity of the non-Java implementations lags that of Java - this is an important consideration. Protobuf and JSON both have great support in .NET.

#### Avro

You can use the Avro serializer and deserializer with the `GenericRecord` class or with specific classes generated
using the `avrogen` tool, available via Nuget (.NET Core 2.1 required):

```
dotnet tool install --global Apache.Avro.Tools
```

Usage:

```
avrogen -s your_schema.avsc .
```

For more information about working with Avro in .NET, refer to the the blog post [Decoupling Systems with Apache Kafka, Schema Registry and Avro](https://www.confluent.io/blog/decoupling-systems-with-apache-kafka-schema-registry-and-avro/)


### Error Handling

Errors delivered to a client's error handler should be considered informational except when the `IsFatal` flag
is set to `true`, indicating that the client is in an un-recoverable state. Currently, this can only happen on
the producer, and only when `enable.idempotence` has been set to `true`. In all other scenarios, clients will
attempt to recover from all errors automatically.

Although calling most methods on the clients will result in a fatal error if the client is in an un-recoverable
state, you should generally only need to explicitly check for fatal errors in your error handler, and handle
this scenario there.

#### Producer

When using `Produce`, to determine whether a particular message has been successfully delivered to a cluster,
check the `Error` field of the `DeliveryReport` during the delivery handler callback.

When using `ProduceAsync`, any delivery result other than `NoError` will cause the returned `Task` to be in the
faulted state, with the `Task.Exception` field set to a `ProduceException` containing information about the message
and error via the `DeliveryResult` and `Error` fields. Note: if you `await` the call, this means a `ProduceException`
will be thrown.

#### Consumer

All `Consume` errors will result in a `ConsumeException` with further information about the error and context
available via the `Error` and `ConsumeResult` fields.


### Confluent Cloud

The [Confluent Cloud example](examples/ConfluentCloud) demonstrates how to configure the .NET client for use with
[Confluent Cloud](https://www.confluent.io/confluent-cloud/).

### Developer Notes

Instructions on building and testing confluent-kafka-dotnet can be found [here](DEVELOPER.md).

Copyright (c) 
2016-2019 [Confluent Inc.](https://www.confluent.io)
2015-2016 [Andreas Heider](mailto:andreas@heider.io)
