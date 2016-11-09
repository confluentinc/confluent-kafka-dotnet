rdkafka-dotnet - C# Apache Kafka client
=======================================

[![Travis Build Status](https://travis-ci.org/ah-/rdkafka-dotnet.svg?branch=master)](https://travis-ci.org/ah-/rdkafka-dotnet)
[![Appveyor Build Status](https://ci.appveyor.com/api/projects/status/github/ah-/rdkafka-dotnet?branch=master&svg=true)](https://ci.appveyor.com/project/ah-/rdkafka-dotnet)
[![Gitter chat](https://badges.gitter.im/edenhill/librdkafka.png)](https://gitter.im/edenhill/librdkafka)

Copyright (c) 2015-2016, [Andreas Heider](mailto:andreas@heider.io)

**rdkafka-dotnet** is a C# client for [Apache Kafka](http://kafka.apache.org/) based on [librdkafka](https://github.com/edenhill/librdkafka).

**rdkafka-dotnet** is licensed under the 2-clause BSD license.

## Usage

Just reference the [RdKafka NuGet package](https://www.nuget.org/packages/RdKafka)

## Examples

### Producing messages

```cs
using (Producer producer = new Producer("127.0.0.1:9092"))
using (Topic topic = producer.Topic("testtopic"))
{
    byte[] data = Encoding.UTF8.GetBytes("Hello RdKafka");
    DeliveryReport deliveryReport = await topic.Produce(data);
    Console.WriteLine($"Produced to Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
}

```

### Consuming messages

```cs
var config = new Config() { GroupId = "example-csharp-consumer" };
using (var consumer = new EventConsumer(config, "127.0.0.1:9092"))
{
    consumer.OnMessage += (obj, msg) =>
    {
        string text = Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length);
        Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {text}");
    };

    consumer.Subscribe(new []{"testtopic"});
    consumer.Start();

    Console.WriteLine("Started consumer, press enter to stop consuming");
    Console.ReadLine();
}
```

### More

See `examples/`

## Documentation

[Read the API Documentation here](https://ah-.github.io/rdkafka-dotnet/api/RdKafka.html)

[Read the FAQ for answers to common questions](https://github.com/ah-/rdkafka-dotnet/wiki/Faq)

## Supported Platforms and .NET Releases

Requires .NET 4.5 or later. Tested with .NET Core on Linux, OS X and Windows, and classic .NET 4.5 on Windows.
