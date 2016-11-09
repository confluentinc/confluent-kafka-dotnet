rdkafka-dotnet - C# Apache Kafka client
=======================================

**rdkafka-dotnet** is a C# client for [Apache Kafka](http://kafka.apache.org/) based on [librdkafka](https://github.com/edenhill/librdkafka).

## Usage

Just reference the [RdKafka NuGet package](https://www.nuget.org/packages/RdKafka)

## Api Reference

[Read the Api Documentation here](api/RdKafka.html)

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
