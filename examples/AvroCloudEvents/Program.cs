using Avro;
using Avro.Generic;
using AvroCloudEvents;
using Confluent.Kafka;
using Confluent.SchemaRegistry;


if (args.Length != 6)
{
    Console.WriteLine("Usage: .. bootstrapServers saslUsername saslPassword schemaRegistryUrl schemaRegistryUsername schemaRegistryPassword");
    return;
}

var producerConfig = new ProducerConfig
{
    BootstrapServers = args[0],
    SaslMechanism = SaslMechanism.Plain,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslUsername = args[1],
    SaslPassword = args[2],
};
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = args[0],
    SaslMechanism = SaslMechanism.Plain,
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslUsername = args[1],
    SaslPassword = args[2],
    GroupId = $"avro-{Guid.NewGuid()}",
};
var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url = args[3],
    BasicAuthUserInfo = $"{args[4]}:{args[5]}",
};

MyProducer.SchemaRegistryConfig = schemaRegistryConfig;
MyProducer.ProducerConfig = producerConfig;
MyConsumer.SchemaRegistryConfig = schemaRegistryConfig;
MyConsumer.ConsumerConfig = consumerConfig;

var s = (RecordSchema)RecordSchema.Parse(
    @"{
        ""type"": ""record"",
        ""name"": ""User"",
        ""fields"": [
            {""name"": ""name"", ""type"": ""string""},
            {""name"": ""favorite_number"",  ""type"": ""long""},
            {""name"": ""favorite_color"", ""type"": ""string""}
        ]
        }"
);
string topic = "cloudevents.users";
CancellationTokenSource cts = new CancellationTokenSource();
MyConsumer.StartConsumerCloudEvent(topic, cts);
Console.WriteLine($"producing on {topic}. Enter user names, q to exit.");
long i = 1;
string text;
while ((text = Console.ReadLine()) != "q")
{
    var record = new GenericRecord(s);
    record.Add("name", text);
    record.Add("favorite_number", i++);
    record.Add("favorite_color", "blue");
    await MyProducer.ProduceCloudEvent(record, "user-events", topic);
    continue;
}

cts.Cancel();
