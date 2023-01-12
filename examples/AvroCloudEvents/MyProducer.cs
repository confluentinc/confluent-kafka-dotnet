using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.NewtonsoftJson;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace AvroCloudEvents
{
    public static class MyProducer
    {
        static MyProducer()
        {
            SchemaRegistryConfig = new SchemaRegistryConfig();
            ProducerConfig = new ProducerConfig();
        }
        
        public static SchemaRegistryConfig SchemaRegistryConfig { get; set; }

        public static ProducerConfig ProducerConfig { get; set; }

        public static async Task ProduceCloudEvent<T>(T eventData, string eventType, string topic)
        {
            using (var schemaRegistry = new CachedSchemaRegistryClient(SchemaRegistryConfig))
            using (var producer = new ProducerBuilder<string?, byte[]>(ProducerConfig).Build())
            {
                var valueSerializer = new AvroSerializer<T>(schemaRegistry);
                try
                {
                    var cloudEvent = new CloudEvent
                    {
                        Id = Guid.NewGuid().ToString(),
                        Type = eventType,
                        Source = new Uri("https://example.com/"),
                        Time = DateTimeOffset.UtcNow,
                        DataContentType = "application/avro",
                        // data is serialized using Confluent.SchemaRegistry.Serdes
                        // with this in place schema registry is used to check validity of the CloudEvent.Data
                        // see https://github.com/confluentinc/confluent-kafka-dotnet
                        Data = await valueSerializer.SerializeAsync(
                            eventData,
                            new SerializationContext(
                                MessageComponentType.Value,
                                topic)),
                    };

                    // this CloudEvent extension attribute will be used as message key when CloudEvent is converted to kafka message
                    cloudEvent[Partitioning.PartitionKeyAttribute] = cloudEvent.Id;

                    // get schema used to serialize event data
                    // note: subject name strategy is assumed to be default - topic
                    var schemaId = (await schemaRegistry.GetLatestSchemaAsync(SubjectNameStrategy.Topic.ConstructValueSubjectName(topic))).Id;
                    cloudEvent.DataSchema = new Uri($"{SchemaRegistryConfig.Url}/schemas/ids/{schemaId}/schema/");

                    // CloudEvent is converted to kafka message using CloudNative.CloudEvents.NewtonsoftJson.JsonEventFormatter
                    // because it can be used with ContentMode.Binary, AvroFormatter cannot
                    // in binary mode message value will be only content of CloudEvent.Data property
                    // all other properties will be contained in message header
                    // see https://github.com/cloudevents/sdk-csharp
                    var message = cloudEvent.ToKafkaMessage(ContentMode.Binary, new JsonEventFormatter());
                    var result = await producer.ProduceAsync(topic, message);
                    Console.WriteLine($"produced to: {result.TopicPartitionOffset}");
                }
                catch (System.Exception e)
                {
                    Console.WriteLine($"error producing message: {e.Message}");
                }
            }
        }
    }
}