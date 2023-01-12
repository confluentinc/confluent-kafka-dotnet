using Avro.Generic;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.NewtonsoftJson;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace AvroCloudEvents
{
    public static class MyConsumer
    {
        static MyConsumer()
        {
            SchemaRegistryConfig = new SchemaRegistryConfig();
            ConsumerConfig = new ConsumerConfig();    
        }

        public static SchemaRegistryConfig SchemaRegistryConfig { get; set; }

        public static ConsumerConfig ConsumerConfig { get; set; }

        public static void StartConsumerCloudEvent(string topic, CancellationTokenSource cts)
        {
            var consumeTask = Task.Run(() =>
            {
                using (var schemaRegistry = new CachedSchemaRegistryClient(SchemaRegistryConfig))
                using (var consumer =
                    new ConsumerBuilder<string?, byte[]>(ConsumerConfig)
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build())
                {
                    var valueDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistry);
                    consumer.Subscribe(topic);

                    try
                    {
                        while (true)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(cts.Token);
                                var cloudEvent = consumeResult.Message.ToCloudEvent(new JsonEventFormatter());
                                var eventData = valueDeserializer.DeserializeAsync(
                                    cloudEvent.Data == null ? ReadOnlyMemory<byte>.Empty : (byte[])cloudEvent.Data,
                                    cloudEvent.Data == null,
                                    new SerializationContext(
                                        MessageComponentType.Value,
                                        topic,
                                        consumeResult.Message.Headers)).Result;
                                Console.WriteLine($"cloudevent id: {cloudEvent.Id}");
                                Console.WriteLine($"cloudevent type: {cloudEvent.Type}");
                                Console.WriteLine($"cloudevent data content type: {cloudEvent.DataContentType}");
                                Console.WriteLine($"cloudevent data schema: {cloudEvent.DataSchema}");
                                Console.WriteLine($"key: {consumeResult.Message.Key}");
                                Console.WriteLine($"value: {eventData}");
                            }
                            catch (ConsumeException e)
                            {
                                Console.WriteLine($"Consume error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                    }
                }
            });
        }
    }
}