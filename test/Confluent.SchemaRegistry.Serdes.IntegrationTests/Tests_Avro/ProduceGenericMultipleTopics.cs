using System;
using Confluent.Kafka;
using Avro;
using Avro.Generic;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that messages produced with the Avro serializer can be consumed with the
        ///     Avro deserializer.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceGenericMultipleTopics(string bootstrapServers, string schemaRegistryServers)
        {
            var s = (RecordSchema)RecordSchema.Parse(
                @"{
                    ""namespace"": ""Confluent.Kafka.Examples.AvroSpecific"",
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string""},
                        {""name"": ""favorite_number"",  ""type"": [""int"", ""null""]},
                        {""name"": ""favorite_color"", ""type"": [""string"", ""null""]}
                    ]
                  }"
            );

            var config = new ProducerConfig { BootstrapServers = bootstrapServers };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryServers };

            var topic = Guid.NewGuid().ToString();
            var topic2 = Guid.NewGuid().ToString();

            DeliveryResult<Null, GenericRecord> dr;
            DeliveryResult<Null, GenericRecord> dr2;

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var p =
                new ProducerBuilder<Null, GenericRecord>(config)
                    .SetKeySerializer(Serializers.Null)
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
                    .Build())
            {
                var record = new GenericRecord(s);
                record.Add("name", "my name 2");
                record.Add("favorite_number", 44);
                record.Add("favorite_color", null);
                dr = p.ProduceAsync(topic, new Message<Null, GenericRecord> { Key = null, Value = record }).Result;
                dr2 = p.ProduceAsync(topic2, new Message<Null, GenericRecord> { Key = null, Value = record }).Result;
            }

            Assert.Null(dr.Key);
            Assert.NotNull(dr.Value);

            Assert.Null(dr2.Key);
            Assert.NotNull(dr2.Value);

        }

    }
}
