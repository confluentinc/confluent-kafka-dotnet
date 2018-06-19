﻿using System;
using System.Collections.Generic;
using Confluent.Kafka.Examples.AvroSpecific;
using Confluent.Kafka.Serialization;
using Avro;
using Avro.Generic;
using Xunit;

namespace Confluent.Kafka.Avro.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Test that messages produced with the avro serializer can be consumed with the
        ///     avro deserializer.
        /// </summary>
        [Theory, MemberData(nameof(TestParameters))]
        public static void ProduceGenericMultipleTopics(string bootstrapServers, string schemaRegistryServers)
        {
            var s = (RecordSchema)Schema.Parse(
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

            var config = new Dictionary<string, object>()
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryServers }
            };

            var topic = Guid.NewGuid().ToString();
            var topic2 = Guid.NewGuid().ToString();

            Message<Null, GenericRecord> dr;
            Message<Null, GenericRecord> dr2;

            using (var p = new Producer<Null, GenericRecord>(config, null, new AvroSerializer<GenericRecord>()))
            {
                var record = new GenericRecord(s);
                record.Add("name", "my name 2");
                record.Add("favorite_number", 44);
                record.Add("favorite_color", null);
                dr = p.ProduceAsync(topic, null, record).Result;
                dr2 = p.ProduceAsync(topic2, null, record).Result;
            }

            Assert.Null(dr.Key);
            Assert.NotNull(dr.Value);

            Assert.Null(dr2.Key);
            Assert.NotNull(dr2.Value);

        }

    }
}
