// Copyright 2016-2017 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

//using Confluent.Kafka.Examples.AvroGeneric;
//using System;
//using System.Collections.Generic;
//using Xunit;
//using ApacheAvro = Avro;

//namespace Confluent.Kafka.Avro.IntegrationTests
//{
//    public static partial class Tests
//    {
//        /// <summary>
//        ///     Test that produces specific avro messages then consume them
//        /// </summary>
//        [Fact]
//        public static void AvroSpecific()
//        {
//            string bootstrapServers = "localhost:9092";
//            string topic = "test";
//            string schemaRegistryServer = "http://localhost:8081";

//            var producerConfig = new Dictionary<string, object>
//            {
//                { "bootstrap.servers", bootstrapServers },
//                { "api.version.request", true }
//            };

//            var consumerConfig = new Dictionary<string, object>
//            {
//                { "group.id", Guid.NewGuid().ToString() },
//                { "bootstrap.servers", bootstrapServers },
//                { "session.timeout.ms", 6000 },
//                { "api.version.request", true }
//            };

//            var sr = new SchemaRegistry.CachedSchemaRegistryClient(schemaRegistryServer);
//            using (var producer = new Producer<string, User>(producerConfig, new AvroSerializer<string>(sr, true), new AvroSerializer<User>(sr, false)))
//            {
//                for (int i = 0; i < 100; i++)
//                {
//                    var user = new User
//                    {
//                        name = i.ToString(),
//                        favorite_number = i,
//                        favorite_color = "blue"
//                    };
//                    producer.ProduceAsync(topic, user.name, user);
//                }
//                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
//            }

//            using (var consumer = new Consumer<string, User>(consumerConfig, new AvroDeserializer<string>(sr, true), new AvroDeserializer<User>(sr, false)))
//            {
//                bool done = false;
//                int i = 0;
//                consumer.OnMessage += (o, e) =>
//                {
//                    Assert.Equal(i.ToString(), e.Key);
//                    Assert.Equal(i.ToString(), e.Value.name);
//                    Assert.Equal(i, e.Value.favorite_number);
//                    Assert.Equal("blue", e.Value.favorite_color);

//                    i++;
//                };

//                consumer.OnPartitionEOF += (o, e)
//                    => done = true;

//                while (!done)
//                {
//                    consumer.Poll(TimeSpan.FromMilliseconds(100));
//                }

//                Assert.Equal(100, i);
//            }
//        }

//        /// <summary>
//        ///     Test that produces generic avro messages then consume them
//        /// </summary>
//        [Fact]
//        public static void AvroGeneric()
//        {
//            // This test assumes broker v0.10.0 or higher:
//            // https://github.com/edenhill/librdkafka/wiki/Broker-version-compatibility
//            string bootstrapServers = "localhost:9092";
//            string topic = "otherTest";

//            var producerConfig = new Dictionary<string, object>
//            {
//                { "bootstrap.servers", bootstrapServers },
//                { "api.version.request", true }
//            };

//            var consumerConfig = new Dictionary<string, object>
//            {
//                { "group.id", Guid.NewGuid().ToString() },
//                { "bootstrap.servers", bootstrapServers },
//                { "session.timeout.ms", 6000 },
//                { "api.version.request", true }
//            };

//            var sr = new SchemaRegistry.CachedSchemaRegistryClient("http://localhost:8081");

//            string userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
//                                "\"name\": \"User\"," +
//                                "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
//            var schema = ApacheAvro.Schema.Parse(userSchema) as ApacheAvro.RecordSchema;
//            var avroRecord = new ApacheAvro.Generic.GenericRecord(schema);
//            avroRecord.Add("name", "testUser");

//            using (var producer = new Producer<object, object>(producerConfig, new Serializer.AvroSerializer(sr, true), new Serializer.AvroSerializer(sr, false)))
//            {
//                for (int i = 0; i < 100; i++)
//                {
//                    // we reuse the same object (we could recreate one)
//                    avroRecord.Add("name", $"user {i}");
//                    producer.ProduceAsync(topic, i, avroRecord);
//                    // produce of type <int, genericrecord>
//                }
//                Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(10)));
//            }

//            // actually, we return an object which is either a GenericRecord, GenericEnum, GenericFixed or a primitive type
//            // retrieving a dynamic is easier in our case
//            var deserializer = new AvroGenericDeserializer(sr);
//            using (var consumer = new Consumer<dynamic, dynamic>(consumerConfig, deserializer, deserializer))
//            {
//                bool done = false;
//                int i = 0;
//                consumer.OnMessage += (o, e) =>
//                {
//                    // we know the type, so we can fetch directly
//                    Assert.Equal(i, e.Key);
//                    Assert.Equal($"user {i}", e.Value["name"]);

//                    i++;
//                };

//                consumer.OnPartitionEOF += (o, e)
//                    => done = true;

//                consumer.Assign(...);
//                while (!done)
//                {
//                    consumer.Poll(TimeSpan.FromMilliseconds(100));
//                }

//                Assert.Equal(100, i);
//            }

//            //we can also use without dynamic
//            using (var consumer = new Consumer<object, object>(consumerConfig, deserializer, deserializer))
//            {
//                bool done = false;
//                int i = 0;
//                consumer.OnMessage += (o, e) =>
//                {
//                    Assert.Equal(i, (int)e.Key);
//                    var record = (ApacheAvro.Generic.GenericRecord)e.Value;
//                    Assert.Equal($"user {i}", record["name"]);

//                    i++;
//                };

//                consumer.OnPartitionEOF += (o, e)
//                    => done = true;

//                consumer.Assign(...);
//                while (!done)
//                {
//                    consumer.Poll(TimeSpan.FromMilliseconds(100));
//                }

//                Assert.Equal(100, i);
//            }



//            /* Some though:
//             * We could consume a custom AvroRecord type rather than object, which would contain the schema + object.
//             * May be way easier to parse the correct type when we consume from some unknown topic for example
//             * 
//             * I don't like having two different avroserializer / avrodeserializer because of the "iskey" only.
//             * I know it's the java way, but I prefer reusing the same object when possible (and we have cache)
//             * I'm really more for the "isKey" in the serialize/deserialize method
//             * 
//             * Serialization / deserialization is not async, but make some network call. 
//             * Perhaps add a "IsAsyncSerializer" in ISerializerInterface with the method SerializerAsync
//             * and at execution, choose the one regarding this property? This is not urgent
//             */
//        }
//    }
//}
