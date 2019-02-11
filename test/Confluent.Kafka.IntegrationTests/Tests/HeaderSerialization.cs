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

#pragma warning disable xUnit1026

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Confluent.Kafka.Serdes;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        /// <summary>
        ///     Various tests related to header serialization
        /// </summary>
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void HeaderSerialization(string bootstrapServers, string singlePartitionTopic, string partitionedTopic)
        {
            LogToFile("start HeaderSerialization");

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
            };

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                SessionTimeoutMs = 6000
            };

            // produce a bunch of headers corresponding to all the types that have default serializers.
            // it's not necessary to specify a serializer in this case, because an appropriate one can
            // be inferred from type.
            DeliveryResult<string, int> dr = null;
            using (var producer = new ProducerBuilder<string, int>(producerConfig).Build())
            {
                dr = producer.ProduceAsync(
                    singlePartitionTopic,
                    new Message<string, int>
                    {
                        Key = "the_key",
                        Value = 42,
                        Headers = new Headers
                        {
                            new Header<Null>("null_header", null),
                            new Header<int>("int_header", 11),
                            new Header<long>("long_header", 10),
                            new Header<float>("float_header", 3.141f),
                            new Header<double>("double_header", 2.7182),
                            new Header<string>("utf8_header", "kafka"),
                            new Header<byte[]>("bytearray_header", new byte[] { 50 })
                        }
                    }
                ).GetAwaiter().GetResult();
            }

            // to deserialize, the consumer must be told the header types (since this is not encoded
            // with the data). SetHeaderType will use the default deserializers. SetHeaderDeserializer
            // allows a custom sync or async deserializer to be specified.
            using (var consumer = new ConsumerBuilder<string, int>(consumerConfig)
                .SetHeaderType<Null>("null_header")
                .SetHeaderType<int>("int_header")
                .SetHeaderType<long>("long_header")
                .SetHeaderType<float>("float_header")
                .SetHeaderType<double>("double_header")
                .SetHeaderDeserializer<string>("utf8_header", Deserializers.Utf8) // can set deserializers explicitly.
                // .SetHeaderType<byte[]>("bytearray_header") // optional/unnecessary - by default headers values will be byte[].
                .Build())
            {
                consumer.Assign(dr.TopicPartitionOffset);
                var cr = consumer.Consume(TimeSpan.FromSeconds(10));

                // get by name.
                Assert.True(cr.Headers.TryGetLast("null_header", out Null nullHeader));
                Assert.True(cr.Headers.TryGetLast("int_header", out int intHeader));
                Assert.True(cr.Headers.TryGetLast("long_header", out long longHeader));
                Assert.True(cr.Headers.TryGetLast("float_header", out float floatHeader));
                Assert.True(cr.Headers.TryGetLast("double_header", out double doubleHeader));
                Assert.True(cr.Headers.TryGetLast("utf8_header", out string utf8Header));
                Assert.True(cr.Headers.TryGetLast("bytearray_header", out byte[] bytearrayHeader));
                Assert.Equal(null, nullHeader);
                Assert.Equal(11, intHeader);
                Assert.Equal(10, longHeader);
                Assert.Equal(3.141f, floatHeader);
                Assert.Equal(2.7182, doubleHeader);
                Assert.Equal("kafka", utf8Header);
                Assert.Equal(new byte[] { 50 }, bytearrayHeader);

                // get by index.
                Assert.Null(cr.Headers[0].GetValue<Null>());
                Assert.Equal(11, cr.Headers[1].GetValue<int>());
                Assert.Equal(10, cr.Headers[2].GetValue<long>());
                Assert.Equal(3.141f, cr.Headers[3].GetValue<float>());
                Assert.Equal(2.7182, cr.Headers[4].GetValue<double>());
                Assert.Equal("kafka", cr.Headers[5].GetValue<string>());
                Assert.Equal(new byte[] { 50 }, cr.Headers[6].GetValue<byte[]>());
            }

            // test mismatch in the header serializer type that's been explicitly specified and the 
            Assert.Throws<ProduceException<Null,string>>(() =>
            {
                using (var producer = new ProducerBuilder<Null, string>(producerConfig)
                    .SetHeaderSerializer("destination", Serializers.Utf8)
                    .Build())
                {
                    producer.ProduceAsync(
                        singlePartitionTopic,
                        new Message<Null, string>
                        {
                            Value = "invalid header message",
                            Headers = new Headers { new Header<int>("destination", 42) } 
                        }).Wait();
                }
            });

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   HeaderSerialization");
        }
    }
}