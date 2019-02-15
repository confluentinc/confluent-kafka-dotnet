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
using Confluent.Kafka;
using Confluent.Kafka.Serdes;


namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        public class Utf32Serializer : ISerializer<string>
        {
            public byte[] Serialize(string data, SerializationContext context)
            {
                return Encoding.UTF32.GetBytes(data);
            }
        }

        public class Utf32Deserializer : IDeserializer<string>
        {
            public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
            {
                if (isNull) { return null; }
                return Encoding.UTF32.GetString(data);
            }
        }

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
            // be inferred from type. Also produce one header ("utf32_header") using a custom serializer.
            DeliveryResult<string, int> dr = null;
            using (var producer = new ProducerBuilder<string, int>(producerConfig)
                .SetHeaderSerializer<string>("utf32_header", new Utf32Serializer())
                .Build())
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
                            new Header<string>("utf8_header", "kafak1"),
                            new Header<string>("utf32_header", "kafka2"),
                            new Header<byte[]>("bytearray_header", new byte[] { 50 })
                        }
                    }
                ).GetAwaiter().GetResult();
            }

            // types are not encoded with the data, so when deserializing, if deserializers
            // are not specified, headers will be of type Header<byte[]>.
            // Note: header deserializer integration is really useful in the Avro/SR case. 
            // In the case of string (i expect very common), it's not that compelling - users
            // may likely prefer to bypass it and deserilize as shown below using the Encoding
            // class. In the int/long case, the built in deserializers capability would be
            // preferable as the hard work in making sure data is interpreted as network byte
            // order regardless of platform has been done, and that functionality is not
            // available off-the-shelf with standard libaries (there's functionality to help,
            // but it's not as clean a solution as what we provide).
            using (var consumer = new ConsumerBuilder<string, int>(consumerConfig).Build())
            {
                consumer.Assign(dr.TopicPartitionOffset);
                var cr = consumer.Consume(TimeSpan.FromSeconds(10));

                // The non-generic GetLast and GetValue methods are defined as a convenience and
                // return byte[]. They will throw an InvalidOperationException if T in Header<T>
                // is not byte[].
                Assert.Equal(4, cr.Headers.GetLast("int_header").Length);
                Assert.Equal(4, cr.Headers.GetLast<byte[]>("int_header").Length);
                Assert.Equal(4, cr.Headers[1].GetValue().Length);
                Assert.Equal(4, cr.Headers[1].GetValue<byte[]>().Length);

                Assert.True(cr.Headers.TryGetLast("int_header", out byte[] intHeaderBytes));
                Assert.Equal(4, intHeaderBytes.Length);

                Assert.Equal("kafka1", Encoding.UTF8.GetString(cr.Headers.GetLast("utf8_header")));
                Assert.Equal("kafka2", Encoding.UTF32.GetString(cr.Headers.GetLast("utf32_header")));

                Assert.Equal(new byte[] { 50 }, cr.Headers.GetLast("bytearray_header"));

                // the type is Header<byte[]> since no deserialization has been specified.
                Assert.Throws<InvalidOperationException>(() => cr.Headers.GetLast<int>("int_header"));
                Assert.Throws<InvalidOperationException>(() => cr.Headers.GetLast<string>("utf8_hedaer"));
            }

            // specify deserializers for all header keys. If no deserializer is specified, the default
            // for the specified generic type is used.
            using (var consumer = new ConsumerBuilder<string, int>(consumerConfig)
                .SetHeaderDeserializer<Null>("null_header")
                .SetHeaderDeserializer<int>("int_header")
                .SetHeaderDeserializer<long>("long_header")
                .SetHeaderDeserializer<float>("float_header")
                .SetHeaderDeserializer<double>("double_header")
                .SetHeaderDeserializer<string>("utf8_header", Deserializers.Utf8) // can set deserializers explicitly.
                .SetHeaderDeserializer<string>("utf32_header", new Utf32Deserializer())
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

                // Demonstrating possibly the worst aspect of the api. The non-generic GetLast / GetValue
                // methods will blow up when used on Header<T> where T is not byte[]. People may find that
                // unexpected. However, these methods are necessary in order to make a terse api for the
                // case where people want to by-pass inbuilt deserialization, which they may rightly want
                // to do if all their header values are string.
                Assert.Throws<InvalidOperationException>(() => cr.Headers.GetLast("int_header"));
                Assert.Throws<InvalidOperationException>(() => cr.Headers[1].GetValue());

                // some other things that will produce runtime exceptions:
                Assert.Throws<InvalidOperationException>(() => cr.Headers.TryGetLast("int_header", out byte[] wontWork));
                Assert.Throws<InvalidOperationException>(() => cr.Headers.GetLast<byte[]>("int_header"));

                Assert.Throws<InvalidOperationException>(() => cr.Headers.GetLast<string>("int_header"));
                Assert.Throws<InvalidOperationException>(() => cr.Headers.TryGetLast("int_header", out string wontWork2));
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