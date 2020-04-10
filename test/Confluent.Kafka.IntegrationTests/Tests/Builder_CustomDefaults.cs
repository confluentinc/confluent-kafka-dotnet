// Copyright 2016-2019 Confluent Inc.
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
using System.Text;
using System.Collections.Generic;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Create a custom builder for producer / consumer that uses
    ///     different default serdes for string. This test is primarily
    ///     to demonstrate the suitability of the API for this purpose.
    /// </summary>
    public partial class Tests
    {
        class MyProducerBuilder<K, V> : ProducerBuilder<K, V>
        {
            class Utf32Serializer : ISerializer<string>
            {
                public byte[] Serialize(string data, SerializationContext context)
                {
                    return Encoding.UTF32.GetBytes(data);
                }
            }

            public MyProducerBuilder(IEnumerable<KeyValuePair<string, string>> config) : base(config) { }

            public override IProducer<K, V> Build()
            {
                if (typeof(K) == typeof(string))
                {
                    if (KeySerializer == null && AsyncKeySerializer == null)
                    {
                        this.KeySerializer = (ISerializer<K>)(new Utf32Serializer());
                    }
                }

                if (typeof(V) == typeof(string))
                {
                    if (ValueSerializer == null && AsyncValueSerializer == null)
                    {
                        this.ValueSerializer = (ISerializer<V>)(new Utf32Serializer());
                    }
                }
                
                return base.Build();
            }
        }

        class MyConsumerBuilder<K, V> : ConsumerBuilder<K, V>
        {
            class Utf32Deserializer : IDeserializer<string>
            {
                public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
                {
                    if (isNull) { return null; }
                    return Encoding.UTF32.GetString(data);
                }
            }

            public MyConsumerBuilder(IEnumerable<KeyValuePair<string, string>> config) : base(config) { }

            public override IConsumer<K, V> Build()
            {
                if (typeof(K) == typeof(string))
                {
                    if (KeyDeserializer == null)
                    {
                        this.KeyDeserializer = (IDeserializer<K>)new Utf32Deserializer();
                    }
                }
                
                if (typeof(V) == typeof(string))
                {
                    if (ValueDeserializer == null)
                    {
                        this.ValueDeserializer = (IDeserializer<V>) new Utf32Deserializer();
                    }
                }

                return base.Build();
            }
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public void ProducerBuilder(string bootstrapServers)
        {
            LogToFile("start Builder_CustomDefaults");

            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var dr = new DeliveryResult<string, string>();
            using (var p = new MyProducerBuilder<string, string>(producerConfig).Build())
            {
                dr = p.ProduceAsync(singlePartitionTopic, new Message<string, string> { Key = "abc", Value = "123" }).Result;
            }
            
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString()
            };
        
            using (var c = new MyConsumerBuilder<string, string>(consumerConfig).Build())
            {
                c.Assign(dr.TopicPartitionOffset);
                var cr = c.Consume(TimeSpan.FromSeconds(10));
                Assert.Equal("abc", cr.Message.Key);
                Assert.Equal("123", cr.Message.Value);
            }

            using (var c = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build())
            {
                c.Assign(dr.TopicPartitionOffset);
                var cr = c.Consume(TimeSpan.FromSeconds(10));
                // check that each character is serialized into 4 bytes.
                Assert.Equal(3*4, cr.Message.Key.Length);
                Assert.Equal(3*4, cr.Message.Value.Length);
            }

            Assert.Equal(0, Library.HandleCount);
            LogToFile("end   Builder_CustomDefaults");
        }
    }
}
