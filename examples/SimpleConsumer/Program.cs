// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Examples.SimpleConsumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = "localhost:9092";
            var topic = "test";

            var config = new Dictionary<string, object>
            {
                { "group.id", "simple-csharp-consumer" },
                { "bootstrap.servers", brokerList },
                { "enable.auto.commit", false },
                //{"debug", "cgrp,topic" }
            };

            using (var consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.OnOffsetsCommitted += (_, commit) =>
                {
                    Console.WriteLine($"[{string.Join(", ", commit.Offsets)}]");

                    if (commit.Error)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                    Console.WriteLine($"Successfully committed offsets: [{string.Join(", ", commit.Offsets)}]");
                };

                consumer.OnPartitionsAssigned += (_, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    consumer.Assign(partitions);
                };

                consumer.OnPartitionsRevoked += (_, partitions) =>
                {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    consumer.CommitAsync(consumer.Position(consumer.Assignment).Select(x=>x.TopicPartitionOffset));
                    consumer.Unassign();
                };

                consumer.Subscribe(topic);
                //consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, Offset.Invalid) });

                while (true)
                {
                    Message<Null, string> msg;
                    if (consumer.Consume(out msg, TimeSpan.FromSeconds(1)))
                    {
                        Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
                    }
                }
            }
        }
    }
}
