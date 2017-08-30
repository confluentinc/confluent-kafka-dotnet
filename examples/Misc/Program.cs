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
using System.Linq;
using System.Collections.Generic;


namespace Confluent.Kafka.Examples.Misc
{
    public class Program
    {
        static string ToString(int[] array) => $"[{string.Join(", ", array)}]";

        static void ListGroups(string brokerList)
        {
            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };

            using (var producer = new Producer(config))
            {
                var groups = producer.ListGroups(TimeSpan.FromSeconds(10));
                Console.WriteLine($"Consumer Groups:");
                foreach (var g in groups)
                {
                    Console.WriteLine($"  Group: {g.Group} {g.Error} {g.State}");
                    Console.WriteLine($"  Broker: {g.Broker.BrokerId} {g.Broker.Host}:{g.Broker.Port}");
                    Console.WriteLine($"  Protocol: {g.ProtocolType} {g.Protocol}");
                    Console.WriteLine($"  Members:");
                    foreach (var m in g.Members)
                    {
                        Console.WriteLine($"    {m.MemberId} {m.ClientId} {m.ClientHost}");
                        Console.WriteLine($"    Metadata: {m.MemberMetadata.Length} bytes");
                        Console.WriteLine($"    Assignment: {m.MemberAssignment.Length} bytes");
                    }
                }
            }
        }

        static void PrintMetadata(string brokerList)
        {
            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
            using (var producer = new Producer(config))
            {
                var meta = producer.GetMetadata(true, null);
                Console.WriteLine($"{meta.OriginatingBrokerId} {meta.OriginatingBrokerName}");
                meta.Brokers.ForEach(broker =>
                    Console.WriteLine($"Broker: {broker.BrokerId} {broker.Host}:{broker.Port}"));

                meta.Topics.ForEach(topic =>
                {
                    Console.WriteLine($"Topic: {topic.Topic} {topic.Error}");
                    topic.Partitions.ForEach(partition =>
                    {
                        Console.WriteLine($"  Partition: {partition.PartitionId}");
                        Console.WriteLine($"    Replicas: {ToString(partition.Replicas)}");
                        Console.WriteLine($"    InSyncReplicas: {ToString(partition.InSyncReplicas)}");
                    });
                });
            }
        }

        static void DeleteTopicsProducer(string brokerList, IEnumerable<string> topics)
        {
            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
            using (var producer = new Producer(config))
            {
                IAdmin admin = producer;
                try
                {
                    var result = admin.DeleteTopics(topics, TimeSpan.FromSeconds(10), true);
                    foreach(var topicError in result)
                    {
                        var message = topicError.Error.HasError
                            ? " deleted"
                            : topicError.Error.Code == ErrorCode.RequestTimedOut ? 
                              "marked for deletion" : $"deletion encoutered error {topicError.Error}";
                        Console.WriteLine($"  Topic: {topicError.Topic} {message}");
                    }
                }
                catch(KafkaException e)
                {
                    Console.WriteLine($"Error while deleting : {e}");
                }
            }
        }

        static void DeleteTopicsAdmin(string brokerList, IEnumerable<string> topics)
        {
            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };
            using (var admin = new AdminClient(config))
            {
                try
                {
                    var result = admin.DeleteTopics(topics, TimeSpan.FromSeconds(10), true);
                    foreach (var topicError in result)
                    {
                        var message = topicError.Error.HasError
                            ? " deleted"
                            : topicError.Error.Code == ErrorCode.RequestTimedOut ?
                              "marked for deletion" : $"deletion encoutered error {topicError.Error}";
                        Console.WriteLine($"  Topic: {topicError.Topic} {message}");
                    }
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Error while deleting : {e}");
                }
            }
        }

        public static void Main(string[] args)
        {
            args = new string[] { "172.22.12.3:49092", "--delete-topics", "tazezae", "poirksd", "topicToDelete" };
            Console.WriteLine($"Hello RdKafka!");
            Console.WriteLine($"{Library.Version:X}");
            Console.WriteLine($"{Library.VersionString}");
            Console.WriteLine($"{string.Join(", ", Library.DebugContexts)}");

            switch (args[1])
            {
                case "--list-groups":
                    ListGroups(args[0]);
                    break;
                case "--metadata":
                    PrintMetadata(args[0]);
                    break;
                case "--delete-topics":
                    DeleteTopicsProducer(args[0], args.Skip(2));
                    break;
                case "--delete-topics-admin":
                    DeleteTopicsAdmin(args[0], args.Skip(2));
                    break;

            }
        }
    }
}
