using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Confluent.Kafka.Misc
{
    public class Program
    {
        static string ToString(int[] array) => $"[{string.Join(", ", array)}]";

        static async Task ListGroups(string brokerList)
        {
            using (var producer = new Producer(new Dictionary<string, string> { { "bootstrap.servers", brokerList } }))
            {
                var groups = await producer.ListGroups(TimeSpan.FromSeconds(10));
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
                        //Console.WriteLine(System.Text.Encoding.UTF8.GetString(m.MemberMetadata));
                        Console.WriteLine($"    Assignment: {m.MemberAssignment.Length} bytes");
                        //Console.WriteLine(System.Text.Encoding.UTF8.GetString(m.MemberAssignment));
                    }
                }
            }
        }

        static async Task PrintMetadata(string brokerList)
        {
            using (var producer = new Producer(new Dictionary<string, string> { { "bootstrap.servers", brokerList } }))
            {
                var meta = await producer.Metadata();
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

        public static void Main(string[] args)
        {
            Console.WriteLine($"Hello RdKafka!");
            Console.WriteLine($"{Library.Version:X}");
            Console.WriteLine($"{Library.VersionString}");
            Console.WriteLine($"{string.Join(", ", Library.DebugContexts)}");

            if (args.Contains("--list-groups"))
            {
                ListGroups(args[0]).Wait();
            }

            if (args.Contains("--metadata"))
            {
                PrintMetadata(args[0]).Wait();
            }

            if (args.Contains("--dump-config"))
            {
                foreach (var kv in new Config(new Dictionary<string, string>()).Dump())
                {
                    Console.WriteLine($"\"{kv.Key}\": \"{kv.Value}\"");
                }
            }
        }
    }
}
