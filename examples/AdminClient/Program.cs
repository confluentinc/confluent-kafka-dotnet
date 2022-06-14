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
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using System.Linq;
using System.Collections.Generic;

namespace Confluent.Kafka.Examples
{
    public class Program
    {
        static string ToString(int[] array) => $"[{string.Join(", ", array)}]";

        static void ListGroups(string bootstrapServers)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                // Warning: The API for this functionality is subject to change.
                var groups = adminClient.ListGroups(TimeSpan.FromSeconds(10));
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

        static void PrintMetadata(string bootstrapServers)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                // Warning: The API for this functionality is subject to change.
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
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

        static async Task CreateTopicAsync(string bootstrapServers, string topicName)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] { 
                        new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 1 } });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }

        static List<AclBinding> ParseAclBindings(string[] args)
        {
            int nAclBindings = args.Length / 7;
            var aclBindings = new List<AclBinding>();
            for (int i = 0; i < nAclBindings; i++)
            {
                var baseArg = i * 7;
                object resourceType, resourcePatternType, operation, permissionType;
                var parseCorrect = true;
                parseCorrect &= Enum.TryParse(typeof(ResourceType), args[baseArg], out resourceType);
                parseCorrect &= Enum.TryParse(typeof(ResourcePatternType), args[baseArg + 2], out resourcePatternType);
                parseCorrect &= Enum.TryParse(typeof(AclOperation), args[baseArg + 5], out operation);
                parseCorrect &= Enum.TryParse(typeof(AclPermissionType), args[baseArg + 6], out permissionType);

                if (!parseCorrect) return null;

                aclBindings.Add(new AclBinding() {
                    Type = (ResourceType) resourceType,
                    Name = args[baseArg + 1],
                    ResourcePatternType = (ResourcePatternType) resourcePatternType,
                    Principal = args[baseArg + 3],
                    Host = args[baseArg + 4],
                    Operation = (AclOperation) operation,
                    PermissionType = (AclPermissionType) permissionType,
                });
            }
            return aclBindings;
        }

        static async Task CreateAclsAsync(string bootstrapServers, List<AclBinding> aclBindings)
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var results = await adminClient.CreateAclsAsync(aclBindings);
                    Console.WriteLine("All the create ACL operations completed successfully");
                }
                catch (CreateAclsException e)
                {
                    Console.WriteLine("Some ACL operations failed");
                    int i = 0;
                    foreach (var result in e.Results)
                    {
                        if (!result.Error.IsError)
                        {
                            Console.WriteLine($"Create ACLs operation {i} completed successfully");
                        }
                        else
                        {
                            Console.WriteLine($"An error occurred in create ACL operation {i}: Code: {result.Error.Code}" +
                            $", Reason: {result.Error.Reason}");
                        }
                        i++;
                    }
                }
            }
        }

        public static async Task Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("usage: .. <bootstrapServers> <list-groups|metadata|library-version|create-topic|create-acls> ..");
                System.Environment.Exit(1);
            }

            var commandArgs = args.Skip(2).ToArray();
            var numCommandArgs = commandArgs.Length;
            switch (args[1])
            {
                case "library-version":
                    Console.WriteLine($"librdkafka Version: {Library.VersionString} ({Library.Version:X})");
                    Console.WriteLine($"Debug Contexts: {string.Join(", ", Library.DebugContexts)}");
                    break;
                case "list-groups":
                    ListGroups(args[0]);
                    break;
                case "metadata":
                    PrintMetadata(args[0]);
                    break;
                case "create-topic":
                    await CreateTopicAsync(args[0], args[2]);
                    break;
                case "create-acls":
                    var printUsage = numCommandArgs == 0 || numCommandArgs % 7 != 0;
                    List<AclBinding> aclBindings = null;
                    if (!printUsage)
                    {
                        aclBindings = ParseAclBindings(commandArgs);
                    }
                    printUsage = aclBindings == null;

                    if (printUsage)
                    {
                        Console.WriteLine("usage: .. <bootstrapServers> create-acls <resource_type1> <resource_name1> <resource_patter_type1> "+
                        "<principal1> <host1> <operation1> <permission_type1> ..");
                        System.Environment.Exit(1);
                    }
                    await CreateAclsAsync(args[0], aclBindings);
                    break;
                default:
                    Console.WriteLine($"unknown command: {args[1]}");
                    break;
            }
        }
    }
}
