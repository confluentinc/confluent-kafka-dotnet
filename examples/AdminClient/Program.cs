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

        static async Task CreateTopicAsync(string bootstrapServers, string[] commandArgs)
        {
            if (commandArgs.Length != 1)
            {
                Console.WriteLine("usage: .. <bootstrapServers> create-topic <topic_name>");
                Environment.ExitCode = 1;
                return;
            }

            var topicName = commandArgs[0];

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[] { 
                        new TopicSpecification { Name = topicName, ReplicationFactor = 1, NumPartitions = 1 } });
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"An error occurred creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                }
            }
        }

        static List<T> ParseAclBindings<T>(string[] args) where T: AclBinding, new()
        {
            int nAclBindings = args.Length / 7;
            var aclBindings = new List<T>();
            for (int i = 0; i < nAclBindings; ++i)
            {
                var baseArg = i * 7;
                var parseCorrect = true;
                parseCorrect &= Enum.TryParse(typeof(ResourceType), args[baseArg], out object resourceType);
                parseCorrect &= Enum.TryParse(typeof(ResourcePatternType), args[baseArg + 2], out object resourcePatternType);
                parseCorrect &= Enum.TryParse(typeof(AclOperation), args[baseArg + 5], out object operation);
                parseCorrect &= Enum.TryParse(typeof(AclPermissionType), args[baseArg + 6], out object permissionType);

                if (!parseCorrect) return null;

                var name = args[baseArg + 1];
                var principal = args[baseArg + 3];
                var host = args[baseArg + 4];

                if (name == "") name = null;
                if (principal == "") principal = null;
                if (host == "") host = null;

                aclBindings.Add(new T()
                {
                    Type = (ResourceType) resourceType,
                    Name = name,
                    ResourcePatternType = (ResourcePatternType) resourcePatternType,
                    Principal = principal,
                    Host = host,
                    Operation = (AclOperation) operation,
                    PermissionType = (AclPermissionType) permissionType,
                });
            }
            return aclBindings;
        }


        static void PrintAclBindings(List<AclBinding> aclBindings)
        {
            foreach (AclBinding aclBinding in aclBindings)
            {
                Console.WriteLine($"\t{aclBinding}");
            }
        }

        static async Task CreateAclsAsync(string bootstrapServers, string[] commandArgs)
        {
            var numCommandArgs = commandArgs.Length;
            var printUsage = numCommandArgs == 0 || numCommandArgs % 7 != 0;
            List<AclBinding> aclBindings = null;
            if (!printUsage)
            {
                aclBindings = ParseAclBindings<AclBinding>(commandArgs);
            }
            printUsage = aclBindings == null;

            if (printUsage)
            {
                Console.WriteLine("usage: .. <bootstrapServers> create-acls <resource_type1> <resource_name1> <resource_patter_type1> "+
                "<principal1> <host1> <operation1> <permission_type1> ..");
                Environment.ExitCode = 1;
                return;
            }

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
                        ++i;
                    }
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"An error occurred calling the CreateAcls operation: {e.Message}");
                }
            }
        }

        static async Task DescribeAclsAsync(string bootstrapServers, string[] commandArgs)
        {
            var numCommandArgs = commandArgs.Length;
            var printUsage = numCommandArgs != 7;
            List<AclBindingFilter> aclBindingFilters = null;
            if (!printUsage)
            {
                aclBindingFilters = ParseAclBindings<AclBindingFilter>(commandArgs);
            }
            printUsage = aclBindingFilters == null;

            if (printUsage)
            {
                Console.WriteLine("usage: .. <bootstrapServers> describe-acls <resource_type> <resource_name> <resource_patter_type> "+
                "<principal> <host> <operation> <permission_type>");
                Environment.ExitCode = 1;
                return;
            }

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var result = await adminClient.DescribeAclsAsync(aclBindingFilters[0]);
                    Console.WriteLine($"Matching ACLs");
                    PrintAclBindings(result.AclBindings);
                }
                catch (DescribeAclsException e)
                {
                    var result = e.Result;
                    Console.WriteLine("Describe ACLs operation failed");
                    Console.WriteLine($"An error occurred in describe ACLs operation: Code: {result.Error.Code}" +
                        $", Reason: {result.Error.Reason}");
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"An error occurred calling the describe ACLs operation: {e.Message}");
                }
            }
        }

        static async Task DeleteAclsAsync(string bootstrapServers, string[] commandArgs)
        {
            var numCommandArgs = commandArgs.Length;
            var printUsage = numCommandArgs == 0 || numCommandArgs % 7 != 0;
            List<AclBindingFilter> aclBindingFilters = null;
            if (!printUsage)
            {
                aclBindingFilters = ParseAclBindings<AclBindingFilter>(commandArgs);
            }
            printUsage = aclBindingFilters == null;

            if (printUsage)
            {
                Console.WriteLine("usage: .. <bootstrapServers> delete-acls <resource_type1> <resource_name1> <resource_patter_type1> "+
                "<principal1> <host1> <operation1> <permission_type1> ..");
                Environment.ExitCode = 1;
                return;
            }

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var results = await adminClient.DeleteAclsAsync(aclBindingFilters);
                    Console.WriteLine("All the delete ACL operations completed successfully");
                    int i = 0;
                    foreach (var result in results)
                    {
                        Console.WriteLine($"Deleted ACLs in operation {i}");
                        PrintAclBindings(result.AclBindings);
                        ++i;
                    }
                }
                catch (DeleteAclsException e)
                {
                    Console.WriteLine("Some ACL operations failed");
                    int i = 0;
                    foreach (var result in e.Results)
                    {
                        if (!result.Error.IsError)
                        {
                            Console.WriteLine($"Delete ACLs operation {i} completed successfully");
                            Console.WriteLine($"Deleted ACLs in operation {i}");
                            PrintAclBindings(result.AclBindings);
                        }
                        else
                        {
                            Console.WriteLine($"An error occurred in delete ACL operation {i}: Code: {result.Error.Code}" +
                            $", Reason: {result.Error.Reason}");
                        }
                        ++i;
                    }
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"An error occurred calling the DeleteAcls operation: {e.Message}");
                }
            }
        }

        public static async Task Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("usage: .. <bootstrapServers> <list-groups|metadata|library-version|create-topic|create-acls|describe-acls|delete-acls> ..");
                Environment.ExitCode = 1;
                return;
            }

            var bootstrapServers = args[0];
            var command = args[1];
            var commandArgs = args.Skip(2).ToArray();
            switch (command)
            {
                case "library-version":
                    Console.WriteLine($"librdkafka Version: {Library.VersionString} ({Library.Version:X})");
                    Console.WriteLine($"Debug Contexts: {string.Join(", ", Library.DebugContexts)}");
                    break;
                case "list-groups":
                    ListGroups(bootstrapServers);
                    break;
                case "metadata":
                    PrintMetadata(bootstrapServers);
                    break;
                case "create-topic":
                    await CreateTopicAsync(bootstrapServers, commandArgs);
                    break;
                case "create-acls":
                    await CreateAclsAsync(bootstrapServers, commandArgs);
                    break;
                case "describe-acls":
                    await DescribeAclsAsync(bootstrapServers, commandArgs);
                    break;
                case "delete-acls":
                    await DeleteAclsAsync(bootstrapServers, commandArgs);
                    break;
                default:
                    Console.WriteLine($"unknown command: {command}");
                    break;
            }
        }
    }
}
