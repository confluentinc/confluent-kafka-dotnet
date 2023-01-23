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

        static List<AclBinding> ParseAclBindings(string[] args, bool many)
        {
            var numCommandArgs = args.Length;
            if (many ? (numCommandArgs == 0 || numCommandArgs % 7 != 0)
                     : numCommandArgs != 7)
            {
                throw new ArgumentException("wrong number of arguments");
            }
            int nAclBindings = args.Length / 7;
            var aclBindings = new List<AclBinding>();
            for (int i = 0; i < nAclBindings; ++i)
            {
                var baseArg = i * 7;
                var resourceType = Enum.Parse<ResourceType>(args[baseArg]);
                var name = args[baseArg + 1];
                var resourcePatternType = Enum.Parse<ResourcePatternType>(args[baseArg + 2]);
                var principal = args[baseArg + 3];
                var host = args[baseArg + 4];
                var operation = Enum.Parse<AclOperation>(args[baseArg + 5]);
                var permissionType = Enum.Parse<AclPermissionType>(args[baseArg + 6]);

                if (name == "") { name = null; }
                if (principal == "") { principal = null; }
                if (host == "") { host = null; }

                aclBindings.Add(new AclBinding()
                {
                    Pattern = new ResourcePattern
                    {
                        Type = resourceType,
                        Name = name,
                        ResourcePatternType = resourcePatternType
                    },
                    Entry = new AccessControlEntry
                    {
                        Principal = principal,
                        Host = host,
                        Operation = operation,
                        PermissionType = permissionType
                    }
                });
            }
            return aclBindings;
        }


        static List<AclBindingFilter> ParseAclBindingFilters(string[] args, bool many)
        {
            var aclBindings = ParseAclBindings(args, many);
            return aclBindings.Select(aclBinding => aclBinding.ToFilter()).ToList();
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
            List<AclBinding> aclBindings;
            try
            {
                aclBindings = ParseAclBindings(commandArgs, true);
            }
            catch
            {
                Console.WriteLine("usage: .. <bootstrapServers> create-acls <resource_type1> <resource_name1> <resource_patter_type1> " +
                    "<principal1> <host1> <operation1> <permission_type1> ..");
                Environment.ExitCode = 1;
                return;
            }

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    await adminClient.CreateAclsAsync(aclBindings);
                    Console.WriteLine("All create ACL operations completed successfully");
                }
                catch (CreateAclsException e)
                {
                    Console.WriteLine("One or more create ACL operations failed.");
                    for (int i = 0; i < e.Results.Count; ++i)
                    {
                        var result = e.Results[i];
                        if (!result.Error.IsError)
                        {
                            Console.WriteLine($"Create ACLs operation {i} completed successfully");
                        }
                        else
                        {
                            Console.WriteLine($"An error occurred in create ACL operation {i}: Code: {result.Error.Code}" +
                            $", Reason: {result.Error.Reason}");
                        }
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
            List<AclBindingFilter> aclBindingFilters;
            try
            {
                aclBindingFilters = ParseAclBindingFilters(commandArgs, false);
            }
            catch
            {
                Console.WriteLine("usage: .. <bootstrapServers> describe-acls <resource_type> <resource_name> <resource_patter_type> " +
                    "<principal> <host> <operation> <permission_type>");
                Environment.ExitCode = 1;
                return;
            }

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var result = await adminClient.DescribeAclsAsync(aclBindingFilters[0]);
                    Console.WriteLine("Matching ACLs:");
                    PrintAclBindings(result.AclBindings);
                }
                catch (DescribeAclsException e)
                {
                    Console.WriteLine($"An error occurred in describe ACLs operation: Code: {e.Result.Error.Code}" +
                        $", Reason: {e.Result.Error.Reason}");
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"An error occurred calling the describe ACLs operation: {e.Message}");
                }
            }
        }

        static async Task DeleteAclsAsync(string bootstrapServers, string[] commandArgs)
        {
            List<AclBindingFilter> aclBindingFilters;
            try
            {
                aclBindingFilters = ParseAclBindingFilters(commandArgs, true);
            }
            catch
            {
                Console.WriteLine("usage: .. <bootstrapServers> delete-acls <resource_type1> <resource_name1> <resource_patter_type1> " +
                    "<principal1> <host1> <operation1> <permission_type1> ..");
                Environment.ExitCode = 1;
                return;
            }

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var results = await adminClient.DeleteAclsAsync(aclBindingFilters);
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
                    Console.WriteLine("One or more create ACL operations failed.");
                    for (int i = 0; i < e.Results.Count; ++i)
                    {
                        var result = e.Results[i];
                        if (!result.Error.IsError)
                        {
                            Console.WriteLine($"Deleted ACLs in operation {i}");
                            PrintAclBindings(result.AclBindings);
                        }
                        else
                        {
                            Console.WriteLine($"An error occurred in delete ACL operation {i}: Code: {result.Error.Code}" +
                                $", Reason: {result.Error.Reason}");
                        }
                    }
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"An error occurred calling the DeleteAcls operation: {e.Message}");
                }
            }
        }

        static async Task AlterConsumerGroupOffsetsAsync(string bootstrapServers, string[] commandArgs)
        {
            if (commandArgs.Length < 4)
            {
                Console.WriteLine("usage: .. <bootstrapServers> alter-consumer-group-offsets <group_id> <topic1> <partition1> <offset1> ... <topicN> <partitionN> <offsetN>");
                Environment.ExitCode = 1;
                return;
            }

            var group = commandArgs[0];
            var tpoes = new List<TopicPartitionOffset>();
            for (int i = 1; i + 2 < commandArgs.Length; i += 3) {
                try
                {
                    var topic = commandArgs[i];
                    var partition = Int32.Parse(commandArgs[i + 1]);
                    var offset = Int64.Parse(commandArgs[i + 2]);
                    tpoes.Add(new TopicPartitionOffset(topic, partition, offset));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"An error occurred while parsing arguments: {e}");
                    Environment.ExitCode = 1;
                    return;
                }
            }

            var input = new List<ConsumerGroupTopicPartitionOffsets>() { new ConsumerGroupTopicPartitionOffsets(group, tpoes) };

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var results = await adminClient.AlterConsumerGroupOffsetsAsync(input);
                    Console.WriteLine("Successfully altered offsets:");
                    foreach(var groupResult in results) {
                        Console.WriteLine(groupResult);
                    }

                }
                catch (AlterConsumerGroupOffsetsException e)
                {
                    Console.WriteLine($"An error occurred altering offsets: {(e.Results.Any() ? e.Results[0] : null)}");
                    Environment.ExitCode = 1;
                    return;
                }
                catch (KafkaException e)
                {
                    Console.WriteLine("An error occurred altering consumer group offsets." +
                        $" Code: {e.Error.Code}" +
                        $", Reason: {e.Error.Reason}");
                    Environment.ExitCode = 1;
                    return;
                }
            }
        }

        static async Task ListConsumerGroupOffsetsAsync(string bootstrapServers, string[] commandArgs)
        {
            if (commandArgs.Length < 1)
            {
                Console.WriteLine("usage: .. <bootstrapServers> list-consumer-group-offsets <group_id> [<topic1> <partition1> ... <topicN> <partitionN>]");
                Environment.ExitCode = 1;
                return;
            }

            var group = commandArgs[0];
            var tpes = new List<TopicPartition>();
            for (int i = 1; i + 1 < commandArgs.Length; i += 2) {
                try
                {
                    var topic = commandArgs[i];
                    var partition = Int32.Parse(commandArgs[i + 1]);
                    tpes.Add(new TopicPartition(topic, partition));
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"An error occurred while parsing arguments: {e}");
                    Environment.ExitCode = 1;
                    return;
                }
            }
            if(!tpes.Any())
            {
                // In case the list is empty, request offsets for all the partitions.
                tpes = null;
            }

            var input = new List<ConsumerGroupTopicPartitions>() { new ConsumerGroupTopicPartitions(group, tpes) };

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var result = await adminClient.ListConsumerGroupOffsetsAsync(input);
                    Console.WriteLine("Successfully listed offsets:");
                    foreach(var groupResult in result) {
                        Console.WriteLine(groupResult);
                    }
                }
                catch (ListConsumerGroupOffsetsException e)
                {
                    Console.WriteLine($"An error occurred listing offsets: {(e.Results.Any() ? e.Results[0] : null)}");
                    Environment.ExitCode = 1;
                    return;
                }
                catch (KafkaException e)
                {
                    Console.WriteLine("An error occurred listing consumer group offsets." +
                        $" Code: {e.Error.Code}" +
                        $", Reason: {e.Error.Reason}");
                    Environment.ExitCode = 1;
                    return;
                }
            }
        }

        static async Task ListConsumerGroupsAsync(string bootstrapServers, string[] commandArgs) {
            var timeout = TimeSpan.FromSeconds(30);
            var statesList = new List<ConsumerGroupState>();
            try
            {
                if (commandArgs.Length > 0)
                {
                    timeout = TimeSpan.FromSeconds(Int32.Parse(commandArgs[0]));
                }
                if (commandArgs.Length > 1)
                {
                    for (int i = 1; i < commandArgs.Length; i++) {
                        statesList.Add(Enum.Parse<ConsumerGroupState>(commandArgs[i]));
                    }
                }
            }
            catch (SystemException)
            {
                Console.WriteLine("usage: .. <bootstrapServers> list-consumer-groups [<timeout_seconds> <match_state_1> <match_state_2> ... <match_state_N>]");
                Environment.ExitCode = 1;
                return;
            }

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var result = await adminClient.ListConsumerGroupsAsync(new ListConsumerGroupsOptions() { 
                        RequestTimeout = timeout,
                        MatchStates = statesList,
                    });
                    Console.WriteLine(result);
                }
                catch (KafkaException e)
                {
                    Console.WriteLine("An error occurred listing consumer groups." +
                        $" Code: {e.Error.Code}" +
                        $", Reason: {e.Error.Reason}");
                    Environment.ExitCode = 1;
                    return;
                }

            }
        }


        static async Task DescribeConsumerGroupsAsync(string bootstrapServers, string[] commandArgs) {
            if (commandArgs.Length < 1)
            {
                Console.WriteLine("usage: .. <bootstrapServers> describe-consumer-groups <group1> [<group2 ... <groupN>]");
                Environment.ExitCode = 1;
                return;
            }

            var groupNames = commandArgs.ToList();
            var timeout = TimeSpan.FromSeconds(30);
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                try
                {
                    var descResult = await adminClient.DescribeConsumerGroupsAsync(groupNames, new DescribeConsumerGroupsOptions() { RequestTimeout = timeout });
                    foreach (var group in descResult.ConsumerGroupDescriptions)
                    {
                        Console.WriteLine($"  Group: {group.GroupId} {group.Error}");
                        Console.WriteLine($"  Broker: {group.Coordinator}");
                        Console.WriteLine($"  IsSimpleConsumerGroup: {group.IsSimpleConsumerGroup}");
                        Console.WriteLine($"  PartitionAssignor: {group.PartitionAssignor}");
                        Console.WriteLine($"  State: {group.State}");
                        Console.WriteLine($"  Members:");
                        foreach (var m in group.Members)
                        {
                            Console.WriteLine($"    {m.ClientId} {m.ConsumerId} {m.Host}");
                            Console.WriteLine($"    Assignment:");
                            var topicPartitions = "";
                            if (m.Assignment.TopicPartitions != null)
                            {
                                topicPartitions = String.Join(", ", m.Assignment.TopicPartitions.Select(tp => tp.ToString()));
                            }
                            Console.WriteLine($"      TopicPartitions: [{topicPartitions}]");
                        }
                    }
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"An error occurred describing consumer groups: {e}");
                    Environment.ExitCode = 1;
                }
            }
        }

        public static async Task Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine(
                    "usage: .. <bootstrapServers> " + String.Join("|", new string[] {
                        "list-groups", "metadata", "library-version", "create-topic", "create-acls",
                        "describe-acls", "delete-acls",
                        "list-consumer-groups", "describe-consumer-groups",
                        "list-consumer-group-offsets", "alter-consumer-group-offsets"
                    }) +
                    " ..");
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
                case "alter-consumer-group-offsets":
                    await AlterConsumerGroupOffsetsAsync(bootstrapServers, commandArgs);
                    break;
                case "list-consumer-group-offsets":
                    await ListConsumerGroupOffsetsAsync(bootstrapServers, commandArgs);
                    break;
                case "list-consumer-groups":
                    await ListConsumerGroupsAsync(bootstrapServers, commandArgs);
                    break;
                case "describe-consumer-groups":
                    await DescribeConsumerGroupsAsync(bootstrapServers, commandArgs);
                    break;
                default:
                    Console.WriteLine($"unknown command: {command}");
                    break;
            }
        }
    }
}
