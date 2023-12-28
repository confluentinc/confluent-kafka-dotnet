// Copyright 2016-2023 Confluent Inc.
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

using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using Newtonsoft.Json.Linq;
using Xunit;


namespace Confluent.Kafka.IntegrationTests
{
    public class GlobalFixture : IDisposable
    {
        private string bootstrapServers;

        public const int partitionedTopicNumPartitions = 2;

        public GlobalFixture()
        {
            var assemblyPath = typeof(Tests).GetTypeInfo().Assembly.Location;
            var assemblyDirectory = Path.GetDirectoryName(assemblyPath);
            var jsonPath = Path.Combine(assemblyDirectory, "testconf.json");
            var json = JObject.Parse(File.ReadAllText(jsonPath));
            bootstrapServers = json["bootstrapServers"].ToString();

            SinglePartitionTopic = "dotnet_test_" + Guid.NewGuid().ToString();
            PartitionedTopic = "dotnet_test_" + Guid.NewGuid().ToString();

            // Create shared topics that are used by many of the tests.
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                adminClient.CreateTopicsAsync(new List<TopicSpecification> {
                    new TopicSpecification { Name = SinglePartitionTopic, NumPartitions = 1, ReplicationFactor = 1 },
                    new TopicSpecification { Name = PartitionedTopic, NumPartitions = partitionedTopicNumPartitions, ReplicationFactor = 1 }
                }).Wait();
            }
        }

        public void Dispose()
        {
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
            {
                adminClient.DeleteTopicsAsync(new List<string> { SinglePartitionTopic, PartitionedTopic }).Wait();
            }
        }

        public string SinglePartitionTopic { get; set; }

        public string PartitionedTopic { get; set; }
    }

    [CollectionDefinition("Global collection")]
    public class GlobalCollection : ICollectionFixture<GlobalFixture>
    {
        // This class has no code, and is never created. Its purpose is
        // simply to be the place to apply [CollectionDefinition] and all
        // the ICollectionFixture<> interfaces.
    }


    [Collection("Global collection")]
    public partial class Tests
    {
        private string singlePartitionTopic;
        private string partitionedTopic;

        public const int partitionedTopicNumPartitions = GlobalFixture.partitionedTopicNumPartitions;

        private static List<object[]> kafkaParameters;
        private static List<object[]> saslPlainKafkaParameters;
        private static List<object[]> oAuthBearerKafkaParameters;

        private object logLockObj = new object();
        private void LogToFile(string msg)
        {
            lock (logLockObj)
            {
                // Uncomment to enable logging to a file. Useful for debugging,
                // for example, which test caused librdkafka to segfault.
                // File.AppendAllLines("/tmp/test.txt", new [] { msg });
            }
        }

        private void LogToFileStartTest([CallerMemberName] string callerMemberName = null)
            => LogToFile($"start {callerMemberName}");

        private void LogToFileEndTest([CallerMemberName] string callerMemberName = null)
            => LogToFile($"end   {callerMemberName}");

        public Tests(GlobalFixture globalFixture)
        {
            // Quick fix for https://github.com/Microsoft/vstest/issues/918
            // Some tests will log using ConsoleLogger which print to standard Err by default, bugged on vstest
            // If we have error in test, they may hang
            // Write to standard output solve the issue
            Console.SetError(Console.Out);

            singlePartitionTopic = globalFixture.SinglePartitionTopic;
            partitionedTopic = globalFixture.PartitionedTopic;
        }

        public static IEnumerable<object[]> KafkaParameters()
        {
            if (kafkaParameters == null)
            {
                var assemblyPath = typeof(Tests).GetTypeInfo().Assembly.Location;
                var assemblyDirectory = Path.GetDirectoryName(assemblyPath);
                var jsonPath = Path.Combine(assemblyDirectory, "testconf.json");
                var json = JObject.Parse(File.ReadAllText(jsonPath));
                kafkaParameters = new List<object[]>
                {
                    new object[] {json["bootstrapServers"].ToString()}
                };
            }
            return kafkaParameters;
        }

        public static IEnumerable<object[]> SaslPlainKafkaParameters()
        {
            if (saslPlainKafkaParameters == null)
            {
                var assemblyPath = typeof(Tests).GetTypeInfo().Assembly.Location;
                var assemblyDirectory = Path.GetDirectoryName(assemblyPath);
                var jsonPath = Path.Combine(assemblyDirectory, "testconf.json");
                var json = JObject.Parse(File.ReadAllText(jsonPath));
                var saslPlain = json["saslPlain"];
                var users = saslPlain["users"];
                var admin = users["admin"];
                var user = users["user"];
                saslPlainKafkaParameters = new List<object[]>
                {
                    new object[]
                    {
                        saslPlain["bootstrapServers"].ToString(),
                        admin["username"].ToString(),
                        admin["password"].ToString(),
                        user["username"].ToString(),
                        user["password"].ToString(),
                    }
                };
            }
            return saslPlainKafkaParameters;
        }

        public static IEnumerable<object[]> OAuthBearerKafkaParameters()
        {
            if (oAuthBearerKafkaParameters == null)
            {
                var assemblyPath = typeof(Tests).GetTypeInfo().Assembly.Location;
                var assemblyDirectory = Path.GetDirectoryName(assemblyPath);
                var jsonPath = Path.Combine(assemblyDirectory, "testconf.json");
                var json = JObject.Parse(File.ReadAllText(jsonPath));
                oAuthBearerKafkaParameters = new List<object[]>
                {
                    new object[] {json["oauthbearerBootstrapServers"].ToString()}
                };
            }
            return oAuthBearerKafkaParameters;
        }
        public static bool semaphoreSkipFlakyTests(){
            string onSemaphore = Environment.GetEnvironmentVariable("SEMAPHORE_SKIP_FLAKY_TETSTS");
            if (onSemaphore != null)
            {
                return true;
            }
            return false;
        }
    }
}
