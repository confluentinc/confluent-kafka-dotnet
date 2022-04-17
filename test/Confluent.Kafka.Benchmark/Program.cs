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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Mono.Options;
using Confluent.Kafka.Admin;


namespace Confluent.Kafka.Benchmark
{
    public class Program
    {

        private static void CreateTopic(string bootstrapServers, string username, string password, string topicName, int partitionCount, short replicationFactor)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SaslUsername = username,
                SaslPassword = password,
                SecurityProtocol = username == null ? SecurityProtocol.Plaintext : SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                try
                {
                    adminClient.DeleteTopicsAsync(new List<string> { topicName }).Wait();
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is DeleteTopicsException) ||
                        (((DeleteTopicsException)ae.InnerException).Results.Select(r => r.Error.Code).Where(el => el != ErrorCode.UnknownTopicOrPart).Count() > 0))
                    {
                        throw new Exception($"Unable to delete topic {topicName}", ae);
                    }
                }

                // Give the cluster a chance to remove the topic. If this isn't long enough (unlikely), there will be an error and the user can just re-run.
                Thread.Sleep(2000);

                try
                {
                    adminClient.CreateTopicsAsync(new List<TopicSpecification> { new TopicSpecification { Name = topicName, NumPartitions = partitionCount, ReplicationFactor = replicationFactor } }).Wait();
                }
                catch (AggregateException e)
                {
                    Console.WriteLine("Failed to create topic: " + e.InnerException.Message);
                }
            }
        }


        public static void Main(string[] args)
        {
            bool showHelp = false;
            string mode = null;
            string bootstrapServers = "localhost:9092";
            string topicName = "dotnet-benchmark";
            string group = "benchmark-consumer-group";
            int headerCount = 0;
            int? messagesPerSecond = null;
            int numberOfMessages = 5000000;
            int messageSize = 100;
            int? partitionCount = null;
            short replicationFactor = 3;
            string username = null;
            string password = null;

            OptionSet p = new OptionSet
            {
                { "m|mode=", "throughput|latency", m => mode = m },
                { "b|brokers=", $"bootstrap.servers (default: {bootstrapServers})", v => bootstrapServers = v },
                { "t=", $"topic (default: {topicName})", t => topicName = t },
                { "g=", $"consumer group (default: {group})", g => group = g },
                { "h=", $"number of headers (default: {headerCount})", h => headerCount = int.Parse(h) },
                { "n=", $"number of messages (default: {numberOfMessages})", n => numberOfMessages = int.Parse(n) },
                { "r=", "rate - messages per second (latency mode only). must be > 1000", (int r) => messagesPerSecond = r },
                { "s=", $"message size (default: {messageSize})", s => messageSize = int.Parse(s) },
                { "p=", "(re)create topic with this partition count (default: not set)", v => partitionCount = int.Parse(v) },
                { "f=", $"replication factor when creating topic (default {replicationFactor})", f => replicationFactor = short.Parse(f) },
                { "u=", "SASL username (will also set protocol=SASL_SSL, mechanism=PLAIN)", u => username = u },
                { "w=", "SASL password", w => password = w },
                { "help", "show this message and exit", v => showHelp = v != null },
            };

            p.Parse(args);

            if (mode == null || showHelp ||
                (messagesPerSecond != null && mode == "throughput") ||
                (messagesPerSecond == null && mode == "latency"))
            {
                Console.WriteLine("Usage:");
                p.WriteOptionDescriptions(Console.Out);
                return;
            }

            if (partitionCount != null)
            {
                CreateTopic(bootstrapServers, username, password, topicName, partitionCount.Value, replicationFactor);
            }

            if (mode == "throughput")
            {
                const int NUMBER_OF_TESTS = 1;
                BenchmarkProducer.TaskProduce(bootstrapServers, topicName, numberOfMessages, messageSize, headerCount, NUMBER_OF_TESTS, username, password);
                var firstMessageOffset = BenchmarkProducer.DeliveryHandlerProduce(bootstrapServers, topicName, numberOfMessages, messageSize, headerCount, NUMBER_OF_TESTS, username, password);
                BenchmarkConsumer.Consume(bootstrapServers, topicName, group, firstMessageOffset, numberOfMessages, headerCount, NUMBER_OF_TESTS, username, password);
            }
            else if (mode == "latency")
            {
                Latency.Run(bootstrapServers, topicName, group, headerCount, messageSize, messagesPerSecond.Value, numberOfMessages, username, password);
            }
        }
    }
}
