// Copyright 2026 Confluent Inc.
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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>
///     An example demonstrating how to produce a message to a topic, and then read
///     it back again using a consumer. Authentication uses AWS IAM via the OAUTHBEARER
///     SASL mechanism, autowired by the optional package
///     <c>Confluent.Kafka.OAuthBearer.Aws</c>.
///
///     Activation is config-only — no callback, no manual JWT minting, no AWS SDK
///     code at the integration site. The optional package handles
///     <c>sts:GetWebIdentityToken</c> under the hood.
/// </summary>
namespace Confluent.Kafka.Examples.OAuthBearerAws
{
    public class Program
    {
        // Edit these placeholders for your environment before running.
        private const string awsRegion           = "your-aws-region";        // e.g. us-east-1
        private const string oidcAudience        = "your-oidc-audience";     // e.g. https://confluent.cloud/oidc
        private const string kafkaLogicalCluster = "your-logical-cluster";   // e.g. lkc-abc
        private const string identityPoolId      = "your-identity-pool-id";  // e.g. pool-xyz

        public static async Task Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Usage: .. brokerList");
                return;
            }
            var bootstrapServers = args[0];
            var topicName = Guid.NewGuid().ToString();
            var groupId   = Guid.NewGuid().ToString();

            // The two AWS-autowire activation keys, plus extensions embedded inside
            // SaslOauthbearerConfig. Do NOT set SaslOauthbearerMethod=Oidc — the AWS
            // path uses the default method.
            var commonConfig = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism    = SaslMechanism.OAuthBearer,
                SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam,
                SaslOauthbearerConfig =
                    $"region={awsRegion} " +
                    $"audience={oidcAudience} " +
                    $"extension_logicalCluster={kafkaLogicalCluster} " +
                    $"extension_identityPoolId={identityPoolId}",
            };

            var consumerConfig = new ConsumerConfig(commonConfig)
            {
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
            };

            try
            {
                createTopic(commonConfig, topicName);
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"An error occurred creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                Environment.Exit(1);
            }

            using (var producer = new ProducerBuilder<Null, string>(commonConfig).Build())
            using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topicName);

                var cancelled = false;
                CancellationTokenSource cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cancelled = true;
                    cts.Cancel();
                };

                try
                {
                    while (!cancelled)
                    {
                        var msg = "User";

                        try
                        {
                            var deliveryReport = await producer.ProduceAsync(topicName, new Message<Null, string> { Value = msg });
                            Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}, {msg}");
                        }
                        catch (ProduceException<Null, string> e)
                        {
                            Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                        }

                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);
                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                            try
                            {
                                consumer.StoreOffset(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Store Offset error: {e.Error.Reason}");
                            }
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }

        private static void createTopic(ClientConfig config, string topicName)
        {
            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                adminClient.CreateTopicsAsync(new TopicSpecification[] {
                    new TopicSpecification { Name = topicName, ReplicationFactor = 3, NumPartitions = 1 }
                }).Wait();
            }
        }
    }
}
