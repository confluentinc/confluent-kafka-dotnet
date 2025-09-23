// Copyright 2022 Confluent Inc.
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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.SyncOverAsync;
using Newtonsoft.Json;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

/// <summary>
///     An example demonstrating how to produce a message to 
///     a topic, and then reading it back again using a consumer.
///     The authentication uses the OpenID Connect method of the OAUTHBEARER SASL mechanism.
///     The token is acquired from the Azure Instance Metadata Service (IMDS)
///     using metadata based secret-less authentication.
/// </summary>
namespace Confluent.Kafka.Examples.OAuthOIDCAzureIMDS
{

    class User
    {
        [JsonRequired] // use Newtonsoft.Json annotations
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonRequired]
        [JsonProperty("favorite_color")]
        public string FavoriteColor { get; set; }

        [JsonProperty("favorite_number")]
        public long FavoriteNumber { get; set; }
    }

    public class Program
    {
        private const string azureIMDSQueryParams = "api-version=&resource=&client_id=";

        public static async Task Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. brokerList schemaRegistryUrl");
                return;
            }
            var bootstrapServers = args[1];
            var schemaRegistryUrl = args[2];
            var topicName = Guid.NewGuid().ToString();
            var groupId = Guid.NewGuid().ToString();

            var commonConfig = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
                SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AzureIMDS,
                SaslOauthbearerConfig = $"query={azureIMDSQueryParams}",
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = schemaRegistryUrl,
                BearerAuthCredentialsSource = BearerAuthCredentialsSource.OAuthBearerAzureIMDS,
                BearerAuthTokenEndpointQuery = azureIMDSQueryParams,
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

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = new ProducerBuilder<Null, User>(commonConfig)
                .SetValueSerializer(new JsonSerializer<User>(schemaRegistry))
                .Build())
            using (var consumer = new ConsumerBuilder<Ignore, User>(consumerConfig)
                .SetValueDeserializer(new JsonDeserializer<User>(schemaRegistry).AsSyncOverAsync()).Build())
            {
                consumer.Subscribe(topicName);

                var cancelled = false;
                CancellationTokenSource cts = new CancellationTokenSource();

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                    cts.Cancel();
                };

                try
                {
                    while (!cancelled)
                    {
                        var msg = new User
                        {
                            Name = "user-" + Guid.NewGuid().ToString(),
                            FavoriteColor = "blue",
                            FavoriteNumber = 7
                        };

                        try
                        {
                            var deliveryReport = await producer.ProduceAsync(topicName, new Message<Null, User> { Value = msg });
                            Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}, {msg}");
                        }
                        catch (ProduceException<Null, User> e)
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

        private static void createTopic(ClientConfig config, String topicName)
        {
            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                adminClient.CreateTopicsAsync(new TopicSpecification[] {
                            new TopicSpecification { Name = topicName, ReplicationFactor = 3, NumPartitions = 1 } }).Wait(); ;
            }
        }
    }

}
