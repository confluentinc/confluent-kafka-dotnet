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
using Newtonsoft.Json;

/// <summary>
///     An example demonstrating how to produce a message to 
///     a topic, and then reading it back again using a consumer.
///     The authentication uses the OpenID Connect method of the OAUTHBEARER SASL mechanism.
/// </summary>
namespace Confluent.Kafka.Examples.OAuthOIDC
{
    class OAuthBearerToken
    {
        public string TokenValue { get; set; }
        public long Expiration { get; set; }
        public String Principal { get; set; }
        public Dictionary<String, String> Extensions { get; set; }
    }

    public class Program
    {
        private const String OAuthBearerClientId = "<oauthbearer_client_id>";
        private const String OAuthBearerClientSecret = "<oauthbearer_client_secret>";
        private const String OAuthBearerTokenEndpointURL = "<token_endpoint_url>";
        private const String OAuthBearerScope = "<scope>";

        private const String OauthConfigRegexPattern = "^(\\s*(\\w+)\\s*=\\s*(\\w+))+\\s*$"; // 1 or more name=value pairs with optional ignored whitespace
        private const String OauthConfigKeyValueRegexPattern = "(\\w+)\\s*=\\s*(\\w+)"; // Extract key=value pairs from OAuth Config
        private const String PrincipalClaimNameKey = "principalClaimName";
        private const String PrincipalKey = "principal";
        private const String ScopeKey = "scope";

        public static async Task Main(string[] args)
        {
            args = new string[] { "testValue", "testBootstrapServersValue", "testOAuthConfiguration" };
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: .. brokerList");
                return;
            }
            var bootstrapServers = args[1];
            string oauthConf = args[2];
            var topicName = Guid.NewGuid().ToString();
            var groupId = Guid.NewGuid().ToString();

            if (!Regex.IsMatch(oauthConf, OauthConfigRegexPattern))
            {
                Console.WriteLine("Invalid OAuth config passed.");
                Environment.Exit(1);
            }

            var commonConfig = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
                SaslOauthbearerClientId = OAuthBearerClientId,
                SaslOauthbearerClientSecret = OAuthBearerClientSecret,
                SaslOauthbearerTokenEndpointUrl = OAuthBearerTokenEndpointURL,
                SaslOauthbearerScope = OAuthBearerScope,
                SaslOauthbearerConfig = oauthConf
            };

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
                SaslOauthbearerClientId = OAuthBearerClientId,
                SaslOauthbearerClientSecret = OAuthBearerClientSecret,
                SaslOauthbearerTokenEndpointUrl = OAuthBearerTokenEndpointURL,
                SaslOauthbearerScope = OAuthBearerScope,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false
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

            // Callback to handle OAuth bearer token refresh. It creates an unsecured JWT based on the configuration defined
            // in OAuth Config and sets the token on the client for use in any future authentication attempt.
            // It must be invoked whenever the client requires a token (i.e. when it first starts and when the
            // previously-received token is 80% of the way to its expiration time).
            void OauthCallback(IClient client, string cfg)
            {
                try
                {
                    var token = retrieveUnsecuredToken(cfg);
                    client.OAuthBearerSetToken(token.TokenValue, token.Expiration, token.Principal);
                }
                catch (Exception e)
                {
                    client.OAuthBearerSetTokenFailure(e.ToString());
                }
            }

            using (var producer = new ProducerBuilder<String, String>(commonConfig)
                .SetOAuthBearerTokenRefreshHandler(OauthCallback)
                .Build())
            using (var consumer = new ConsumerBuilder<String, String>(consumerConfig).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
                Console.WriteLine("-----------------------------------------------------------------------");

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
                        var msg = Guid.NewGuid().ToString();
                        try
                        {
                            var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, string> { Value = msg });
                            Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}, {msg}");
                        }
                        catch (ProduceException<string, string> e)
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


        private static string ToUnpaddedBase64(string s)
            => Convert.ToBase64String(Encoding.UTF8.GetBytes(s)).TrimEnd('=');

        private static OAuthBearerToken retrieveUnsecuredToken(String oauthConfig)
        {
            Console.WriteLine("Refreshing the token");

            var parsedConfig = new Dictionary<String, String>();
            foreach (Match match in Regex.Matches(oauthConfig, OauthConfigKeyValueRegexPattern))
            {
                parsedConfig[match.Groups[1].ToString()] = match.Groups[2].ToString();
            }

            if (!parsedConfig.ContainsKey(PrincipalKey) || !parsedConfig.ContainsKey(ScopeKey) || parsedConfig.Count > 2)
            {
                throw new Exception($"Invalid OAuth config {oauthConfig} passed.");
            }

            var principalClaimName = parsedConfig.ContainsKey(PrincipalClaimNameKey) ? parsedConfig[PrincipalClaimNameKey] : "sub";
            var principal = parsedConfig[PrincipalKey];
            var scopeValue = parsedConfig[ScopeKey];

            var issuedAt = DateTimeOffset.UtcNow;
            var expiresAt = issuedAt.AddSeconds(15); // setting a low value to show the token refresh in action.

            var header = new
            {
                alg = "none",
                typ = "JWT"
            };

            var payload = new Dictionary<String, Object>
            {
                {principalClaimName, principal},
                {"iat", issuedAt.ToUnixTimeSeconds()},
                {"exp", expiresAt.ToUnixTimeSeconds()},
                {ScopeKey, scopeValue}
            };

            var headerJson = JsonConvert.SerializeObject(header);
            var payloadJson = JsonConvert.SerializeObject(payload);

            return new OAuthBearerToken
            {
                TokenValue = $"{ToUnpaddedBase64(headerJson)}.{ToUnpaddedBase64(payloadJson)}.",
                Expiration = expiresAt.ToUnixTimeMilliseconds(),
                Principal = principal,
                Extensions = new Dictionary<string, string>()
            };
        }

    }
}
