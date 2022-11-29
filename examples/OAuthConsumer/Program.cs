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
using Newtonsoft.Json;

/// <summary>
///     An example showing consumer 
///     with a custom OAUTHBEARER token implementation.
/// </summary>
namespace Confluent.Kafka.Examples.OAuthConsumer
{
    /// <summary>
    ///     A class to store the token and related properties.
    /// </summary>
    class OAuthBearerToken
    {
        public string TokenValue { get; set; }
        public long Expiration { get; set; }
        public String Principal { get; set; }
        public Dictionary<String, String> Extensions { get; set; }
    }

    public class Program
    {
        private const String OauthConfigRegexPattern = "^(\\s*(\\w+)\\s*=\\s*(\\w+))+\\s*$"; // 1 or more name=value pairs with optional ignored whitespace
        private const String OauthConfigKeyValueRegexPattern = "(\\w+)\\s*=\\s*(\\w+)"; // Extract key=value pairs from OAuth Config
        private const String PrincipalClaimNameKey = "principalClaimName";
        private const String PrincipalKey = "principal";
        private const String ScopeKey = "scope";


        public static async Task Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.WriteLine("Usage: .. brokerList topic group \"principal=<value> scope=<scope>\"");
                return;
            }
            string bootstrapServers = args[1];
            string topicName = args[2];
            string groupId = args[3];
            string oauthConf = args[4];

            if (!Regex.IsMatch(oauthConf, OauthConfigRegexPattern))
            {
                Console.WriteLine($"Invalid OAuth config {oauthConf} passed.");
                Environment.Exit(1);
            }

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerConfig = oauthConf,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
            };

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


            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                                    .SetOAuthBearerTokenRefreshHandler(OauthCallback).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Consumer {consumer.Name} consuming from topic {topicName}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("Ctrl-C to quit.\n");

                consumer.Subscribe(topicName);
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
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
            var expiresAt = issuedAt.AddSeconds(5); // setting a low value to show the token refresh in action.

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
            };
        }
    }

}
