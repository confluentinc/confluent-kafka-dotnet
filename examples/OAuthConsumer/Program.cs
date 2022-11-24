using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Confluent.Kafka.Examples.OAuthConsumer
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
        private const String OauthConfigRegexPattern = "^(\\s*(\\w+)\\s*=\\s*(\\w+))+\\s*$";

        private const String OauthconfigKeyValueRegexPattern = "(\\w+)\\s*=\\s*(\\w+)";

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
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false,
            };

            void OauthCallback(IClient client, string cfg)
            {
                var token = retrieveUnsecuredToken(cfg);
                client.OAuthBearerSetToken(token.TokenValue, token.Expiration, token.Principal);
            }


            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                                    .SetOAuthBearerTokenRefreshHandler(OauthCallback).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Consumer {consumer.Name} consuming on topic {topicName}.");
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
            foreach (Match match in Regex.Matches(oauthConfig, OauthconfigKeyValueRegexPattern))
            {
                parsedConfig[match.Groups[1].ToString()] = match.Groups[2].ToString();
            }

            if (!parsedConfig.ContainsKey(PrincipalKey) || !parsedConfig.ContainsKey(ScopeKey) || parsedConfig.Count > 2)
            {
                Console.WriteLine($"Invalid OAuth config {oauthConfig} passed. Ignoring the token refresh");
                return new OAuthBearerToken{};
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
