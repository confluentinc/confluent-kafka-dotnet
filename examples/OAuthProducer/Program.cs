using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Confluent.Kafka.Examples.OAuthProducer
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
            if (args.Length != 4)
            {
                Console.WriteLine("Usage: .. brokerList topic \"principal=<value> scope=<scope>\"");
                return;
            }
            string bootstrapServers = args[1];
            string topicName = args[2];
            string oauthConf = args[3];

            if (!Regex.IsMatch(oauthConf, OauthConfigRegexPattern))
            {
                Console.WriteLine("Invalid OAuth config passed.");
                Environment.Exit(1);
            }

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerConfig = oauthConf,
            };

            void OauthCallback(IClient client, string cfg)
            {
                var token = retrieveUnsecuredToken(cfg);
                client.OAuthBearerSetToken(token.TokenValue, token.Expiration, token.Principal);
            }


            using (var producer = new ProducerBuilder<string, string>(producerConfig)
                                     .SetOAuthBearerTokenRefreshHandler(OauthCallback).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {topicName}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("Ctrl-C to quit.\n");

                var cancelled = false;
                var msgCnt = 1;
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    var msg = String.Format("Producer example, message #{0}", msgCnt++);

                    try
                    {
                        var deliveryReport = await producer.ProduceAsync(topicName, new Message<string, string> { Value = msg });
                        Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}, {msg}");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                    Thread.Sleep(1000); // sleep one second
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
                return null;
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
                Extensions = new Dictionary<string, string>()
            };
        }
    }
}