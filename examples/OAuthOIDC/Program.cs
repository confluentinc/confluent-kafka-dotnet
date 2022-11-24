using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;

namespace Confluent.Kafka.Examples.OAuthOIDC
{
    public class Program
    {
        private const String OAuthBearerClientId = "<oauthbearer_client_id>";
        private const String OAuthBearerClientSecret = "<oauthbearer_client_secret>";
        private const String OAuthBearerTokenEndpointURL = "<token_endpoint_url>";
        private const String OAuthBearerScope = "<scope>";
        public static async Task Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: .. brokerList");
                return;
            }
            var bootstrapServers = args[1];
            var topicName = Guid.NewGuid().ToString();
            var groupId = Guid.NewGuid().ToString();

            var commonConfig = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
                SaslOauthbearerClientId = OAuthBearerClientId,
                SaslOauthbearerClientSecret = OAuthBearerClientSecret,
                SaslOauthbearerTokenEndpointUrl = OAuthBearerTokenEndpointURL,
                SaslOauthbearerScope = OAuthBearerScope
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

            using (var producer = new ProducerBuilder<String, String>(commonConfig).Build())
            using (var consumer = new ConsumerBuilder<String, String>(consumerConfig).Build())
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
                            new TopicSpecification { Name = topicName, ReplicationFactor = 3, NumPartitions = 1 } }).Wait();;
            }
        }
    }

}
