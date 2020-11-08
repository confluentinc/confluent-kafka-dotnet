using System;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        /// <summary>
        ///     Tests that token refresh callback is called when <see cref="ClientConfig.SaslMechanism"/> is set to <see cref="SaslMechanism.OAuthBearer"/>.
        /// </summary>
        [Theory, MemberData(nameof(OAuthBearerKafkaParameters))]
        public void OAuthBearerToken_Delegate(string bootstrapServers)
        {
            LogToFileStartTest();

            if (string.IsNullOrEmpty(bootstrapServers))
            {
                // skip test if oauth enabled broker is not specified.
                return;
            }

            var config = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerConfig = $"{Guid.NewGuid()}"
            };

            // test Consumer
            var consumerConfig = new ConsumerConfig(config)
            {
                GroupId = $"{Guid.NewGuid()}"
            };
            var consumerCallsCount = 0;
            var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetOAuthBearerTokenRefreshHandler((client, cfg) =>
                {
                    Assert.Equal(config.SaslOauthbearerConfig, cfg);
                    consumerCallsCount++;
                })
                .Build();
            consumer.Subscribe(singlePartitionTopic);
            consumer.Consume(0);
            Assert.True(consumerCallsCount > 0);

            // test Producer
            var producerConfig = new ProducerConfig(config);
            var producerCallsCount = 0;
            var producer = new ProducerBuilder<string, string>(producerConfig)
                .SetOAuthBearerTokenRefreshHandler((client, cfg) =>
                {
                    Assert.Equal(config.SaslOauthbearerConfig, cfg);
                    producerCallsCount++;
                })
                .Build();
            producer.Flush();
            Assert.True(producerCallsCount > 0);

            LogToFileEndTest();
        }
    }
}