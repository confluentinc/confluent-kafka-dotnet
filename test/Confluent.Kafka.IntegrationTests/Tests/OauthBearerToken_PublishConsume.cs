using System;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        [Theory, MemberData(nameof(OAuthBearerKafkaParameters))]
        public void OAuthBearerToken_PublishConsume(string bootstrapServers)
        {
            LogToFileStartTest();
            
            const string principal = "IntegrationTests";
            var issuedAt = DateTimeOffset.UtcNow;
            var expiresAt = issuedAt.AddMinutes(5);
            var token = Util.GetUnsecuredJwt(principal, issuedAt, expiresAt);

            void Callback(IClient client, string cfg)
            {
                client.OAuthBearerSetToken(token, expiresAt.ToUnixTimeMilliseconds(), principal);
            }

            var message = new Message<string, string>
            {
                Key = $"{Guid.NewGuid()}",
                Value = $"{DateTimeOffset.UtcNow:T}"
            };

            var config = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer
            };
            var producerConfig = new ProducerConfig(config);
            var consumerConfig = new ConsumerConfig(config)
            {
                GroupId = $"{Guid.NewGuid()}",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var producer = new ProducerBuilder<string, string>(producerConfig)
                .SetOAuthBearerTokenRefreshHandler(Callback)
                .Build();
            var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetOAuthBearerTokenRefreshHandler(Callback)
                .Build();

            producer.Produce(partitionedTopic, message);
            producer.Flush(TimeSpan.FromSeconds(5));
            consumer.Subscribe(partitionedTopic);
            var received = consumer.Consume(TimeSpan.FromSeconds(5));
            
            Assert.NotNull(received);
            consumer.Commit(received);
            
            Assert.Equal(message.Key, received.Message.Key);
            Assert.Equal(message.Value, received.Message.Value);

            LogToFileEndTest();
        }

        [Theory, MemberData(nameof(OAuthBearerKafkaParameters))]
        public void OAuthBearerToken_PublishConsume_SetFailure(string bootstrapServers)
        {
            LogToFileStartTest();

            var errorMessage = $"{Guid.NewGuid()}";
            void TokenCallback(IClient client, string cfg)
            {
                client.OAuthBearerSetTokenFailure(errorMessage);
            }

            var message = new Message<string, string>
            {
                Key = $"{Guid.NewGuid()}",
                Value = $"{Guid.NewGuid()}"
            };

            var config = new ClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.OAuthBearer
            };

            // test Producer
            var producerConfig = new ProducerConfig(config);
            Error producerError = null;
            var producer = new ProducerBuilder<string, string>(producerConfig)
                .SetOAuthBearerTokenRefreshHandler(TokenCallback)
                .SetErrorHandler((p, e) => producerError = e)
                .Build();
            producer.Produce(singlePartitionTopic, message);
            producer.Flush(TimeSpan.Zero);
            AssertError(producerError);

            // test Consumer
            var consumerConfig = new ConsumerConfig(config)
            {
                GroupId = $"{Guid.NewGuid()}"
            };
            Error consumerError = null;
            var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetOAuthBearerTokenRefreshHandler(TokenCallback)
                .SetErrorHandler((c, e) => consumerError = e)
                .Build();
            consumer.Subscribe(singlePartitionTopic);
            consumer.Consume(TimeSpan.Zero);
            AssertError(consumerError);

            LogToFileEndTest();

            void AssertError(Error error)
            {
                Assert.NotNull(error);
                Assert.True(error.IsError);
                Assert.True(error.IsLocalError);
                Assert.False(error.IsBrokerError);
                Assert.False(error.IsFatal);
                Assert.Equal(ErrorCode.Local_Authentication, error.Code);
                Assert.EndsWith(errorMessage, error.Reason);
            }
        }
    }
}