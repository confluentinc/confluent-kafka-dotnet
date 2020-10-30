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

            if (string.IsNullOrEmpty(bootstrapServers))
            {
                // skip test if oauth enabled broker is not specified.
                return;
            }

            const string principal = "Tester";
            var issuedAt = DateTimeOffset.UtcNow;
            var expiresAt = issuedAt.AddMinutes(5);
            var token = Util.GetUnsecuredJwt(principal, "requiredScope", issuedAt, expiresAt);

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

            consumer.Subscribe(partitionedTopic);
            producer.Produce(partitionedTopic, message);
            producer.Flush(TimeSpan.FromSeconds(30));
            var received = consumer.Consume(TimeSpan.FromSeconds(30));

            Assert.NotNull(received);
            consumer.Commit(received);

            Assert.Equal(message.Key, received.Message.Key);
            Assert.Equal(message.Value, received.Message.Value);

            LogToFileEndTest();
        }

        [Theory, MemberData(nameof(OAuthBearerKafkaParameters))]
        public void OAuthBearerToken_PublishConsume_InvalidScope(string bootstrapServers)
        {
            LogToFileStartTest();

            if (string.IsNullOrEmpty(bootstrapServers))
            {
                // skip test if oauth enabled broker is not specified.
                return;
            }

            const string principal = "Tester";
            var issuedAt = DateTimeOffset.UtcNow;
            var expiresAt = issuedAt.AddMinutes(5);
            var token = Util.GetUnsecuredJwt(principal, "invalidScope", issuedAt, expiresAt);

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

            Error producerError = null;
            var producer = new ProducerBuilder<string, string>(producerConfig)
                .SetOAuthBearerTokenRefreshHandler(Callback)
                .SetErrorHandler((p, e) => producerError = e)
                .Build();
            Error consumerError = null;
            var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetOAuthBearerTokenRefreshHandler(Callback)
                .SetErrorHandler((c, e) => consumerError = e)
                .Build();

            consumer.Subscribe(partitionedTopic);
            producer.Produce(partitionedTopic, message);
            producer.Flush(TimeSpan.FromSeconds(5));
            consumer.Consume(TimeSpan.FromSeconds(5));

            AssertError(producerError);
            AssertError(consumerError);

            LogToFileEndTest();

            void AssertError(Error error)
            {
                Assert.NotNull(error);
                Assert.True(error.IsError);
                Assert.True(error.IsLocalError);
                Assert.False(error.IsBrokerError);
                Assert.False(error.IsFatal);
                Assert.Equal(ErrorCode.Local_AllBrokersDown, error.Code);
                Assert.EndsWith("brokers are down", error.Reason);
            }
        }


        [Theory, MemberData(nameof(OAuthBearerKafkaParameters))]
        public void OAuthBearerToken_PublishConsume_SetFailure(string bootstrapServers)
        {
            LogToFileStartTest();

            if (string.IsNullOrEmpty(bootstrapServers))
            {
                // skip test if oauth enabled broker is not specified.
                return;
            }

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