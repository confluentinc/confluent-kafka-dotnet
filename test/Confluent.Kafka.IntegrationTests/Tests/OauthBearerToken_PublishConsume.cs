using System;
using System.Text;
using Newtonsoft.Json;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    public partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public void OauthBearerToken_PublishConsume(string bootstrapServers)
        {
            LogToFileStartTest();
            
            const string principal = "IntegrationTests";
            var issuedAt = DateTimeOffset.UtcNow;
            var expiresAt = issuedAt.AddSeconds(5);
            var token = GetUnsecuredToken(principal, issuedAt, expiresAt);

            void Callback(IClient client, string cfg)
            {
                client.OauthBearerSetToken(token, expiresAt.ToUnixTimeMilliseconds(), principal);
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
                SaslMechanism = SaslMechanism.OauthBearer,
                EnableSaslOauthbearerUnsecureJwt = true
            };
            var producerConfig = new ProducerConfig(config);
            var consumerConfig = new ConsumerConfig(config)
            {
                GroupId = $"{Guid.NewGuid()}",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var producer = new ProducerBuilder<string, string>(producerConfig)
                .SetOauthBearerTokenRefreshHandler(Callback)
                .Build();
            var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetOauthBearerTokenRefreshHandler(Callback)
                .Build();

            producer.Produce(partitionedTopic, message);
            producer.Flush(TimeSpan.FromSeconds(5));
            consumer.Subscribe(partitionedTopic);
            var received = consumer.Consume(TimeSpan.FromSeconds(5));
            consumer.Commit(received);
            
            Assert.NotNull(received);
            Assert.Equal(message.Key, received.Message.Key);
            Assert.Equal(message.Value, received.Message.Value);

            LogToFileEndTest();
        }

        [Theory, MemberData(nameof(KafkaParameters))]
        public void OauthBearerToken_PublishConsume_SetFailure(string bootstrapServers)
        {
            LogToFileStartTest();

            var errorMessage = $"{Guid.NewGuid()}";
            void TokenCallback(IClient client, string cfg)
            {
                client.OauthBearerSetTokenFailure(errorMessage);
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
                SaslMechanism = SaslMechanism.OauthBearer
            };

            // test Producer
            var producerConfig = new ProducerConfig(config);
            Error producerError = null;
            var producer = new ProducerBuilder<string, string>(producerConfig)
                .SetOauthBearerTokenRefreshHandler(TokenCallback)
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
                .SetOauthBearerTokenRefreshHandler(TokenCallback)
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

        private static string GetUnsecuredToken(string aud, DateTimeOffset iat, DateTimeOffset exp)
        {
            var header = new
            {
                alg = "none",
                typ = "JWT"
            };
            var payload = new
            {
                iat = iat.ToUnixTimeSeconds(),
                exp = exp.ToUnixTimeSeconds(),
                typ = "Bearer",
                sub = "Tester",
                aud
            };

            var headerJson = JsonConvert.SerializeObject(header);
            var payloadJson = JsonConvert.SerializeObject(payload);

            var jwt = ToTrimmedBase64(headerJson) + '.' +
                      ToTrimmedBase64(payloadJson) + '.';

            return jwt;
            
            string ToTrimmedBase64(string s)
                => Convert.ToBase64String(Encoding.UTF8.GetBytes(s)).TrimEnd('=');
        }
    }
}