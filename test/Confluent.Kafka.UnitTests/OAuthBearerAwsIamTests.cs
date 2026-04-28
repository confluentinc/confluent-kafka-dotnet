// Copyright 2026 Confluent Inc.
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

using System.Linq;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    /// <summary>
    ///     R1 exit-gate tests: the .NET-only enum value <c>AwsIam</c> serialises
    ///     to <c>aws_iam</c>, gets stripped before reaching librdkafka, and does
    ///     not regress the existing <c>AzureIMDS</c> pass-through to native.
    /// </summary>
    public class OAuthBearerAwsIamTests
    {
        [Fact]
        public void AwsIam_SerialisesToAwsIamWireValue()
        {
            var cfg = new ProducerConfig
            {
                SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam
            };

            var entry = cfg.Single(kv => kv.Key == "sasl.oauthbearer.metadata.authentication.type");
            Assert.Equal("aws_iam", entry.Value);
        }

        [Fact]
        public void AzureIMDS_StillSerialisesToAzureImdsWireValue()
        {
            var cfg = new ProducerConfig
            {
                SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AzureIMDS
            };

            var entry = cfg.Single(kv => kv.Key == "sasl.oauthbearer.metadata.authentication.type");
            Assert.Equal("azure_imds", entry.Value);
        }

        [Fact]
        public void AwsIam_RoundTripsThroughTypedGetter()
        {
            var cfg = new ProducerConfig
            {
                SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam
            };

            Assert.Equal(SaslOauthbearerMetadataAuthenticationType.AwsIam,
                cfg.SaslOauthbearerMetadataAuthenticationType);
        }

        [Fact]
        public void AwsIam_ProducerBuildSucceeds_StripPreventsLibrdkafkaRejection()
        {
            // If the strip in Producer.cs is broken, librdkafka rejects the
            // unknown enum value at config validation time and throws.
            var cfg = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam,
            };

            var builder = new ProducerBuilder<byte[], byte[]>(cfg);
            // OAUTHBEARER requires either a refresh handler or unsecure-jwt mode.
            builder.SetOAuthBearerTokenRefreshHandler((p, _) => p.OAuthBearerSetTokenFailure("test no-op"));

            using (builder.Build())
            {
                // Reaching this line means rd_kafka_new accepted the (post-strip)
                // config, which is what R1 is verifying.
            }
        }

        [Fact]
        public void AwsIam_ConsumerBuildSucceeds_StripPreventsLibrdkafkaRejection()
        {
            var cfg = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-group",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.OAuthBearer,
                SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam,
            };

            var builder = new ConsumerBuilder<byte[], byte[]>(cfg);
            builder.SetOAuthBearerTokenRefreshHandler((c, _) => c.OAuthBearerSetTokenFailure("test no-op"));

            using (builder.Build())
            {
            }
        }
    }
}
