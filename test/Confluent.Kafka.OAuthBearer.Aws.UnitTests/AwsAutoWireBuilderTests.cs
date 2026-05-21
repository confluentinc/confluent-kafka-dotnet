// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using Confluent.Kafka.Internal.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    /// <summary>
    ///     Builder-level happy-path tests. Lives in the optional-pkg test project
    ///     because it requires both Confluent.Kafka and Confluent.Kafka.OAuthBearer.Aws
    ///     to be loaded — Confluent.Kafka.UnitTests deliberately doesn't reference
    ///     the optional pkg, so it can only test the missing-pkg failure mode.
    /// </summary>
    public class AwsAutoWireBuilderTests
    {
        // TODO: Remove the Skip on the three *_MarkerWithPkgPresent_Succeeds tests
        // below once librdkafka.redist ships with the AWS_IAM marker patch. Stock
        // librdkafka rejects 'aws_iam' as a value of
        // sasl.oauthbearer.metadata.authentication.type at config validation time
        // (SafeConfigHandle.Set), which prevents Build() from completing in CI.
        // These tests pass locally where a patched librdkafka.so is on the
        // probing path.
        [Fact(Skip = "Requires librdkafka.redist with the AWS_IAM marker patch; remove Skip when available.")]
        public void ProducerBuilder_Build_MarkerAndMethodOidc_WithPkgPresent_Succeeds()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            using var p = new ProducerBuilder<string, string>(NewConfig()).Build();
            Assert.NotNull(p);
        }

        [Fact(Skip = "Requires librdkafka.redist with the AWS_IAM marker patch; remove Skip when available.")]
        public void ConsumerBuilder_Build_MarkerAndMethodOidc_WithPkgPresent_Succeeds()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var consumerConfig = new ConsumerConfig(NewConfig()) { GroupId = "test-group" };
            using var c = new ConsumerBuilder<string, string>(consumerConfig).Build();
            Assert.NotNull(c);
        }

        [Fact(Skip = "Requires librdkafka.redist with the AWS_IAM marker patch; remove Skip when available.")]
        public void AdminClientBuilder_Build_MarkerAndMethodOidc_WithPkgPresent_Succeeds()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            using var a = new AdminClientBuilder(NewConfig()).Build();
            Assert.NotNull(a);
        }

        private static ProducerConfig NewConfig() => new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism    = SaslMechanism.OAuthBearer,
            SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
            SaslOauthbearerMetadataAuthenticationType =
                SaslOauthbearerMetadataAuthenticationType.AwsIam,
            SaslOauthbearerConfig = "region=us-east-1 audience=https://a",
        };
    }
}
