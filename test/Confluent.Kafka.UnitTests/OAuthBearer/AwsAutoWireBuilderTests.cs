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

namespace Confluent.Kafka.UnitTests.OAuthBearer
{
    public class AwsAutoWireBuilderTests
    {
        [Fact]
        public void ProducerBuilder_Build_MarkerAndMethodOidc_HitsMissingPkgPath()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            // With method=oidc set, the RequireMethodIsOidc guard passes; Build()
            // then attempts to load the optional pkg, which this project doesn't
            // reference, so we land on the missing-pkg error.
            var ex = Assert.Throws<InvalidOperationException>(
                () => new ProducerBuilder<string, string>(config).Build());
            Assert.Contains("Confluent.Kafka.OAuthBearer.Aws", ex.Message);
            Assert.Contains("PackageReference", ex.Message);
        }

        [Fact]
        public void ProducerBuilder_Build_MarkerWithoutMethodOidc_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();   // marker set, but no SaslOauthbearerMethod
            var ex = Assert.Throws<InvalidOperationException>(
                () => new ProducerBuilder<string, string>(config).Build());
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
            Assert.Contains("aws_iam", ex.Message);
            Assert.Contains("oidc", ex.Message);
        }

        [Fact]
        public void ConsumerBuilder_Build_MarkerAndMethodOidc_HitsMissingPkgPath()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            var consumerConfig = new ConsumerConfig(config) { GroupId = "test-group" };
            var ex = Assert.Throws<InvalidOperationException>(
                () => new ConsumerBuilder<string, string>(consumerConfig).Build());
            Assert.Contains("Confluent.Kafka.OAuthBearer.Aws", ex.Message);
            Assert.Contains("PackageReference", ex.Message);
        }

        [Fact]
        public void ConsumerBuilder_Build_MarkerWithoutMethodOidc_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var consumerConfig = new ConsumerConfig(NewConfig()) { GroupId = "test-group" };
            var ex = Assert.Throws<InvalidOperationException>(
                () => new ConsumerBuilder<string, string>(consumerConfig).Build());
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
            Assert.Contains("aws_iam", ex.Message);
            Assert.Contains("oidc", ex.Message);
        }

        [Fact]
        public void AdminClientBuilder_Build_MarkerAndMethodOidc_HitsMissingPkgPath()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            var ex = Assert.Throws<InvalidOperationException>(
                () => new AdminClientBuilder(config).Build());
            Assert.Contains("Confluent.Kafka.OAuthBearer.Aws", ex.Message);
            Assert.Contains("PackageReference", ex.Message);
        }

        [Fact]
        public void AdminClientBuilder_Build_MarkerWithoutMethodOidc_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            var ex = Assert.Throws<InvalidOperationException>(
                () => new AdminClientBuilder(config).Build());
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
            Assert.Contains("aws_iam", ex.Message);
            Assert.Contains("oidc", ex.Message);
        }

        // TODO: Remove the Skip on the three *_ExplicitHandlerWithMarker_PrecedenceRuleHonored
        // tests below once librdkafka.redist ships with the AWS_IAM marker patch. Stock
        // librdkafka rejects 'aws_iam' as a value of
        // sasl.oauthbearer.metadata.authentication.type at config validation time
        // (SafeConfigHandle.Set), which prevents Build() from completing in CI. These
        // tests pass locally where a patched librdkafka.so is on the probing path.
        [Fact(Skip = "Requires librdkafka.redist with the AWS_IAM marker patch; remove Skip when available.")]
        public void ProducerBuilder_Build_ExplicitHandlerWithMarker_PrecedenceRuleHonored()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();   // marker is set
            var builder = new ProducerBuilder<string, string>(config)
                .SetOAuthBearerTokenRefreshHandler((_, _) => { /* no-op */ });

            // If precedence works, this Build() does NOT throw the missing-pkg error.
            using var p = builder.Build();
            Assert.NotNull(p);
        }

        [Fact(Skip = "Requires librdkafka.redist with the AWS_IAM marker patch; remove Skip when available.")]
        public void ConsumerBuilder_Build_ExplicitHandlerWithMarker_PrecedenceRuleHonored()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var consumerConfig = new ConsumerConfig(NewConfig()) { GroupId = "test-group" };
            var builder = new ConsumerBuilder<string, string>(consumerConfig)
                .SetOAuthBearerTokenRefreshHandler((_, _) => { /* no-op */ });

            // If precedence works, this Build() does NOT throw the missing-pkg error.
            using var c = builder.Build();
            Assert.NotNull(c);
        }

        [Fact(Skip = "Requires librdkafka.redist with the AWS_IAM marker patch; remove Skip when available.")]
        public void AdminClientBuilder_Build_ExplicitHandlerWithMarker_PrecedenceRuleHonored()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var builder = new AdminClientBuilder(NewConfig())
                .SetOAuthBearerTokenRefreshHandler((_, _) => { /* no-op */ });

            // If precedence works, this Build() does NOT throw the missing-pkg error.
            using var a = builder.Build();
            Assert.NotNull(a);
        }

        private static ProducerConfig NewConfig() => new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism    = SaslMechanism.OAuthBearer,
            SaslOauthbearerMetadataAuthenticationType =
                SaslOauthbearerMetadataAuthenticationType.AwsIam,
            SaslOauthbearerConfig = "region=us-east-1 audience=https://a",
        };
    }
}
