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
        public void ProducerBuilder_Build_MarkerWithoutPkg_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            var ex = Assert.Throws<InvalidOperationException>(
                () => new ProducerBuilder<string, string>(config).Build());
            Assert.Contains("Confluent.Kafka.OAuthBearer.Aws", ex.Message);
            Assert.Contains("PackageReference", ex.Message);
        }

        [Fact]
        public void ConsumerBuilder_Build_MarkerWithoutPkg_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var consumerConfig = new ConsumerConfig(NewConfig()) { GroupId = "test-group" };
            var ex = Assert.Throws<InvalidOperationException>(
                () => new ConsumerBuilder<string, string>(consumerConfig).Build());
            Assert.Contains("Confluent.Kafka.OAuthBearer.Aws", ex.Message);
        }

        [Fact]
        public void ProducerBuilder_Build_MarkerAndMethodOidc_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            var ex = Assert.Throws<InvalidOperationException>(
                () => new ProducerBuilder<string, string>(config).Build());
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
            Assert.Contains("aws_iam", ex.Message);
        }

        [Fact]
        public void ConsumerBuilder_Build_MarkerAndMethodOidc_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            var consumerConfig = new ConsumerConfig(config) { GroupId = "test-group" };
            var ex = Assert.Throws<InvalidOperationException>(
                () => new ConsumerBuilder<string, string>(consumerConfig).Build());
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
        }

        [Fact]
        public void AdminClientBuilder_Build_MarkerWithoutPkg_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            var ex = Assert.Throws<InvalidOperationException>(
                () => new AdminClientBuilder(config).Build());
            Assert.Contains("Confluent.Kafka.OAuthBearer.Aws", ex.Message);
        }

        [Fact]
        public void AdminClientBuilder_Build_MarkerAndMethodOidc_Throws()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();
            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
            var ex = Assert.Throws<InvalidOperationException>(
                () => new AdminClientBuilder(config).Build());
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
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
