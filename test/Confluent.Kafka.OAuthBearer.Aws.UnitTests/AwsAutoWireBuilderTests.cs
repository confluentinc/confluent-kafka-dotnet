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
        // Full autowire happy paths (optional package present, no explicit handler):
        // CreateHandler is reflected + the refresh callback wired, then Build()
        // hands aws_iam + method=oidc to librdkafka. This requires a librdkafka build
        // that recognizes aws_iam (companion PR #5428); stock builds reject it at
        // config validation time.
        [Fact(Skip = "Requires librdkafka with aws_iam support (companion PR #5428): Build() passes aws_iam to librdkafka, which stock builds reject. Un-skip + bump librdkafka.redist once it ships.")]
        public void ProducerBuilder_Build_MarkerAndMethodOidc_WithPkgPresent_Succeeds()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            using var p = new ProducerBuilder<string, string>(NewConfig())
                .SetLogHandler((_, _) => { })
                .Build();
            Assert.NotNull(p);
        }

        [Fact(Skip = "Requires librdkafka with aws_iam support (companion PR #5428): Build() passes aws_iam to librdkafka, which stock builds reject. Un-skip + bump librdkafka.redist once it ships.")]
        public void ConsumerBuilder_Build_MarkerAndMethodOidc_WithPkgPresent_Succeeds()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var consumerConfig = new ConsumerConfig(NewConfig()) { GroupId = "test-group" };
            using var c = new ConsumerBuilder<string, string>(consumerConfig)
                .SetLogHandler((_, _) => { })
                .Build();
            Assert.NotNull(c);
        }

        [Fact(Skip = "Requires librdkafka with aws_iam support (companion PR #5428): Build() passes aws_iam to librdkafka, which stock builds reject. Un-skip + bump librdkafka.redist once it ships.")]
        public void AdminClientBuilder_Build_MarkerAndMethodOidc_WithPkgPresent_Succeeds()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            using var a = new AdminClientBuilder(NewConfig())
                .SetLogHandler((_, _) => { })
                .Build();
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
