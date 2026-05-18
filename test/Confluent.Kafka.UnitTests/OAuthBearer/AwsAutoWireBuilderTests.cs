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

        [Fact]
        public void ProducerBuilder_Build_ExplicitHandlerWithMarker_PrecedenceRuleHonored()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var config = NewConfig();   // marker is set
            var builder = new ProducerBuilder<string, string>(config)
                .SetOAuthBearerTokenRefreshHandler((_, _) => { /* no-op */ });

            // The precedence rule under test: an explicit OAuthBearer refresh
            // handler must pre-empt the autowire path, so AwsAutoWireDispatcher
            // is NEVER invoked. We verify this from the dispatcher cache state
            // below — independently of whether Build() ultimately succeeds.
            //
            // Build() may fail when librdkafka.redist doesn't yet ship the
            // AWS_IAM marker patch (e.g., CI). librdkafka's rejection of the
            // 'aws_iam' value happens in SafeConfigHandle.Set, which runs
            // *after* the autowire-skip decision in
            // ProducerBuilder.ResolveOAuthBearerHandler — so the cache state
            // by the time we exit Build() is still authoritative evidence.
            TryBuildIgnoringLibrdkafkaAwsIamRejection(() =>
            {
                using var p = builder.Build();
                Assert.NotNull(p);
            });

            Assert.False(AwsAutoWireDispatcher.IsCacheWarmForTests(),
                "Explicit handler should pre-empt the autowire path; the dispatcher " +
                "cache being warm proves AwsAutoWireDispatcher.LoadHandler was invoked " +
                "despite an explicit handler being registered.");
        }

        [Fact]
        public void ConsumerBuilder_Build_ExplicitHandlerWithMarker_PrecedenceRuleHonored()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var consumerConfig = new ConsumerConfig(NewConfig()) { GroupId = "test-group" };
            var builder = new ConsumerBuilder<string, string>(consumerConfig)
                .SetOAuthBearerTokenRefreshHandler((_, _) => { /* no-op */ });

            TryBuildIgnoringLibrdkafkaAwsIamRejection(() =>
            {
                using var c = builder.Build();
                Assert.NotNull(c);
            });

            Assert.False(AwsAutoWireDispatcher.IsCacheWarmForTests(),
                "Explicit handler should pre-empt the autowire path; the dispatcher " +
                "cache being warm proves AwsAutoWireDispatcher.LoadHandler was invoked " +
                "despite an explicit handler being registered.");
        }

        [Fact]
        public void AdminClientBuilder_Build_ExplicitHandlerWithMarker_PrecedenceRuleHonored()
        {
            AwsAutoWireDispatcher.ResetCacheForTests();
            var builder = new AdminClientBuilder(NewConfig())
                .SetOAuthBearerTokenRefreshHandler((_, _) => { /* no-op */ });

            TryBuildIgnoringLibrdkafkaAwsIamRejection(() =>
            {
                using var a = builder.Build();
                Assert.NotNull(a);
            });

            Assert.False(AwsAutoWireDispatcher.IsCacheWarmForTests(),
                "Explicit handler should pre-empt the autowire path; the dispatcher " +
                "cache being warm proves AwsAutoWireDispatcher.LoadHandler was invoked " +
                "despite an explicit handler being registered.");
        }

        /// <summary>
        ///     Runs <paramref name="build"/> and swallows the specific
        ///     <see cref="ArgumentException"/> thrown by
        ///     <c>SafeConfigHandle.Set</c> when running against a stock
        ///     <c>librdkafka.redist</c> that does not yet accept
        ///     <c>aws_iam</c> as a value of
        ///     <c>sasl.oauthbearer.metadata.authentication.type</c>.
        /// </summary>
        /// <remarks>
        ///     <para>
        ///         The catch block asserts that the exception message matches the
        ///         known rejection signature. Any other <see cref="ArgumentException"/>
        ///         signals a real regression and fails the test, so this helper
        ///         doesn't silently swallow unexpected failures.
        ///     </para>
        ///     <para>
        ///         TODO: Remove this helper (and the <c>try/catch</c> at every
        ///         caller) once <c>librdkafka.redist</c> ships with the AWS_IAM
        ///         marker patch. At that point <c>Build()</c> succeeds end-to-end
        ///         and the workaround becomes dead code — the failing
        ///         <see cref="Assert.Contains(string, string)"/> in the catch will
        ///         then never fire, which is the cue to delete this method.
        ///     </para>
        /// </remarks>
        private static void TryBuildIgnoringLibrdkafkaAwsIamRejection(Action build)
        {
            try
            {
                build();
            }
            catch (ArgumentException ex)
            {
                // Guard the swallow: if the exception isn't the known librdkafka
                // rejection of aws_iam, it's a real bug and we want the test to
                // fail loudly. Assert.Contains throws on mismatch, propagating
                // the unexpected ArgumentException's details into the failure
                // output.
                Assert.Contains("sasl.oauthbearer.metadata.authentication.type", ex.Message);
                Assert.Contains("aws_iam", ex.Message);
            }
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
