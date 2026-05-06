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
using System.Collections.Generic;
using System.Reflection;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    /// <summary>
    ///     Tests <see cref="AwsAutoWire.CreateHandler"/> validation behaviour.
    ///     Handler-invocation behaviour (success/failure routing) is covered by
    ///     <c>AwsOAuthBearerHandlerTests</c>; provider-level fields are covered by
    ///     <c>AwsStsTokenProviderTests</c> and <c>AwsOAuthBearerConfigTests</c>.
    /// </summary>
    public class AwsAutoWireTests
    {
        // ---- CreateHandler input validation ----

        [Fact]
        public void CreateHandler_NullConfig_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => AwsAutoWire.CreateHandler(null));
        }

        [Fact]
        public void CreateHandler_MissingSaslOauthbearerConfig_Throws()
        {
            var config = new Dictionary<string, string>
            {
                ["sasl.mechanism"] = "OAUTHBEARER",
                // No sasl.oauthbearer.config entry.
            };
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(config));
            Assert.Contains("sasl.oauthbearer.config", ex.Message);
        }

        [Fact]
        public void CreateHandler_EmptySaslOauthbearerConfig_Throws()
        {
            var config = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.config"] = "",
            };
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(config));
            Assert.Contains("sasl.oauthbearer.config", ex.Message);
        }

        // ---- Parse delegation: errors propagate from AwsOAuthBearerConfig.Parse ----

        [Fact]
        public void CreateHandler_MissingRegion_Throws()
        {
            var config = NewConfig("audience=https://a");
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(config));
            Assert.Contains("region", ex.Message);
        }

        [Fact]
        public void CreateHandler_MissingAudience_Throws()
        {
            var config = NewConfig("region=us-east-1");
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(config));
            Assert.Contains("audience", ex.Message);
        }

        [Fact]
        public void CreateHandler_InvalidSigningAlgorithm_Throws()
        {
            var config = NewConfig("region=us-east-1 audience=https://a signing_algorithm=HS256");
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(config));
            Assert.Contains("signing_algorithm", ex.Message);
        }

        [Fact]
        public void CreateHandler_InvalidDuration_Throws()
        {
            var config = NewConfig("region=us-east-1 audience=https://a duration_seconds=10");
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(config));
            Assert.Contains("duration_seconds", ex.Message);
        }

        [Fact]
        public void CreateHandler_UnknownKey_Throws()
        {
            var config = NewConfig("region=us-east-1 audience=https://a not_a_key=foo");
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(config));
            Assert.Contains("not_a_key", ex.Message);
        }

        // ---- Success cases — handler returned, no throw ----

        [Fact]
        public void CreateHandler_MarkerOnlyMinimumConfig_ReturnsHandler()
        {
            var config = NewConfig("region=us-east-1 audience=https://a");
            var handler = AwsAutoWire.CreateHandler(config);
            Assert.NotNull(handler);
        }

        [Fact]
        public void CreateHandler_AllOptionalFields_ReturnsHandler()
        {
            var config = NewConfig(
                "region=us-east-1 audience=https://a " +
                "duration_seconds=900 signing_algorithm=RS256 " +
                "sts_endpoint=https://sts.us-east-1.amazonaws.com " +
                "principal_name=my-principal " +
                "extension_logicalCluster=lkc-abc extension_identityPoolId=pool-x" +
                "tag_team=platform tag_environment=prod");
            var handler = AwsAutoWire.CreateHandler(config);
            Assert.NotNull(handler);
        }

        [Fact]
        public void CreateHandler_ExtensionsParsed_HandlerReady()
        {
            // Verify extension_<NAME> entries don't trip parsing (no throw).
            var config = NewConfig(
                "region=us-east-1 audience=https://a extension_traceId=abc-123");
            var handler = AwsAutoWire.CreateHandler(config);
            Assert.NotNull(handler);
        }

        [Fact]
        public void CreateHandler_TagConfig_HandlerReady()
        {
            // Verify tag_<NAME>=<VALUE> entries don't trip parsing.
            var config = NewConfig(
                "region=us-east-1 audience=https://a tag_team=platform tag_environment=prod");
            var handler = AwsAutoWire.CreateHandler(config);
            Assert.NotNull(handler);
        }


        [Fact]
        public void CreateHandler_PrincipalNameOverride_HandlerReady()
        {
            // Verify principal_name doesn't trip parsing (no throw).
            var config = NewConfig(
                "region=us-east-1 audience=https://a principal_name=explicit-principal");
            var handler = AwsAutoWire.CreateHandler(config);
            Assert.NotNull(handler);
        }

        // ---- Frozen reflection contract ----

        [Fact]
        public void AwsAutoWire_TypeAndMethodSignature_Frozen()
        {
            // This test is a contract guard: the type name, method name, and
            // exact parameter / return shapes are part of the cross-package
            // reflection contract that Confluent.Kafka core depends on. Bumping
            // any of these is a breaking change that requires a major version
            // increment of Confluent.Kafka.OAuthBearer.Aws.
            var asm = typeof(AwsAutoWire).Assembly;
            var type = asm.GetType("Confluent.Kafka.OAuthBearer.Aws.AwsAutoWire");
            Assert.NotNull(type);
            Assert.True(type.IsPublic);
            Assert.True(type.IsAbstract && type.IsSealed); // static class

            var method = type.GetMethod(
                "CreateHandler",
                BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(IReadOnlyDictionary<string, string>) },
                modifiers: null);
            Assert.NotNull(method);
            Assert.Equal(typeof(Action<IClient, string>), method.ReturnType);
        }

        // ---- Helper ----

        /// <summary>
        ///     Builds a minimal config dict with only the
        ///     <c>sasl.oauthbearer.config</c> entry that <see cref="AwsAutoWire.CreateHandler"/>
        ///     reads. The marker key is not required here because core's
        ///     dispatcher (which checks the marker) is upstream of this entry-point.
        /// </summary>
        private static IReadOnlyDictionary<string, string> NewConfig(string saslOauthbearerConfig)
            => new Dictionary<string, string>
            {
                ["sasl.oauthbearer.config"] = saslOauthbearerConfig,
            };
    }
}
