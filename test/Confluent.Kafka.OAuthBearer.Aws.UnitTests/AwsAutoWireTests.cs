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
using System.Reflection;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class AwsAutoWireTests
    {
        // ---- CreateHandler input validation (defensive checks for direct callers) ----

        [Fact]
        public void CreateHandler_NullConfig_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsAutoWire.CreateHandler(null, null));
            Assert.Contains("SaslOauthbearerConfig", ex.Message);
            Assert.Contains("missing or empty", ex.Message);
        }

        [Fact]
        public void CreateHandler_EmptyConfig_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsAutoWire.CreateHandler("", null));
            Assert.Contains("SaslOauthbearerConfig", ex.Message);
            Assert.Contains("missing or empty", ex.Message);
        }

        // ---- Parse delegation: errors propagate from AwsOAuthBearerConfig.Parse ----

        [Fact]
        public void CreateHandler_MissingRegion_Throws()
        {
            var rawConfig = "audience=https://a";
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(rawConfig, null));
            Assert.Contains("region", ex.Message);
        }

        [Fact]
        public void CreateHandler_MissingAudience_Throws()
        {
            var rawConfig = "region=us-east-1";
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(rawConfig, null));
            Assert.Contains("audience", ex.Message);
        }

        [Fact]
        public void CreateHandler_InvalidSigningAlgorithm_Throws()
        {
            var rawConfig = "region=us-east-1 audience=https://a signing_algorithm=HS256";
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(rawConfig, null));
            Assert.Contains("signing_algorithm", ex.Message);
        }

        [Fact]
        public void CreateHandler_InvalidDuration_Throws()
        {
            var rawConfig = "region=us-east-1 audience=https://a duration_seconds=10";
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(rawConfig, null));
            Assert.Contains("duration_seconds", ex.Message);
        }

        [Fact]
        public void CreateHandler_UnknownKey_Throws()
        {
            var rawConfig = "region=us-east-1 audience=https://a not_a_key=foo";
            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.CreateHandler(rawConfig, null));
            Assert.Contains("not_a_key", ex.Message);
        }

        // ---- Success cases — handler returned, no throw ----

        [Fact]
        public void CreateHandler_MarkerOnlyMinimumConfig_ReturnsHandler()
        {
            var handler = AwsAutoWire.CreateHandler(
                "region=us-east-1 audience=https://a", null);
            Assert.NotNull(handler);
        }

        [Fact]
        public void CreateHandler_AllOptionalFields_ReturnsHandler()
        {
            var handler = AwsAutoWire.CreateHandler(
                "region=us-east-1 audience=https://a " +
                "duration_seconds=900 signing_algorithm=RS256 " +
                "sts_endpoint=https://sts.us-east-1.amazonaws.com " +
                "aws_debug=none " +
                "tag_team=platform tag_environment=prod",
                null);
            Assert.NotNull(handler);
        }

        [Fact]
        public void CreateHandler_TagConfig_HandlerReady()
        {
            var handler = AwsAutoWire.CreateHandler(
                "region=us-east-1 audience=https://a tag_team=platform tag_environment=prod",
                null);
            Assert.NotNull(handler);
        }


        [Fact]
        public void CreateHandler_NullExtensions_TreatsAsAbsent()
        {
            var handler = AwsAutoWire.CreateHandler(
                "region=us-east-1 audience=https://a",
                saslOauthbearerExtensions: null);
            Assert.NotNull(handler);
        }

        [Fact]
        public void CreateHandler_EmptyExtensions_TreatsAsAbsent()
        {
            var handler = AwsAutoWire.CreateHandler(
                "region=us-east-1 audience=https://a",
                saslOauthbearerExtensions: "");
            Assert.NotNull(handler);
        }

        // ---- Typed sasl.oauthbearer.extensions parsing ----

        [Fact]
        public void CreateHandler_TypedExtensions_SingleEntry_HandlerReady()
        {
            var handler = AwsAutoWire.CreateHandler(
                "region=us-east-1 audience=https://a",
                "logicalCluster=lkc-abc");
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
                types: new[] { typeof(string), typeof(string) },
                modifiers: null);
            Assert.NotNull(method);
            Assert.Equal(typeof(Action<IClient, string>), method.ReturnType);
        }
    }
}
