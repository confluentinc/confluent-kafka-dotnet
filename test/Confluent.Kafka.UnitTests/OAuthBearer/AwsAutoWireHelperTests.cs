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
using System.Linq;
using Confluent.Kafka.Internal.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.UnitTests.OAuthBearer
{
    public class AwsAutoWireHelperTests
    {
        // ---- AwsAutoWireHelper.SnapshotConfig ----

        [Fact]
        public void SnapshotConfig_NullEnumerable_ReturnsEmpty()
        {
            var snap = AwsAutoWireHelper.SnapshotConfig(null);
            Assert.Empty(snap);
        }

        [Fact]
        public void SnapshotConfig_DuplicateKeys_LastWins()
        {
            var entries = new[]
            {
                new KeyValuePair<string, string>("k", "v1"),
                new KeyValuePair<string, string>("k", "v2"),
            };
            var snap = AwsAutoWireHelper.SnapshotConfig(entries);
            Assert.Single(snap);
            Assert.Equal("v2", snap["k"]);
        }

        // ---- AwsAutoWireHelper.HasAwsIamMarker ----

        [Fact]
        public void HasAwsIamMarker_Present_ReturnsTrue()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
            };
            Assert.True(AwsAutoWireHelper.HasAwsIamMarker(snap));
        }

        [Fact]
        public void HasAwsIamMarker_Absent_ReturnsFalse()
        {
            var snap = new Dictionary<string, string> { ["bootstrap.servers"] = "x:9092" };
            Assert.False(AwsAutoWireHelper.HasAwsIamMarker(snap));
        }

        [Fact]
        public void HasAwsIamMarker_AzureImdsValue_ReturnsFalse()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "azure_imds",
            };
            Assert.False(AwsAutoWireHelper.HasAwsIamMarker(snap));
        }

        // ---- AwsAutoWireHelper.ShouldAutoWire ----

        [Fact]
        public void ShouldAutoWire_MarkerAbsent_ReturnsFalse()
        {
            var snap = new Dictionary<string, string> { ["bootstrap.servers"] = "x:9092" };
            Assert.False(AwsAutoWireHelper.ShouldAutoWire(snap));
        }

        [Fact]
        public void ShouldAutoWire_MarkerPresentMethodOidcAndConfig_ReturnsTrue()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
                ["sasl.oauthbearer.method"] = "oidc",
                ["sasl.oauthbearer.config"] = "region=us-east-1 audience=https://a",
            };
            Assert.True(AwsAutoWireHelper.ShouldAutoWire(snap));
        }

        [Fact]
        public void ShouldAutoWire_MarkerAndConfigPresentNoMethodOidc_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
                ["sasl.oauthbearer.config"] = "region=us-east-1 audience=https://a",
            };
            var ex = Assert.Throws<ArgumentException>(
                () => AwsAutoWireHelper.ShouldAutoWire(snap));
            Assert.Contains("SaslOauthbearerMethod.Oidc", ex.Message);
        }

        [Fact]
        public void ShouldAutoWire_MarkerAndMethodOidcPresentNoConfig_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
                ["sasl.oauthbearer.method"] = "oidc",
            };
            var ex = Assert.Throws<ArgumentException>(
                () => AwsAutoWireHelper.ShouldAutoWire(snap));
            Assert.Contains("SaslOauthbearerConfig", ex.Message);
            Assert.Contains("missing or empty", ex.Message);
        }

        // ---- AwsAutoWireHelper.RewriteConfigIfAwsIamEnabled ----

        [Fact]
        public void RewriteConfigIfAwsIamEnabled_Null_ReturnsNull()
        {
            Assert.Null(AwsAutoWireHelper.RewriteConfigIfAwsIamEnabled(null));
        }

        [Fact]
        public void RewriteConfigIfAwsIamEnabled_MarkerAbsent_ReturnsInputUnchanged()
        {
            var input = new[]
            {
                new KeyValuePair<string, string>("bootstrap.servers", "x:9092"),
                new KeyValuePair<string, string>("sasl.oauthbearer.method", "oidc"),
            };
            // Marker absent => same reference back, zero behavior change.
            Assert.Same(input, AwsAutoWireHelper.RewriteConfigIfAwsIamEnabled(input));
        }

        [Fact]
        public void RewriteConfigIfAwsIamEnabled_MarkerPresent_StripsMarker()
        {
            var input = new[]
            {
                new KeyValuePair<string, string>("sasl.oauthbearer.metadata.authentication.type", "aws_iam"),
                new KeyValuePair<string, string>("sasl.oauthbearer.method", "oidc"),
                new KeyValuePair<string, string>("sasl.oauthbearer.config", "region=us-east-1 audience=https://a"),
            };
            var snap = AwsAutoWireHelper.SnapshotConfig(AwsAutoWireHelper.RewriteConfigIfAwsIamEnabled(input));
            Assert.False(snap.ContainsKey("sasl.oauthbearer.metadata.authentication.type"));
        }

        [Fact]
        public void RewriteConfigIfAwsIamEnabled_MarkerPresent_MethodRewrittenToDefault()
        {
            var input = new[]
            {
                new KeyValuePair<string, string>("sasl.oauthbearer.metadata.authentication.type", "aws_iam"),
                new KeyValuePair<string, string>("sasl.oauthbearer.method", "oidc"),
            };
            var snap = AwsAutoWireHelper.SnapshotConfig(AwsAutoWireHelper.RewriteConfigIfAwsIamEnabled(input));
            Assert.Equal("default", snap["sasl.oauthbearer.method"]);
        }

        [Fact]
        public void RewriteConfigIfAwsIamEnabled_MarkerPresent_PreservesOtherKeys()
        {
            var input = new[]
            {
                new KeyValuePair<string, string>("bootstrap.servers", "x:9092"),
                new KeyValuePair<string, string>("sasl.oauthbearer.metadata.authentication.type", "aws_iam"),
                new KeyValuePair<string, string>("sasl.oauthbearer.method", "oidc"),
                new KeyValuePair<string, string>("sasl.oauthbearer.config", "region=us-east-1 audience=https://a"),
                new KeyValuePair<string, string>("sasl.oauthbearer.extensions", "logicalCluster=lkc-1,identityPoolId=pool-1"),
            };
            var snap = AwsAutoWireHelper.SnapshotConfig(AwsAutoWireHelper.RewriteConfigIfAwsIamEnabled(input));
            Assert.Equal("x:9092", snap["bootstrap.servers"]);
            Assert.Equal("region=us-east-1 audience=https://a", snap["sasl.oauthbearer.config"]);
            Assert.Equal("logicalCluster=lkc-1,identityPoolId=pool-1", snap["sasl.oauthbearer.extensions"]);
        }

        [Fact]
        public void RewriteConfigIfAwsIamEnabled_MarkerPresent_DuplicateMethodKeys_CollapseToSingleDefault()
        {
            var input = new[]
            {
                new KeyValuePair<string, string>("sasl.oauthbearer.metadata.authentication.type", "aws_iam"),
                new KeyValuePair<string, string>("sasl.oauthbearer.method", "oidc"),
                new KeyValuePair<string, string>("sasl.oauthbearer.method", "oidc"),
            };
            var methodEntries = AwsAutoWireHelper.RewriteConfigIfAwsIamEnabled(input)
                .Where(kv => kv.Key == "sasl.oauthbearer.method").ToList();
            Assert.Single(methodEntries);
            Assert.Equal("default", methodEntries[0].Value);
        }

    }
}
