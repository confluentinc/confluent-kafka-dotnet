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
using Confluent.Kafka.Internal.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.UnitTests.OAuthBearer
{
    /// <summary>
    ///     Unit tests for <see cref="AwsAutoWireHelper"/> — the snapshot,
    ///     marker-check, and OIDC-enforcement helpers used by the
    ///     Producer/Consumer/AdminClient builders to dispatch the
    ///     AWS IAM autowire path.
    /// </summary>
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

        // ---- AwsAutoWireHelper.RequireMethodIsOidc ----

        [Fact]
        public void RequireMethodIsOidc_MethodOidc_Allowed()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "oidc",
            };
            AwsAutoWireHelper.RequireMethodIsOidc(snap); // does not throw
        }

        [Fact]
        public void RequireMethodIsOidc_MethodOidcCaseInsensitive_Allowed()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "OIDC",
            };
            AwsAutoWireHelper.RequireMethodIsOidc(snap); // does not throw
        }

        [Fact]
        public void RequireMethodIsOidc_MethodMissing_Throws()
        {
            var snap = new Dictionary<string, string>();
            var ex = Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireHelper.RequireMethodIsOidc(snap));
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
            Assert.Contains("aws_iam", ex.Message);
            Assert.Contains("oidc", ex.Message);
        }

        [Fact]
        public void RequireMethodIsOidc_MethodDefault_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "default",
            };
            var ex = Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireHelper.RequireMethodIsOidc(snap));
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
            Assert.Contains("'default'", ex.Message);
        }

        [Fact]
        public void RequireMethodIsOidc_MethodUnknownValue_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "garbage",
            };
            var ex = Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireHelper.RequireMethodIsOidc(snap));
            Assert.Contains("'garbage'", ex.Message);
        }

        // ---- AwsAutoWireHelper.RequireSaslOauthbearerConfig ----

        [Fact]
        public void RequireSaslOauthbearerConfig_Present_DoesNotThrow()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.config"] = "region=us-east-1 audience=https://a",
            };
            AwsAutoWireHelper.RequireSaslOauthbearerConfig(snap); // does not throw
        }

        [Fact]
        public void RequireSaslOauthbearerConfig_Missing_Throws()
        {
            var snap = new Dictionary<string, string>();
            var ex = Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireHelper.RequireSaslOauthbearerConfig(snap));
            Assert.Contains("sasl.oauthbearer.config", ex.Message);
            Assert.Contains("missing or empty", ex.Message);
        }

        [Fact]
        public void RequireSaslOauthbearerConfig_Empty_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.config"] = "",
            };
            var ex = Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireHelper.RequireSaslOauthbearerConfig(snap));
            Assert.Contains("sasl.oauthbearer.config", ex.Message);
            Assert.Contains("missing or empty", ex.Message);
        }

    }
}
