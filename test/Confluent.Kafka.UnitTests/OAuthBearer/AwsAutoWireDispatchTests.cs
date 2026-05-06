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
    ///     Tests the core-side dispatch helpers. The Confluent.Kafka.UnitTests
    ///     project deliberately does <b>not</b> reference
    ///     <c>Confluent.Kafka.OAuthBearer.Aws</c> — exercising the "pkg missing"
    ///     failure mode requires the optional package to be absent from the
    ///     test bin/.
    /// </summary>
    public class AwsAutoWireDispatchTests
    {
        // ---- AwsIamMarker.IsMarker ----

        [Fact]
        public void IsMarker_AwsIamValue_ReturnsTrue()
        {
            var prop = new KeyValuePair<string, string>(
                "sasl.oauthbearer.metadata.authentication.type", "aws_iam");
            Assert.True(AwsIamMarker.IsMarker(prop));
        }

        [Fact]
        public void IsMarker_AwsIamValue_CaseInsensitive()
        {
            var prop = new KeyValuePair<string, string>(
                "sasl.oauthbearer.metadata.authentication.type", "AWS_IAM");
            Assert.True(AwsIamMarker.IsMarker(prop));
        }

        [Theory]
        [InlineData("none")]
        [InlineData("azure_imds")]
        [InlineData("")]
        public void IsMarker_OtherValues_ReturnFalse(string value)
        {
            var prop = new KeyValuePair<string, string>(
                "sasl.oauthbearer.metadata.authentication.type", value);
            Assert.False(AwsIamMarker.IsMarker(prop));
        }

        [Fact]
        public void IsMarker_DifferentKey_ReturnsFalse()
        {
            var prop = new KeyValuePair<string, string>("some.other.key", "aws_iam");
            Assert.False(AwsIamMarker.IsMarker(prop));
        }

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

        // ---- AwsAutoWireHelper.RejectIfMethodIsOidc ----

        [Fact]
        public void RejectIfMethodIsOidc_MethodOidc_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "oidc",
            };
            var ex = Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireHelper.RejectIfMethodIsOidc(snap));
            Assert.Contains("sasl.oauthbearer.method", ex.Message);
            Assert.Contains("aws_iam", ex.Message);
        }

        [Fact]
        public void RejectIfMethodIsOidc_MethodOidcCaseInsensitive_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "OIDC",
            };
            Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireHelper.RejectIfMethodIsOidc(snap));
        }

        [Fact]
        public void RejectIfMethodIsOidc_MethodMissing_Allowed()
        {
            var snap = new Dictionary<string, string>();
            AwsAutoWireHelper.RejectIfMethodIsOidc(snap); // does not throw
        }

        [Fact]
        public void RejectIfMethodIsOidc_MethodDefault_Allowed()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "default",
            };
            AwsAutoWireHelper.RejectIfMethodIsOidc(snap); // does not throw
        }

        // ---- AwsAutoWireDispatcher.LoadHandler ----

        [Fact]
        public void LoadHandler_NullConfig_Throws()
        {
            // Reset the dispatcher's MethodInfo cache to ensure a fresh resolve attempt.
            AwsAutoWireDispatcher.ResetCacheForTests();
            Assert.Throws<ArgumentNullException>(() => AwsAutoWireDispatcher.LoadHandler(null));
        }

        [Fact]
        public void LoadHandler_OptionalPackageMissing_ThrowsFriendlyInvalidOperation()
        {
            // The Confluent.Kafka.UnitTests project does not reference
            // Confluent.Kafka.OAuthBearer.Aws — its assembly is therefore not
            // resolvable. The dispatcher should translate the underlying
            // FileNotFoundException into a friendly InvalidOperationException
            // pointing the user at the missing PackageReference.
            AwsAutoWireDispatcher.ResetCacheForTests();
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
                ["sasl.oauthbearer.config"] = "region=us-east-1 audience=https://a",
            };
            var ex = Assert.Throws<InvalidOperationException>(
                () => AwsAutoWireDispatcher.LoadHandler(snap));
            Assert.Contains("Confluent.Kafka.OAuthBearer.Aws", ex.Message);
            Assert.Contains("PackageReference", ex.Message);
            // Inner exception preserved for diagnostics.
            Assert.NotNull(ex.InnerException);
        }
    }
}
