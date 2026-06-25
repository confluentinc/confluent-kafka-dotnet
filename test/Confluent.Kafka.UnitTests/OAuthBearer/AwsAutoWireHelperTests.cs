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
    public class AwsAutoWireHelperTests
    {
        // ---- AwsAutoWireHelper.ShouldAutoWire (pure marker check) ----

        [Fact]
        public void ShouldAutoWire_MarkerPresent_ReturnsTrue()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "aws_iam",
            };
            Assert.True(AwsAutoWireHelper.ShouldAutoWire(snap));
        }

        [Fact]
        public void ShouldAutoWire_MarkerAbsent_ReturnsFalse()
        {
            var snap = new Dictionary<string, string> { ["bootstrap.servers"] = "x:9092" };
            Assert.False(AwsAutoWireHelper.ShouldAutoWire(snap));
        }

        [Fact]
        public void ShouldAutoWire_AzureImdsValue_ReturnsFalse()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.metadata.authentication.type"] = "azure_imds",
            };
            Assert.False(AwsAutoWireHelper.ShouldAutoWire(snap));
        }

        // ---- AwsAutoWireHelper.Validate (AWS precondition policy) ----

        [Fact]
        public void Validate_MethodOidcAndConfig_DoesNotThrow()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "oidc",
                ["sasl.oauthbearer.config"] = "region=us-east-1,audience=https://a",
            };
            AwsAutoWireHelper.Validate(snap); // does not throw
        }

        [Fact]
        public void Validate_NoMethodOidc_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.config"] = "region=us-east-1,audience=https://a",
            };
            var ex = Assert.Throws<ArgumentException>(
                () => AwsAutoWireHelper.Validate(snap));
            Assert.Contains("SaslOauthbearerMethod.Oidc", ex.Message);
        }

        [Fact]
        public void Validate_NoConfig_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "oidc",
            };
            var ex = Assert.Throws<ArgumentException>(
                () => AwsAutoWireHelper.Validate(snap));
            Assert.Contains("SaslOauthbearerConfig", ex.Message);
            Assert.Contains("missing or empty", ex.Message);
        }

    }
}
