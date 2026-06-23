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
using Confluent.Kafka.Internal.OAuthBearer;
using Xunit;

namespace Confluent.Kafka.UnitTests.OAuthBearer
{
    public class SaslOauthbearerConfigHelperTests
    {
        // ---- SaslOauthbearerConfigHelper.RequireMethodIsOidc ----

        [Fact]
        public void RequireMethodIsOidc_MethodOidc_Allowed()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "oidc",
            };
            SaslOauthbearerConfigHelper.RequireMethodIsOidc(snap); // does not throw
        }

        [Fact]
        public void RequireMethodIsOidc_MethodOidcCaseInsensitive_Allowed()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "OIDC",
            };
            SaslOauthbearerConfigHelper.RequireMethodIsOidc(snap); // does not throw
        }

        [Fact]
        public void RequireMethodIsOidc_MethodMissing_Throws()
        {
            var snap = new Dictionary<string, string>();
            var ex = Assert.Throws<ArgumentException>(
                () => SaslOauthbearerConfigHelper.RequireMethodIsOidc(snap));
            Assert.Contains("SaslOauthbearerMethod.Oidc", ex.Message);
        }

        [Fact]
        public void RequireMethodIsOidc_MethodDefault_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "default",
            };
            var ex = Assert.Throws<ArgumentException>(
                () => SaslOauthbearerConfigHelper.RequireMethodIsOidc(snap));
            Assert.Contains("SaslOauthbearerMethod.Oidc", ex.Message);
            Assert.Contains("'default'", ex.Message);
        }

        [Fact]
        public void RequireMethodIsOidc_MethodUnknownValue_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.method"] = "garbage",
            };
            var ex = Assert.Throws<ArgumentException>(
                () => SaslOauthbearerConfigHelper.RequireMethodIsOidc(snap));
            Assert.Contains("SaslOauthbearerMethod.Oidc", ex.Message);
            Assert.Contains("'garbage'", ex.Message);
        }

        // ---- SaslOauthbearerConfigHelper.RequireSaslOauthbearerConfig ----

        [Fact]
        public void RequireSaslOauthbearerConfig_Present_DoesNotThrow()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.config"] = "region=us-east-1,audience=https://a",
            };
            SaslOauthbearerConfigHelper.RequireSaslOauthbearerConfig(snap); // does not throw
        }

        [Fact]
        public void RequireSaslOauthbearerConfig_Missing_Throws()
        {
            var snap = new Dictionary<string, string>();
            var ex = Assert.Throws<ArgumentException>(
                () => SaslOauthbearerConfigHelper.RequireSaslOauthbearerConfig(snap));
            Assert.Contains("SaslOauthbearerConfig", ex.Message);
            Assert.Contains("missing or empty", ex.Message);
        }

        [Fact]
        public void RequireSaslOauthbearerConfig_Empty_Throws()
        {
            var snap = new Dictionary<string, string>
            {
                ["sasl.oauthbearer.config"] = "",
            };
            var ex = Assert.Throws<ArgumentException>(
                () => SaslOauthbearerConfigHelper.RequireSaslOauthbearerConfig(snap));
            Assert.Contains("SaslOauthbearerConfig", ex.Message);
            Assert.Contains("missing or empty", ex.Message);
        }
    }
}
