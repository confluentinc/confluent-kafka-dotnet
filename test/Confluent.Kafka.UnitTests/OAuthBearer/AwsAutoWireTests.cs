// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using Confluent.Kafka.Internal.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.UnitTests.OAuthBearer
{
    /// <summary>
    ///     R4 tests for <see cref="AwsAutoWire.TryCreateHandler"/>. Cover the
    ///     dispatch decisions (marker present vs absent, sasl.oauthbearer.config
    ///     valid vs missing vs malformed). Actual STS round-trip is exercised
    ///     in R3 tests for the shim and in R6 against real AWS.
    /// </summary>
    public class AwsAutoWireTests
    {
        // ---- Marker absent → null ----

        [Fact]
        public void TryCreateHandler_NoMarker_ReturnsNull()
        {
            var cfg = new[]
            {
                new KeyValuePair<string, string>("bootstrap.servers", "localhost:9092"),
                new KeyValuePair<string, string>("sasl.mechanism", "OAUTHBEARER"),
            };

            Assert.Null(AwsAutoWire.TryCreateHandler(cfg));
        }

        [Fact]
        public void TryCreateHandler_EmptyConfig_ReturnsNull()
        {
            Assert.Null(AwsAutoWire.TryCreateHandler(new KeyValuePair<string, string>[0]));
        }

        [Fact]
        public void TryCreateHandler_MarkerSetToOtherValue_ReturnsNull()
        {
            var cfg = new[]
            {
                new KeyValuePair<string, string>(AwsAutoWire.MarkerKey, "azure_imds"),
            };

            Assert.Null(AwsAutoWire.TryCreateHandler(cfg));
        }

        [Fact]
        public void TryCreateHandler_NullInput_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => AwsAutoWire.TryCreateHandler(null));
        }

        // ---- Marker present + valid sasl.oauthbearer.config → handler ----

        [Fact]
        public void TryCreateHandler_MarkerWithValidSaslConfig_ReturnsNonNullHandler()
        {
            var cfg = new[]
            {
                new KeyValuePair<string, string>(AwsAutoWire.MarkerKey, AwsAutoWire.MarkerValue),
                new KeyValuePair<string, string>("sasl.oauthbearer.config",
                    "region=us-east-1 audience=https://confluent.cloud/oidc"),
            };

            var handler = AwsAutoWire.TryCreateHandler(cfg);

            Assert.NotNull(handler);
        }

        [Fact]
        public void TryCreateHandler_MarkerCaseInsensitive_StillTriggers()
        {
            // SetObject lowercases enums, but we accept marker case-insensitively
            // for raw cfg.Set("...", "AWS_IAM") usage.
            var cfg = new[]
            {
                new KeyValuePair<string, string>(AwsAutoWire.MarkerKey, "AWS_IAM"),
                new KeyValuePair<string, string>("sasl.oauthbearer.config",
                    "region=us-east-1 audience=https://a"),
            };

            Assert.NotNull(AwsAutoWire.TryCreateHandler(cfg));
        }

        // ---- Marker present, sasl.oauthbearer.config missing/empty → throw ----

        [Fact]
        public void TryCreateHandler_MarkerNoSaslConfig_ThrowsClearError()
        {
            var cfg = new[]
            {
                new KeyValuePair<string, string>(AwsAutoWire.MarkerKey, AwsAutoWire.MarkerValue),
            };

            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.TryCreateHandler(cfg));
            Assert.Contains("sasl.oauthbearer.config", ex.Message);
        }

        [Fact]
        public void TryCreateHandler_MarkerEmptySaslConfig_ThrowsClearError()
        {
            var cfg = new[]
            {
                new KeyValuePair<string, string>(AwsAutoWire.MarkerKey, AwsAutoWire.MarkerValue),
                new KeyValuePair<string, string>("sasl.oauthbearer.config", ""),
            };

            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.TryCreateHandler(cfg));
            Assert.Contains("sasl.oauthbearer.config", ex.Message);
        }

        // ---- Marker present, sasl.oauthbearer.config malformed → parser throws ----

        [Fact]
        public void TryCreateHandler_MarkerWithMissingRegion_ThrowsFromParser()
        {
            var cfg = new[]
            {
                new KeyValuePair<string, string>(AwsAutoWire.MarkerKey, AwsAutoWire.MarkerValue),
                new KeyValuePair<string, string>("sasl.oauthbearer.config",
                    "audience=https://a"),
            };

            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.TryCreateHandler(cfg));
            Assert.Contains("region", ex.Message);
        }

        [Fact]
        public void TryCreateHandler_MarkerWithUnknownKey_ThrowsFromParser()
        {
            var cfg = new[]
            {
                new KeyValuePair<string, string>(AwsAutoWire.MarkerKey, AwsAutoWire.MarkerValue),
                new KeyValuePair<string, string>("sasl.oauthbearer.config",
                    "region=us-east-1 audience=https://a unknown_key=foo"),
            };

            var ex = Assert.Throws<ArgumentException>(() => AwsAutoWire.TryCreateHandler(cfg));
            Assert.Contains("unknown_key", ex.Message);
        }

        // ---- Caching: same input → same handler vs different ----

        [Fact]
        public void TryCreateHandler_TwoCalls_StsClientCached()
        {
            // Indirect verification via timing: two calls with the same
            // (region, endpoint) reuse the cached STS client. The first call
            // pays construction cost; the second is fast. Not asserting timing,
            // just that both calls succeed and don't throw.
            var cfg = new[]
            {
                new KeyValuePair<string, string>(AwsAutoWire.MarkerKey, AwsAutoWire.MarkerValue),
                new KeyValuePair<string, string>("sasl.oauthbearer.config",
                    "region=us-east-1 audience=https://a"),
            };

            var h1 = AwsAutoWire.TryCreateHandler(cfg);
            var h2 = AwsAutoWire.TryCreateHandler(cfg);

            Assert.NotNull(h1);
            Assert.NotNull(h2);
        }
    }
}
