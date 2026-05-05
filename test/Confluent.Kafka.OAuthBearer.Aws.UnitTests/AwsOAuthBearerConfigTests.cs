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
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class AwsOAuthBearerConfigTests
    {
        // ---- Required fields ----

        [Fact]
        public void Parse_MinimalRequired_PopulatesRegionAndAudience()
        {
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            Assert.Equal("us-east-1", cfg.Region);
            Assert.Equal("https://a", cfg.Audience);
        }

        [Fact]
        public void Parse_MissingRegion_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse("audience=https://a"));
            Assert.Contains("region", ex.Message);
        }

        [Fact]
        public void Parse_MissingAudience_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse("region=us-east-1"));
            Assert.Contains("audience", ex.Message);
        }

        [Fact]
        public void Parse_NullInput_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => AwsOAuthBearerConfig.Parse(null));
        }

        [Fact]
        public void Parse_EmptyInput_ThrowsForMissingRegion()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse(""));
            Assert.Contains("region", ex.Message);
        }

        [Fact]
        public void Parse_EmptyValueOnRequiredKey_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse("region= audience=https://a"));
            Assert.Contains("region", ex.Message);
        }

        [Theory]
        [InlineData("signing_algorithm")]
        [InlineData("sts_endpoint")]
        [InlineData("principal_name")]
        public void Parse_EmptyValueOnOptionalKey_Throws(string key)
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse($"region=us-east-1 audience=https://a {key}="));
            Assert.Contains(key, ex.Message);
        }

        // ---- Defaults applied during Parse ----

        [Fact]
        public void Parse_NoSigningAlgorithm_DefaultsToES384()
        {
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            Assert.Equal("ES384", cfg.SigningAlgorithm);
        }

        [Fact]
        public void Parse_NoDuration_DefaultsTo300Seconds()
        {
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            Assert.Equal(TimeSpan.FromSeconds(300), cfg.Duration);
        }

        [Fact]
        public void Parse_OptionalFields_DefaultToNull()
        {
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            Assert.Null(cfg.StsEndpointOverride);
            Assert.Null(cfg.PrincipalNameOverride);
            Assert.Null(cfg.SaslExtensions);
            Assert.Null(cfg.Tags);
        }

        // ---- duration_seconds ----

        [Theory]
        [InlineData(60)]
        [InlineData(300)]
        [InlineData(3600)]
        public void Parse_DurationInRange_Accepted(int seconds)
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                $"region=us-east-1 audience=https://a duration_seconds={seconds}");
            Assert.Equal(TimeSpan.FromSeconds(seconds), cfg.Duration);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(59)]
        [InlineData(3601)]
        [InlineData(-10)]
        public void Parse_DurationOutOfRange_Throws(int seconds)
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse(
                    $"region=us-east-1 audience=https://a duration_seconds={seconds}"));
            Assert.Contains("duration_seconds", ex.Message);
        }

        [Fact]
        public void Parse_DurationNotInteger_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse(
                    "region=us-east-1 audience=https://a duration_seconds=abc"));
            Assert.Contains("duration_seconds", ex.Message);
        }

        // ---- signing_algorithm ----

        [Theory]
        [InlineData("ES384")]
        [InlineData("RS256")]
        public void Parse_AllowedSigningAlgorithm_Accepted(string alg)
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                $"region=us-east-1 audience=https://a signing_algorithm={alg}");
            Assert.Equal(alg, cfg.SigningAlgorithm);
        }

        [Theory]
        [InlineData("HS256")]
        [InlineData("es384")]
        [InlineData("RS512")]
        public void Parse_DisallowedSigningAlgorithm_Throws(string alg)
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse(
                    $"region=us-east-1 audience=https://a signing_algorithm={alg}"));
            Assert.Contains("signing_algorithm", ex.Message);
        }

        // ---- sts_endpoint, principal_name ----

        [Fact]
        public void Parse_StsEndpoint_StoredVerbatim()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a sts_endpoint=https://sts-fips.us-east-1.amazonaws.com");
            Assert.Equal("https://sts-fips.us-east-1.amazonaws.com", cfg.StsEndpointOverride);
        }

        [Fact]
        public void Parse_PrincipalName_StoredVerbatim()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a principal_name=my-principal");
            Assert.Equal("my-principal", cfg.PrincipalNameOverride);
        }

        // ---- extension_<name> ----

        [Fact]
        public void Parse_SingleExtension_CollectedIntoSaslExtensions()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a extension_logicalCluster=lkc-abc");
            Assert.NotNull(cfg.SaslExtensions);
            Assert.Equal("lkc-abc", cfg.SaslExtensions["logicalCluster"]);
        }

        [Fact]
        public void Parse_MultipleExtensions_AllCollected()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a " +
                "extension_logicalCluster=lkc-abc " +
                "extension_identityPoolId=pool-xyz");
            Assert.Equal(2, cfg.SaslExtensions.Count);
            Assert.Equal("lkc-abc", cfg.SaslExtensions["logicalCluster"]);
            Assert.Equal("pool-xyz", cfg.SaslExtensions["identityPoolId"]);
        }

        [Fact]
        public void Parse_EmptyExtensionName_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse(
                    "region=us-east-1 audience=https://a extension_=value"));
            Assert.Contains("extension_", ex.Message);
        }

        // ---- tag_<name> ----

        [Fact]
        public void Parse_SingleTag_CollectedIntoTags()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a tag_team=platform");
            Assert.NotNull(cfg.Tags);
            Assert.Single(cfg.Tags);
            Assert.Equal("platform", cfg.Tags["team"]);
        }

        [Fact]
        public void Parse_MultipleTags_AllCollected()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a " +
                "tag_team=platform " +
                "tag_environment=prod");
            Assert.Equal(2, cfg.Tags.Count);
            Assert.Equal("platform", cfg.Tags["team"]);
            Assert.Equal("prod", cfg.Tags["environment"]);
        }

        [Fact]
        public void Parse_EmptyTagName_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse(
                    "region=us-east-1 audience=https://a tag_=value"));
            Assert.Contains("tag", ex.Message);
        }

        [Fact]
        public void Parse_EmptyTagValue_Accepted()
        {
            // AWS allows tag values of 0 chars; mirror that.
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a tag_team=");
            Assert.Equal("", cfg.Tags["team"]);
        }

        [Fact]
        public void Parse_DuplicateTagName_LastWins()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a tag_team=infra tag_team=platform");
            Assert.Equal("platform", cfg.Tags["team"]);
        }

        [Fact]
        public void Parse_TagsAndExtensionsCoexist()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a " +
                "extension_logicalCluster=lkc-abc " +
                "tag_team=platform");
            Assert.Equal("lkc-abc", cfg.SaslExtensions["logicalCluster"]);
            Assert.Equal("platform", cfg.Tags["team"]);
        }

        [Fact]
        public void Parse_ExactlyMaxTags_Accepted()
        {
            var sb = new System.Text.StringBuilder("region=us-east-1 audience=https://a");
            for (int i = 0; i < 50; i++)
            {
                sb.Append($" tag_k{i}=v{i}");
            }
            var cfg = AwsOAuthBearerConfig.Parse(sb.ToString());
            Assert.Equal(50, cfg.Tags.Count);
        }

        [Fact]
        public void Parse_OverMaxTags_Throws()
        {
            var sb = new System.Text.StringBuilder("region=us-east-1 audience=https://a");
            for (int i = 0; i < 51; i++)
            {
                sb.Append($" tag_k{i}=v{i}");
            }
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse(sb.ToString()));
            Assert.Contains("50", ex.Message);
        }

        // ---- Unknown keys ----

        [Fact]
        public void Parse_UnknownKey_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse(
                    "region=us-east-1 audience=https://a not_a_key=foo"));
            Assert.Contains("not_a_key", ex.Message);
        }

        // ---- Whitespace / ordering ----

        [Fact]
        public void Parse_TabsAndMultipleSpaces_Tolerated()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1\taudience=https://a   duration_seconds=600");
            Assert.Equal("us-east-1", cfg.Region);
            Assert.Equal("https://a", cfg.Audience);
            Assert.Equal(TimeSpan.FromSeconds(600), cfg.Duration);
        }

        [Fact]
        public void Parse_LeadingAndTrailingWhitespace_Tolerated()
        {
            var cfg = AwsOAuthBearerConfig.Parse("  region=us-east-1 audience=https://a   ");
            Assert.Equal("us-east-1", cfg.Region);
            Assert.Equal("https://a", cfg.Audience);
        }

        [Fact]
        public void Parse_OrderInvariant()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "duration_seconds=600 audience=https://a region=us-east-1 signing_algorithm=RS256");
            Assert.Equal("us-east-1", cfg.Region);
            Assert.Equal("https://a", cfg.Audience);
            Assert.Equal(TimeSpan.FromSeconds(600), cfg.Duration);
            Assert.Equal("RS256", cfg.SigningAlgorithm);
        }

        [Fact]
        public void Parse_DuplicateKey_LastWins()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a region=us-west-2");
            Assert.Equal("us-west-2", cfg.Region);
        }

        // ---- Malformed entries ----

        [Fact]
        public void Parse_NoEquals_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse("region us-east-1 audience=https://a"));
            Assert.Contains("Malformed", ex.Message);
        }

        [Fact]
        public void Parse_LeadingEquals_Throws()
        {
            var ex = Assert.Throws<ArgumentException>(
                () => AwsOAuthBearerConfig.Parse("=value audience=https://a region=us-east-1"));
            Assert.Contains("Malformed", ex.Message);
        }

        [Fact]
        public void Parse_AllFieldsTogether_AllPopulatedCorrectly()
        {
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 " +
                "audience=https://confluent.cloud/oidc " +
                "duration_seconds=1800 " +
                "signing_algorithm=RS256 " +
                "sts_endpoint=https://sts.us-east-1.amazonaws.com " +
                "principal_name=test-principal " +
                "extension_logicalCluster=lkc-abc " +
                "tag_team=platform");

            Assert.Equal("us-east-1", cfg.Region);
            Assert.Equal("https://confluent.cloud/oidc", cfg.Audience);
            Assert.Equal(TimeSpan.FromSeconds(1800), cfg.Duration);
            Assert.Equal("RS256", cfg.SigningAlgorithm);
            Assert.Equal("https://sts.us-east-1.amazonaws.com", cfg.StsEndpointOverride);
            Assert.Equal("test-principal", cfg.PrincipalNameOverride);
            Assert.Equal("lkc-abc", cfg.SaslExtensions["logicalCluster"]);
            Assert.Equal("platform", cfg.Tags["team"]);
        }
    }
}
