using System;
using System.Collections.Generic;
using Confluent.Kafka.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class AwsOAuthBearerConfigTests
    {
        // ---- Validate: required fields ----

        [Fact]
        public void Validate_NullRegion_Throws()
        {
            var cfg = new AwsOAuthBearerConfig { Region = null, Audience = "a" };
            var ex = Assert.Throws<ArgumentException>(() => cfg.Validate());
            Assert.Contains(nameof(AwsOAuthBearerConfig.Region), ex.Message);
        }

        [Fact]
        public void Validate_EmptyRegion_Throws()
        {
            var cfg = new AwsOAuthBearerConfig { Region = "", Audience = "a" };
            var ex = Assert.Throws<ArgumentException>(() => cfg.Validate());
            Assert.Contains(nameof(AwsOAuthBearerConfig.Region), ex.Message);
        }

        [Fact]
        public void Validate_NullAudience_Throws()
        {
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = null };
            var ex = Assert.Throws<ArgumentException>(() => cfg.Validate());
            Assert.Contains(nameof(AwsOAuthBearerConfig.Audience), ex.Message);
        }

        [Fact]
        public void Validate_EmptyAudience_Throws()
        {
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "" };
            var ex = Assert.Throws<ArgumentException>(() => cfg.Validate());
            Assert.Contains(nameof(AwsOAuthBearerConfig.Audience), ex.Message);
        }

        // ---- Validate: SigningAlgorithm ----

        [Theory]
        [InlineData("ES384")]
        [InlineData("RS256")]
        [InlineData(null)]
        public void Validate_ValidSigningAlgorithm_Accepted(string alg)
        {
            var cfg = MinimalValid();
            cfg.SigningAlgorithm = alg;
            cfg.Validate(); // does not throw
        }

        [Theory]
        [InlineData("HS256")]
        [InlineData("")]
        [InlineData("es384")]     // case-sensitive
        [InlineData("RS512")]
        public void Validate_InvalidSigningAlgorithm_Throws(string alg)
        {
            var cfg = MinimalValid();
            cfg.SigningAlgorithm = alg;
            var ex = Assert.Throws<ArgumentException>(() => cfg.Validate());
            Assert.Contains(nameof(AwsOAuthBearerConfig.SigningAlgorithm), ex.Message);
        }

        // ---- Validate: Duration bounds ----

        [Fact]
        public void Validate_DefaultDuration_Accepted()
        {
            var cfg = MinimalValid();
            // Duration left at default(TimeSpan) == TimeSpan.Zero — treated as "use default".
            cfg.Validate();
        }

        [Theory]
        [InlineData(60)]      // lower bound, inclusive
        [InlineData(300)]
        [InlineData(3600)]    // upper bound, inclusive
        public void Validate_InRangeDuration_Accepted(int seconds)
        {
            var cfg = MinimalValid();
            cfg.Duration = TimeSpan.FromSeconds(seconds);
            cfg.Validate();
        }

        [Theory]
        [InlineData(30)]      // below floor
        [InlineData(59)]      // just below floor
        [InlineData(3601)]    // just above ceiling
        [InlineData(7200)]    // 2 hours
        public void Validate_OutOfRangeDuration_Throws(int seconds)
        {
            var cfg = MinimalValid();
            cfg.Duration = TimeSpan.FromSeconds(seconds);
            var ex = Assert.Throws<ArgumentException>(() => cfg.Validate());
            Assert.Contains(nameof(AwsOAuthBearerConfig.Duration), ex.Message);
        }

        [Fact]
        public void Validate_NegativeDuration_Throws()
        {
            var cfg = MinimalValid();
            cfg.Duration = TimeSpan.FromSeconds(-10);
            var ex = Assert.Throws<ArgumentException>(() => cfg.Validate());
            Assert.Contains(nameof(AwsOAuthBearerConfig.Duration), ex.Message);
        }

        // ---- Validate: PrincipalNameOverride ----

        [Fact]
        public void Validate_NullPrincipalNameOverride_Accepted()
        {
            var cfg = MinimalValid();
            cfg.PrincipalNameOverride = null;
            cfg.Validate();
        }

        [Fact]
        public void Validate_NonEmptyPrincipalNameOverride_Accepted()
        {
            var cfg = MinimalValid();
            cfg.PrincipalNameOverride = "explicit-principal";
            cfg.Validate();
        }

        [Fact]
        public void Validate_EmptyPrincipalNameOverride_Throws()
        {
            var cfg = MinimalValid();
            cfg.PrincipalNameOverride = "";
            var ex = Assert.Throws<ArgumentException>(() => cfg.Validate());
            Assert.Contains(nameof(AwsOAuthBearerConfig.PrincipalNameOverride), ex.Message);
        }

        // ---- Validate: idempotent ----

        [Fact]
        public void Validate_CalledTwice_DoesNotMutate()
        {
            var cfg = MinimalValid();
            cfg.SigningAlgorithm = null;
            cfg.Validate();
            cfg.Validate();
            // SigningAlgorithm must still be null — Validate never fills defaults.
            Assert.Null(cfg.SigningAlgorithm);
            Assert.Equal(default, cfg.Duration);
        }

        // ---- ApplyDefaults ----

        [Fact]
        public void ApplyDefaults_NullSigningAlgorithm_BecomesES384()
        {
            var cfg = MinimalValid();
            cfg.SigningAlgorithm = null;
            cfg.ApplyDefaults();
            Assert.Equal("ES384", cfg.SigningAlgorithm);
        }

        [Fact]
        public void ApplyDefaults_DefaultDuration_Becomes300Seconds()
        {
            var cfg = MinimalValid();
            cfg.Duration = default;
            cfg.ApplyDefaults();
            Assert.Equal(TimeSpan.FromSeconds(300), cfg.Duration);
        }

        [Fact]
        public void ApplyDefaults_ExplicitSigningAlgorithm_Unchanged()
        {
            var cfg = MinimalValid();
            cfg.SigningAlgorithm = "RS256";
            cfg.ApplyDefaults();
            Assert.Equal("RS256", cfg.SigningAlgorithm);
        }

        [Fact]
        public void ApplyDefaults_ExplicitDuration_Unchanged()
        {
            var cfg = MinimalValid();
            cfg.Duration = TimeSpan.FromMinutes(15);
            cfg.ApplyDefaults();
            Assert.Equal(TimeSpan.FromMinutes(15), cfg.Duration);
        }

        [Fact]
        public void ApplyDefaults_CalledTwice_Idempotent()
        {
            var cfg = MinimalValid();
            cfg.ApplyDefaults();
            var algAfterFirst = cfg.SigningAlgorithm;
            var durAfterFirst = cfg.Duration;
            cfg.ApplyDefaults();
            Assert.Equal(algAfterFirst, cfg.SigningAlgorithm);
            Assert.Equal(durAfterFirst, cfg.Duration);
        }

        // ---- Optional fields pass through untouched ----

        [Fact]
        public void OptionalFields_SurviveValidateAndApplyDefaults()
        {
            var exts = new Dictionary<string, string> { ["k"] = "v" };
            var cfg = new AwsOAuthBearerConfig
            {
                Region = "us-east-1",
                Audience = "https://a",
                StsEndpointOverride = "https://vpce-abc.sts.us-east-1.vpce.amazonaws.com",
                SaslExtensions = exts,
                PrincipalNameOverride = "explicit",
            };
            cfg.Validate();
            cfg.ApplyDefaults();
            Assert.Equal("https://vpce-abc.sts.us-east-1.vpce.amazonaws.com",
                cfg.StsEndpointOverride);
            Assert.Same(exts, cfg.SaslExtensions);
            Assert.Equal("explicit", cfg.PrincipalNameOverride);
        }

        // ---- Helper ----

        private static AwsOAuthBearerConfig MinimalValid()
            => new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
    }
}
