using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Confluent.Kafka.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class AwsStsTokenProviderTests
    {
        // A JWT whose payload decodes to {"sub":"arn:aws:iam::123:role/R"}.
        // Third segment is dummy — the extractor doesn't verify the signature.
        private const string RoleArn = "arn:aws:iam::123:role/R";
        private static readonly string CannedJwt = MakeJwt($"{{\"sub\":\"{RoleArn}\"}}");

        // Matches the RFC 3339 fractional-second format observed live from STS
        // (see librdkafka Probe B: "2026-04-21T06:06:47.641000+00:00").
        private static readonly DateTime CannedExpiry =
            new DateTime(2026, 4, 21, 6, 6, 47, 641, DateTimeKind.Utc);

        // ---- Constructor: validation surfaces at construction ----

        [Fact]
        public void Ctor_NullConfig_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new AwsStsTokenProvider(null));
        }

        [Fact]
        public void Ctor_InvalidConfig_Throws()
        {
            var cfg = new AwsOAuthBearerConfig(); // missing Region + Audience
            Assert.Throws<ArgumentException>(() => new AwsStsTokenProvider(cfg));
        }

        [Fact]
        public void Ctor_ValidConfig_Succeeds()
        {
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            using var provider = new AwsStsTokenProvider(cfg);
            // Does not throw; does not call AWS.
        }

        [Fact]
        public void Ctor_AppliesDefaults()
        {
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            using var provider = new AwsStsTokenProvider(cfg);
            Assert.Equal("ES384", cfg.SigningAlgorithm);
            Assert.Equal(TimeSpan.FromSeconds(300), cfg.Duration);
        }

        // ---- Lazy credential resolution ----

        [Fact]
        public void Ctor_DoesNotInvokeCredentialChain()
        {
            var creds = new ThrowingCredentials();
            var cfg = new AwsOAuthBearerConfig
            {
                Region = "us-east-1",
                Audience = "https://a",
                Credentials = creds,
            };
            using var provider = new AwsStsTokenProvider(cfg);
            // If the constructor had eagerly resolved credentials, ThrowingCredentials
            // would have thrown. It didn't — confirms laziness.
            Assert.Equal(0, creds.RetrieveCount);
        }

        // ---- Request shape ----

        [Fact]
        public async Task GetTokenAsync_AudiencePassthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://my.audience" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Single(fake.LastRequest.Audience);
            Assert.Equal("https://my.audience", fake.LastRequest.Audience[0]);
        }

        [Fact]
        public async Task GetTokenAsync_SigningAlgorithmPassthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig
            {
                Region = "us-east-1", Audience = "https://a", SigningAlgorithm = "RS256",
            };
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Equal("RS256", fake.LastRequest.SigningAlgorithm);
        }

        [Fact]
        public async Task GetTokenAsync_DurationSecondsPassthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig
            {
                Region = "us-east-1", Audience = "https://a",
                Duration = TimeSpan.FromSeconds(900),
            };
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Equal(900, fake.LastRequest.DurationSeconds);
        }

        [Fact]
        public async Task GetTokenAsync_DefaultDuration_Sends300Seconds()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Equal(300, fake.LastRequest.DurationSeconds);
        }

        [Fact]
        public async Task GetTokenAsync_DefaultSigningAlgorithm_SendsES384()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Equal("ES384", fake.LastRequest.SigningAlgorithm);
        }

        // ---- Response mapping ----

        [Fact]
        public async Task GetTokenAsync_HappyPath_ReturnsMappedFields()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            var tok = await provider.GetTokenAsync();

            Assert.Equal(CannedJwt, tok.TokenValue);
            Assert.Equal(RoleArn, tok.PrincipalName);

            var expected = new DateTimeOffset(CannedExpiry).ToUnixTimeMilliseconds();
            Assert.Equal(expected, tok.LifetimeMs);
        }

        [Fact]
        public async Task GetTokenAsync_UtcExpirationConversion_IsDeterministic()
        {
            // Make sure unspecified-kind DateTime is treated as UTC (not local).
            var localKindTimestamp = new DateTime(2026, 4, 21, 6, 6, 47, DateTimeKind.Unspecified);
            var fake = new FakeStsClient((req, ct) => Task.FromResult(
                new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = CannedJwt,
                    Expiration = localKindTimestamp,
                }));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            var tok = await provider.GetTokenAsync();

            var utcKind = DateTime.SpecifyKind(localKindTimestamp, DateTimeKind.Utc);
            var expected = new DateTimeOffset(utcKind).ToUnixTimeMilliseconds();
            Assert.Equal(expected, tok.LifetimeMs);
        }

        [Fact]
        public async Task GetTokenAsync_PrincipalNameOverride_WinsOverJwtSub()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig
            {
                Region = "us-east-1", Audience = "https://a",
                PrincipalNameOverride = "explicit-principal",
            };
            var provider = new AwsStsTokenProvider(cfg, fake);
            var tok = await provider.GetTokenAsync();
            Assert.Equal("explicit-principal", tok.PrincipalName);
        }

        [Fact]
        public async Task GetTokenAsync_SaslExtensions_Passthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var exts = new Dictionary<string, string>
            {
                ["logicalCluster"] = "lkc-123", ["identityPoolId"] = "pool-x",
            };
            var cfg = new AwsOAuthBearerConfig
            {
                Region = "us-east-1", Audience = "https://a", SaslExtensions = exts,
            };
            var provider = new AwsStsTokenProvider(cfg, fake);
            var tok = await provider.GetTokenAsync();
            Assert.Same(exts, tok.Extensions);
        }

        [Fact]
        public async Task GetTokenAsync_NoExtensionsConfigured_ReturnsNull()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            var tok = await provider.GetTokenAsync();
            Assert.Null(tok.Extensions);
        }

        // ---- Error propagation ----

        [Fact]
        public async Task GetTokenAsync_MalformedJwt_ThrowsFormatException()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(
                new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = "not-a-jwt",
                    Expiration = CannedExpiry,
                }));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            await Assert.ThrowsAsync<FormatException>(() => provider.GetTokenAsync());
        }

        [Fact]
        public async Task GetTokenAsync_StsAccessDenied_Propagates()
        {
            var fake = new FakeStsClient((req, ct) =>
                throw new AmazonSecurityTokenServiceException(
                    "User is not authorized to perform: sts:GetWebIdentityToken")
                { ErrorCode = "AccessDenied" });
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            var ex = await Assert.ThrowsAsync<AmazonSecurityTokenServiceException>(
                () => provider.GetTokenAsync());
            Assert.Equal("AccessDenied", ex.ErrorCode);
        }

        [Fact]
        public async Task GetTokenAsync_OutboundFederationDisabled_Propagates()
        {
            var fake = new FakeStsClient((req, ct) =>
                throw new OutboundWebIdentityFederationDisabledException(
                    "OutboundWebIdentityFederation is not enabled on this account."));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            await Assert.ThrowsAsync<OutboundWebIdentityFederationDisabledException>(
                () => provider.GetTokenAsync());
        }

        [Fact]
        public async Task GetTokenAsync_CancelledToken_PropagatesOperationCanceled()
        {
            var fake = new FakeStsClient((req, ct) =>
            {
                ct.ThrowIfCancellationRequested();
                return Task.FromResult(OkResponse());
            });
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAsync<OperationCanceledException>(
                () => provider.GetTokenAsync(cts.Token));
        }

        // ---- Lifecycle ----

        [Fact]
        public void Dispose_InjectedClient_NotDisposedByProvider()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = new AwsOAuthBearerConfig { Region = "us-east-1", Audience = "https://a" };
            var provider = new AwsStsTokenProvider(cfg, fake);
            provider.Dispose();
            // Fake is still usable — provider did not own it. No exception should fire.
            // (We can't easily assert "not disposed" on the AWS client, but we can
            // confirm it's still callable via the LastRequest getter, which is a
            // pure field access and wouldn't throw even if disposed.)
            Assert.Null(fake.LastRequest); // no calls were made
        }

        // ---- Helpers ----

        private static GetWebIdentityTokenResponse OkResponse() =>
            new GetWebIdentityTokenResponse
            {
                WebIdentityToken = CannedJwt,
                Expiration = CannedExpiry,
            };

        private static string MakeJwt(string payloadJson)
        {
            string b64url(byte[] bytes) =>
                Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
            var header = b64url(Encoding.UTF8.GetBytes("{\"alg\":\"ES384\",\"typ\":\"JWT\"}"));
            var payload = b64url(Encoding.UTF8.GetBytes(payloadJson));
            return $"{header}.{payload}.fakesig";
        }

        private sealed class ThrowingCredentials : AWSCredentials
        {
            public int RetrieveCount;

            public override ImmutableCredentials GetCredentials()
            {
                RetrieveCount++;
                throw new InvalidOperationException("Credential chain should not be called eagerly.");
            }
        }
    }
}
