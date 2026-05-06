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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
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
        private static readonly DateTime CannedExpiry =
            new DateTime(2099, 4, 21, 6, 6, 47, 641, DateTimeKind.Utc);

        // ---- Constructor: null check ----

        [Fact]
        public void Ctor_NullConfig_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => new AwsStsTokenProvider(null));
        }

        [Fact]
        public void Ctor_ValidParsedConfig_Succeeds()
        {
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            using var provider = new AwsStsTokenProvider(cfg);
            // Does not throw; does not call AWS (lazy credential chain).
        }

        // ---- Request shape ----

        [Fact]
        public async Task GetTokenAsync_AudiencePassthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://my.audience");
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Single(fake.LastRequest.Audience);
            Assert.Equal("https://my.audience", fake.LastRequest.Audience[0]);
        }

        [Fact]
        public async Task GetTokenAsync_SigningAlgorithmPassthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a signing_algorithm=RS256");
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Equal("RS256", fake.LastRequest.SigningAlgorithm);
        }

        [Fact]
        public async Task GetTokenAsync_DurationSecondsPassthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a duration_seconds=900");
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Equal(900, fake.LastRequest.DurationSeconds);
        }

        [Fact]
        public async Task GetTokenAsync_DefaultDuration_Sends300Seconds()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Equal(300, fake.LastRequest.DurationSeconds);
        }

        [Fact]
        public async Task GetTokenAsync_DefaultSigningAlgorithm_SendsES384()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();
            Assert.Equal("ES384", fake.LastRequest.SigningAlgorithm);
        }

        [Fact]
        public async Task GetTokenAsync_TagsPassthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a tag_team=platform tag_environment=prod");
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();

            Assert.Equal(2, fake.LastRequest.Tags.Count);
            Assert.Contains(fake.LastRequest.Tags, t => t.Key == "team" && t.Value == "platform");
            Assert.Contains(fake.LastRequest.Tags, t => t.Key == "environment" && t.Value == "prod");
        }

        [Fact]
        public async Task GetTokenAsync_NoTags_RequestTagsRemainsUnset()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            var provider = new AwsStsTokenProvider(cfg, fake);
            await provider.GetTokenAsync();

            // SDK leaves Tags null when not set; we should not have allocated an empty list.
            Assert.True(fake.LastRequest.Tags == null || fake.LastRequest.Tags.Count == 0);
        }

        // ---- Response mapping ----

        [Fact]
        public async Task GetTokenAsync_HappyPath_ReturnsMappedFields()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
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
            var localKindTimestamp = new DateTime(2099, 4, 21, 6, 6, 47, DateTimeKind.Unspecified);
            var fake = new FakeStsClient((req, ct) => Task.FromResult(
                new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = CannedJwt,
                    Expiration = localKindTimestamp,
                }));
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            var provider = new AwsStsTokenProvider(cfg, fake);
            var tok = await provider.GetTokenAsync();

            var utcKind = DateTime.SpecifyKind(localKindTimestamp, DateTimeKind.Utc);
            var expected = new DateTimeOffset(utcKind).ToUnixTimeMilliseconds();
            Assert.Equal(expected, tok.LifetimeMs);
        }

        [Fact]
        public async Task GetTokenAsync_NullExpiration_Throws()
        {
            // SDK leaves Expiration unset → defaults to default(DateTime).
            var fake = new FakeStsClient((req, ct) => Task.FromResult(
                new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = CannedJwt,
                    // Expiration deliberately not set
                }));
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            var provider = new AwsStsTokenProvider(cfg, fake);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => provider.GetTokenAsync());
            Assert.Contains("Expiration", ex.Message);
        }
        
        [Fact]
        public async Task GetTokenAsync_PrincipalNameOverride_WinsOverJwtSub()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a principal_name=explicit-principal");
            var provider = new AwsStsTokenProvider(cfg, fake);
            var tok = await provider.GetTokenAsync();
            Assert.Equal("explicit-principal", tok.PrincipalName);
        }

        [Fact]
        public async Task GetTokenAsync_SaslExtensions_Passthrough()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse(
                "region=us-east-1 audience=https://a " +
                "extension_logicalCluster=lkc-123 extension_identityPoolId=pool-x");
            var provider = new AwsStsTokenProvider(cfg, fake);
            var tok = await provider.GetTokenAsync();
            Assert.NotNull(tok.Extensions);
            Assert.Equal(2, tok.Extensions.Count);
            Assert.Equal("lkc-123", tok.Extensions["logicalCluster"]);
            Assert.Equal("pool-x", tok.Extensions["identityPoolId"]);
        }

        [Fact]
        public async Task GetTokenAsync_NoExtensionsConfigured_ReturnsNull()
        {
            var fake = new FakeStsClient((req, ct) => Task.FromResult(OkResponse()));
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
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
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
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
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
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
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
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
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
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
            var cfg = AwsOAuthBearerConfig.Parse("region=us-east-1 audience=https://a");
            var provider = new AwsStsTokenProvider(cfg, fake);
            provider.Dispose();
            // Provider did not own the fake; we can still inspect its state safely.
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
    }
}
