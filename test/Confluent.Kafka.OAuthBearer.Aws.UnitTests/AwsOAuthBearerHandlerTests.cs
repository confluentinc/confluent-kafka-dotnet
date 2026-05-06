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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    public class AwsOAuthBearerHandlerTests
    {
        private const string RoleArn = "arn:aws:iam::123:role/R";
        private static readonly string CannedJwt = MakeJwt($"{{\"sub\":\"{RoleArn}\"}}");
        private static readonly DateTime CannedExpiry =
            new DateTime(2099, 4, 21, 6, 6, 47, 641, DateTimeKind.Utc);

        [Fact]
        public void Create_NullProvider_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => AwsOAuthBearerHandler.Create(null));
        }

        // ---- Invoke: success path ----

        [Fact]
        public void Invoke_HappyPath_CallsSetTokenWithMappedFields()
        {
            var provider = NewProvider((req, ct) => Task.FromResult(
                new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = CannedJwt, Expiration = CannedExpiry,
                }));
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);

            Assert.Single(sink.SetCalls);
            Assert.Empty(sink.FailureCalls);
            var call = sink.SetCalls[0];
            Assert.Equal(CannedJwt, call.TokenValue);
            Assert.Equal(RoleArn, call.PrincipalName);
            Assert.Equal(
                new DateTimeOffset(CannedExpiry).ToUnixTimeMilliseconds(),
                call.LifetimeMs);
        }

        [Fact]
        public void Invoke_ExtensionsPassedThroughFromConfig()
        {
            var provider = NewProvider(
                (req, ct) => Task.FromResult(
                    new GetWebIdentityTokenResponse
                    {
                        WebIdentityToken = CannedJwt, Expiration = CannedExpiry,
                    }),
                "region=us-east-1 audience=https://a extension_logicalCluster=lkc-1");
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);

            Assert.Single(sink.SetCalls);
            Assert.NotNull(sink.SetCalls[0].Extensions);
            Assert.Equal("lkc-1", sink.SetCalls[0].Extensions["logicalCluster"]);
        }

        [Fact]
        public void Invoke_PrincipalNameOverride_PropagatedToSink()
        {
            var provider = NewProvider(
                (req, ct) => Task.FromResult(
                    new GetWebIdentityTokenResponse
                    {
                        WebIdentityToken = CannedJwt, Expiration = CannedExpiry,
                    }),
                "region=us-east-1 audience=https://a principal_name=explicit-principal");
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);

            Assert.Single(sink.SetCalls);
            Assert.Equal("explicit-principal", sink.SetCalls[0].PrincipalName);
        }

        // ---- Invoke: failure paths — every exception must land in SetTokenFailure ----

        [Fact]
        public void Invoke_AwsStsException_RoutedToSetTokenFailure()
        {
            // AmazonSecurityTokenServiceException.ToString() emits
            // "TypeName: Message" — it does NOT include ErrorCode in the
            // rendered text, even if set. Assert on what actually appears
            // in ex.ToString(), which is what the handler hands to
            // OAuthBearerSetTokenFailure.
            var provider = NewProvider((req, ct) =>
                throw new AmazonSecurityTokenServiceException(
                    "User is not authorized to perform: sts:GetWebIdentityToken")
                { ErrorCode = "AccessDenied" });
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);

            Assert.Empty(sink.SetCalls);
            Assert.Single(sink.FailureCalls);
            Assert.Contains("AmazonSecurityTokenServiceException", sink.FailureCalls[0]);
            Assert.Contains("is not authorized", sink.FailureCalls[0]);
        }

        [Fact]
        public void Invoke_OutboundFederationDisabled_RoutedToSetTokenFailure()
        {
            var provider = NewProvider((req, ct) =>
                throw new OutboundWebIdentityFederationDisabledException(
                    "OutboundWebIdentityFederation is not enabled on this account."));
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);

            Assert.Empty(sink.SetCalls);
            Assert.Single(sink.FailureCalls);
            Assert.Contains("OutboundWebIdentityFederationDisabled", sink.FailureCalls[0]);
        }

        [Fact]
        public void Invoke_MalformedJwtResponse_RoutedToSetTokenFailure()
        {
            var provider = NewProvider((req, ct) => Task.FromResult(
                new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = "not-a-jwt",
                    Expiration = CannedExpiry,
                }));
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);

            Assert.Empty(sink.SetCalls);
            Assert.Single(sink.FailureCalls);
            Assert.Contains("FormatException", sink.FailureCalls[0]);
        }

        [Fact]
        public void Invoke_UnexpectedException_RoutedToSetTokenFailure()
        {
            var provider = NewProvider((req, ct) =>
                throw new InvalidOperationException("invalid operation"));
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);

            Assert.Empty(sink.SetCalls);
            Assert.Single(sink.FailureCalls);
            Assert.Contains("invalid operation", sink.FailureCalls[0]);
        }

        [Fact]
        public void Invoke_DoesNotThrow()
        {
            // The handler must never propagate exceptions — librdkafka would
            // swallow them without retry, leaving the client in a stuck state.
            var provider = NewProvider((req, ct) =>
                throw new Exception("anything"));
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink); // must not throw
            Assert.Single(sink.FailureCalls);
        }

        // ---- Mutual exclusion: exactly one of Set/Fail per invocation ----

        [Fact]
        public void Invoke_Success_DoesNotCallFailure()
        {
            var provider = NewProvider((req, ct) => Task.FromResult(
                new GetWebIdentityTokenResponse
                {
                    WebIdentityToken = CannedJwt, Expiration = CannedExpiry,
                }));
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);
            Assert.Empty(sink.FailureCalls);
        }

        [Fact]
        public void Invoke_Failure_DoesNotCallSet()
        {
            var provider = NewProvider((req, ct) =>
                throw new Exception("boom"));
            var sink = new RecordingSink();

            AwsOAuthBearerHandler.Invoke(provider, sink);
            Assert.Empty(sink.SetCalls);
        }

        // ---- Helpers ----

        private static AwsStsTokenProvider NewProvider(
            Func<GetWebIdentityTokenRequest, CancellationToken,
                 Task<GetWebIdentityTokenResponse>> responder,
            string configString = "region=us-east-1 audience=https://a")
        {
            var cfg = AwsOAuthBearerConfig.Parse(configString);
            return new AwsStsTokenProvider(cfg, new FakeStsClient(responder));
        }

        private static string MakeJwt(string payloadJson)
        {
            string b64url(byte[] b) =>
                Convert.ToBase64String(b).TrimEnd('=').Replace('+', '-').Replace('/', '_');
            var h = b64url(Encoding.UTF8.GetBytes("{\"alg\":\"ES384\"}"));
            var p = b64url(Encoding.UTF8.GetBytes(payloadJson));
            return $"{h}.{p}.sig";
        }

        private sealed class RecordingSink : ITokenSink
        {
            public readonly List<(string TokenValue, long LifetimeMs, string PrincipalName,
                                  IDictionary<string, string> Extensions)> SetCalls
                = new List<(string, long, string, IDictionary<string, string>)>();
            public readonly List<string> FailureCalls = new List<string>();

            public void SetToken(string tokenValue, long lifetimeMs, string principalName,
                IDictionary<string, string> extensions)
                => SetCalls.Add((tokenValue, lifetimeMs, principalName, extensions));

            public void SetTokenFailure(string error) => FailureCalls.Add(error);
        }
    }
}
