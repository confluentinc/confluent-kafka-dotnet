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
using Confluent.Kafka.OAuthBearer.Aws.Internal;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    /// <summary>
    ///     End-to-end integration tests against the live AWS STS service.
    ///     Skipped unless <c>RUN_AWS_STS_REAL=1</c>. Run on an EC2 instance
    ///     (or Lambda) with a role granting <c>sts:GetWebIdentityToken</c>
    ///     for the configured audience. Outbound web identity federation
    ///     must be enabled on the AWS account.
    /// </summary>
    /// <remarks>
    ///     Three env vars control the test:
    ///     <list type="bullet">
    ///       <item><c>AWS_STS_TEST_REGION</c> (default <c>us-east-1</c>)</item>
    ///       <item><c>AWS_STS_TEST_AUDIENCE</c> (default <c>https://api.example.com</c>)</item>
    ///       <item><c>AWS_STS_TEST_SIGNING_ALGORITHM</c> (default <c>ES384</c>; also accepts <c>RS256</c>)</item>
    ///     </list>
    /// </remarks>
    public class AwsRealStsIntegrationTests
    {
        [SkippableFact]
        public void RoundTrip_ProducesValidJwtWithExpectedClaims()
        {
            Skip.IfNot(
                Environment.GetEnvironmentVariable("RUN_AWS_STS_REAL") == "1",
                "Set RUN_AWS_STS_REAL=1 to enable. Requires EC2/Lambda with the right IAM role.");

            // Configurable for whatever role/region/audience/algorithm the test environment uses.
            var region    = Environment.GetEnvironmentVariable("AWS_STS_TEST_REGION")            ?? "us-east-1";
            var audience  = Environment.GetEnvironmentVariable("AWS_STS_TEST_AUDIENCE")          ?? "https://api.example.com";
            var algorithm = Environment.GetEnvironmentVariable("AWS_STS_TEST_SIGNING_ALGORITHM") ?? "ES384";

            // duration_seconds=300 explicit — matches the role's permission-policy cap
            // (NumericLessThanEquals on sts:DurationSeconds).
            var cfg = AwsOAuthBearerConfig.Parse(
                $"region={region} audience={audience} duration_seconds=300 signing_algorithm={algorithm}");

            using var provider = new AwsStsTokenProvider(cfg);
            var sink = new RecordingSink();

            // Exercise the full handler chain (sync-bridge → STS round-trip → JWT parse → handoff).
            AwsOAuthBearerHandler.Invoke(provider, sink);

            // 1. No failure routed to sink.
            Assert.Empty(sink.FailureCalls);

            // 2. Exactly one successful SetToken call.
            Assert.Single(sink.SetCalls);
            var call = sink.SetCalls[0];

            // 3. JWT shape: present and within an order-of-magnitude of expected.
            Assert.False(string.IsNullOrEmpty(call.TokenValue));
            Assert.InRange(call.TokenValue.Length, 800, 4096);

            // 4. Principal name = JWT 'sub' claim = role ARN.
            Assert.StartsWith("arn:aws:", call.PrincipalName);

            // 5. Lifetime is in the near future. With duration_seconds=300, expect
            //    lifetimeMs ≈ now + 300s. Upper bound 10min is generous for clock skew.
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Assert.InRange(call.LifetimeMs, nowMs, nowMs + (10 * 60 * 1000));

            // 6. JWT header carries the algorithm we requested. Proves the SDK
            //    round-trip preserved the choice end-to-end.
            var parts = call.TokenValue.Split('.');
            var headerJson = JObject.Parse(
                Encoding.UTF8.GetString(AwsTestHelpers.Base64UrlDecode(parts[0])));
            Assert.Equal(algorithm, headerJson["alg"].Value<string>());

            // 7. JWT payload 'aud' matches what we asked AWS for. Defensive —
            //    catches any audience-shaping surprises (AWS shouldn't munge).
            var payloadJson = JObject.Parse(
                Encoding.UTF8.GetString(AwsTestHelpers.Base64UrlDecode(parts[1])));
            Assert.Equal(audience, payloadJson["aud"].Value<string>());

            // Diagnostic output — captured when running with --logger "console;verbosity=detailed"
            Console.WriteLine(
                $"JWT length={call.TokenValue.Length}, alg={algorithm}, " +
                $"principal={call.PrincipalName}, " +
                $"lifetimeMs={call.LifetimeMs} (now={nowMs}, ttl={call.LifetimeMs - nowMs}ms)");
        }
    }
}
