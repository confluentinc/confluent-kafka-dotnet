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
using Confluent.Kafka.OAuthBearer.Aws.Internal;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    /// <summary>
    ///     Live AWS STS round-trip test. Skipped unless RUN_AWS_STS_REAL=1.
    ///     Run on an EC2 instance (or Lambda) with a role granting
    ///     sts:GetWebIdentityToken for the configured audience. Outbound web
    ///     identity federation must be enabled on the AWS account.
    /// </summary>
    public class AwsAutoWireRealStsTests
    {
        [SkippableFact]
        public void Path_A_RealSts_MintsValidJwtAndPrincipal()
        {
            Skip.IfNot(
                Environment.GetEnvironmentVariable("RUN_AWS_STS_REAL") == "1",
                "Set RUN_AWS_STS_REAL=1 to enable. Requires EC2/Lambda with the right IAM role.");

            // Configurable for whatever role/region/audience the test environment uses.
            var region   = Environment.GetEnvironmentVariable("AWS_STS_TEST_REGION")   ?? "us-east-1";
            var audience = Environment.GetEnvironmentVariable("AWS_STS_TEST_AUDIENCE") ?? "https://api.example.com";

            // duration_seconds=300 explicit — matches the role's permission-policy cap
            // (NumericLessThanEquals on sts:DurationSeconds).
            var cfg = AwsOAuthBearerConfig.Parse(
                $"region={region} audience={audience} duration_seconds=300");

            using var provider = new AwsStsTokenProvider(cfg);
            var sink = new RecordingSink();

            // Sync-bridge invocation — exactly what Path A does at refresh time.
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

            // Diagnostic output — captured when running with --logger "console;verbosity=detailed"
            Console.WriteLine(
                $"[H6] JWT length={call.TokenValue.Length}, " +
                $"principal={call.PrincipalName}, " +
                $"lifetimeMs={call.LifetimeMs} (now={nowMs}, ttl={call.LifetimeMs - nowMs}ms)");
        }

        // Local copy — RecordingSink lives privately in AwsOAuthBearerHandlerTests.
        // Duplicating ~10 lines is cheaper than promoting it to a shared helper for one extra use.
        private sealed class RecordingSink : ITokenSink
        {
            public readonly List<(string TokenValue, long LifetimeMs, string PrincipalName, IDictionary<string,string> Extensions)>
                SetCalls = new();
            public readonly List<string> FailureCalls = new();

            public void SetToken(string tv, long ms, string p, IDictionary<string,string> ext)
                => SetCalls.Add((tv, ms, p, ext));
            public void SetTokenFailure(string error) => FailureCalls.Add(error);
        }
    }
}
