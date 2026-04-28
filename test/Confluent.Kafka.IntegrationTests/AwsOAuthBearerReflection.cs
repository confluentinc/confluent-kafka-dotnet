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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Internal.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     R5 real-AWS integration tests for the OAUTHBEARER reflection autowire
    ///     path. Gated by <c>RUN_AWS_STS_REAL=1</c>; off by default in CI. Run
    ///     manually before each release on the EC2 test box that has the
    ///     <c>ktrue-iam-sts-test-role</c> attached (region <c>eu-north-1</c>).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         These tests prove the reflection shim mints a real JWT against
    ///         live AWS STS and that the JWT's <c>sub</c> claim resolves to the
    ///         expected role ARN. They do NOT exercise broker auth — that
    ///         requires a Confluent Cloud cluster and is verified by the manual
    ///         release smoke-test, not by this suite.
    ///     </para>
    ///     <para>
    ///         Pre-requisites for the runner:
    ///         <list type="bullet">
    ///             <item><description>Running on EC2 (or any host with valid AWS credentials).</description></item>
    ///             <item><description>Default credential chain resolves a role with <c>sts:GetWebIdentityToken</c>.</description></item>
    ///             <item><description><c>aws iam enable-outbound-web-identity-federation</c> has been run on the account.</description></item>
    ///         </list>
    ///     </para>
    /// </remarks>
    public class AwsOAuthBearerReflectionTests
    {
        private const string EnvGate = "RUN_AWS_STS_REAL";
        private const string TestRegion = "eu-north-1";
        private const string TestAudience = "https://confluent.cloud/oidc";
        private const string ExpectedRoleSubstring = "ktrue-iam-sts-test-role";

        [SkippableFact]
        public async Task ReflectionShim_MintsRealJwt_OnEc2()
        {
            Skip.IfNot(string.Equals(Environment.GetEnvironmentVariable(EnvGate), "1"),
                $"Set {EnvGate}=1 to run this real-AWS integration test on the EC2 box.");

            var shim = StsReflectionShim.LoadOrThrow();
            var stsClient = shim.CreateStsClient(TestRegion, stsEndpointOverride: null);

            var result = await shim.GetWebIdentityTokenAsync(
                stsClient,
                audience: TestAudience,
                signingAlgorithm: "ES384",
                durationSeconds: 60,
                cancellationToken: CancellationToken.None);

            Assert.False(string.IsNullOrEmpty(result.Jwt), "STS returned an empty JWT.");
            Assert.True(result.Expiration > DateTime.UtcNow,
                $"STS returned an already-expired token (Expiration={result.Expiration:O}).");
            Assert.True(result.Expiration < DateTime.UtcNow.AddMinutes(2),
                $"STS returned a token with surprisingly long lifetime (Expiration={result.Expiration:O}); requested 60s.");

            // JWT compact serialisation: 3 base64url segments separated by '.'.
            var segments = result.Jwt.Split('.');
            Assert.Equal(3, segments.Length);

            var sub = JwtSubjectExtractor.ExtractSub(result.Jwt);
            Assert.Contains(ExpectedRoleSubstring, sub);
        }

        [SkippableFact]
        public void TryCreateHandler_RealAws_ReturnsHandler()
        {
            // End-to-end through the public dispatch entry point. We can't
            // invoke the returned handler without a real IClient, but we can
            // verify TryCreateHandler resolves shim, parses config, and creates
            // an STS client without throwing.
            Skip.IfNot(string.Equals(Environment.GetEnvironmentVariable(EnvGate), "1"),
                $"Set {EnvGate}=1 to run this real-AWS integration test.");

            var cfg = new[]
            {
                new System.Collections.Generic.KeyValuePair<string, string>(
                    AwsAutoWire.MarkerKey, AwsAutoWire.MarkerValue),
                new System.Collections.Generic.KeyValuePair<string, string>(
                    "sasl.oauthbearer.config",
                    $"region={TestRegion} audience={TestAudience} duration_seconds=60"),
            };

            var handler = AwsAutoWire.TryCreateHandler(cfg);
            Assert.NotNull(handler);
        }
    }
}
