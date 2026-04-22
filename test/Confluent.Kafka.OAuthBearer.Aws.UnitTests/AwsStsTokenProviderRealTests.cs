using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Confluent.Kafka.OAuthBearer.Aws;
using Xunit;

namespace Confluent.Kafka.OAuthBearer.Aws.UnitTests
{
    /// <summary>
    ///     Real-AWS integration test. Skipped by default; opt in by running
    ///     on a host whose default credential chain resolves successfully
    ///     (EC2 with an instance role, or equivalent) and setting the
    ///     <c>RUN_AWS_STS_REAL</c> environment variable to <c>1</c>.
    /// </summary>
    /// <remarks>
    ///     See <c>test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/TESTING.md</c>
    ///     for the exact reproduce recipe (role permissions, account-level
    ///     enablement, run command). This test only verifies the client can
    ///     mint a token — broker-side acceptance of the minted JWT is out of
    ///     scope.
    /// </remarks>
    public class AwsStsTokenProviderRealTests
    {
        [SkippableFact]
        public async Task GetTokenAsync_RealSts_MintsValidJwt()
        {
            Skip.IfNot(
                Environment.GetEnvironmentVariable("RUN_AWS_STS_REAL") == "1",
                "Set RUN_AWS_STS_REAL=1 and provide AWS credentials to run.");

            // All AWS-side construction happens after the skip so non-opted
            // hosts never load credentials or touch the network.
            var cfg = new AwsOAuthBearerConfig
            {
                Region = Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-north-1",
                Audience = Environment.GetEnvironmentVariable("AUDIENCE")
                           ?? "https://api.example.com",
                Duration = TimeSpan.FromMinutes(5),
            };

            using var provider = new AwsStsTokenProvider(cfg);
            var tok = await provider.GetTokenAsync();

            // Three-segment compact JWT: header.payload.signature.
            Assert.Matches(
                @"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$",
                tok.TokenValue);

            // Must expire in the future; should also be within the requested
            // 5-minute window plus/minus AWS's clock skew tolerance.
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var windowMs = (long)TimeSpan.FromMinutes(10).TotalMilliseconds;
            Assert.InRange(tok.LifetimeMs, nowMs, nowMs + windowMs);

            // Live STS behaviour: "sub" is the bare role ARN or assumed-role ARN.
            Assert.Matches(
                @"^arn:aws:(iam|sts)::\d+:(role|assumed-role)/.+$",
                tok.PrincipalName);
        }
    }
}
