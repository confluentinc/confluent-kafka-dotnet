# Running the real-AWS integration test

Most tests in this project are pure unit tests — no network, no AWS credentials, no configuration. They run by default under `make test` / CI and finish in milliseconds.

One test in [AwsStsTokenProviderRealTests.cs](AwsStsTokenProviderRealTests.cs) is different: it calls AWS STS for real to verify the full path from the default credential chain through `GetWebIdentityToken` to a parsed `AwsOAuthBearerToken`. It is gated on an environment variable and skips by default.

## Running it

```bash
# Run from the repo root.
RUN_AWS_STS_REAL=1 \
  AWS_REGION=eu-north-1 \
  AUDIENCE=https://api.example.com \
  dotnet test test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/Confluent.Kafka.OAuthBearer.Aws.UnitTests.csproj \
  -c Release \
  --filter "FullyQualifiedName~AwsStsTokenProviderRealTests"
```

`AWS_REGION` and `AUDIENCE` both default to plausible test values (`eu-north-1` and `https://api.example.com`) if unset, so `RUN_AWS_STS_REAL=1` alone is the minimum to opt in.

## Prerequisites

All of these are administrative steps outside this repo's control.

### 1. Account-level enablement

The AWS account must have run the one-time enablement action:

```bash
aws iam enable-outbound-web-identity-federation
```

Without this, the first `GetWebIdentityToken` call returns `OutboundWebIdentityFederationDisabled` and the test fails with that error code. See the AWS IAM documentation for "Outbound Identity Federation" (GA 2025-11-19).

### 2. Caller IAM permission

The IAM identity the test runs as must have this permission:

```json
{
  "Effect": "Allow",
  "Action": "sts:GetWebIdentityToken",
  "Resource": "*"
}
```

Without it, the call returns `AccessDenied`.

### 3. Reachable credentials

The AWS SDK's default credential chain (`FallbackCredentialsFactory`) must resolve. On an EC2 instance, attaching an IAM role to the instance is sufficient — IMDSv2 is probed automatically. Other supported sources: environment variables, `AWS_WEB_IDENTITY_TOKEN_FILE` (EKS IRSA), the ECS/Pod-Identity credential endpoint, `~/.aws/credentials`, or an SSO cache.

Set `AWS_REGION` explicitly if running outside an environment that injects it (most developer laptops).

### 4. Network reachability

The host must be able to reach the regional STS endpoint `sts.<region>.amazonaws.com` over HTTPS. VPC-only hosts need an STS interface endpoint or a route to the public internet. For FIPS endpoints, set `AwsOAuthBearerConfig.StsEndpointOverride` in the test (or change the test to read it from an env var).

## Recommended test environment

The librdkafka-side and Go-side work on the same feature standardized on an EC2 box in `eu-north-1` under the IAM role `ktrue-iam-sts-test-role` (account 708975691912). Using the same box for this test keeps the cross-client evidence comparable and avoids re-enabling federation in a new account.

## What this test does and does not prove

**It proves:** the client-side code path — `FallbackCredentialsFactory` → `AmazonSecurityTokenServiceClient.GetWebIdentityTokenAsync` → JWT sub extraction → `AwsOAuthBearerToken` — works end-to-end against real AWS.

**It does not prove:** the minted token is accepted by a Kafka broker. That requires the broker-side OIDC trust configuration (JWKS URL, audience matching, issuer mapping), which is an admin action outside this test's scope. Broker acceptance is the manual E2E step the owner drives separately.

## CI status

This test is `[SkippableFact]`-gated. `make test` and the Semaphore PR pipeline run with `RUN_AWS_STS_REAL` unset, so the test is skipped (reported green by xUnit, zero AWS side effects). Do not enable it in CI without wiring up a trusted credential source — setting `RUN_AWS_STS_REAL=1` on a host with no credentials will turn the test red.
