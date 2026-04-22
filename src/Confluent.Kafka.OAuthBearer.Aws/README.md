# Confluent.Kafka.OAuthBearer.Aws

Optional AWS STS `GetWebIdentityToken` OAUTHBEARER token provider for `Confluent.Kafka`. Installing this package gives .NET clients running on AWS (EC2, EKS with IRSA or Pod Identity, ECS, Fargate, Lambda) a one-line way to authenticate to OIDC-gated Kafka endpoints — such as Confluent Cloud clusters configured for AWS IAM Outbound Identity Federation — using their AWS identity instead of a long-lived secret.

Users who don't install this package see **zero change** to their `Confluent.Kafka` dependency graph. The AWS SDK is pulled in only when this package is referenced.

## Install

```
dotnet add package Confluent.Kafka.OAuthBearer.Aws
```

Requires:

- `Confluent.Kafka` (same version; the package is released in lockstep).
- `AWSSDK.SecurityToken` >= **3.7.504** — this is the first NuGet release that actually contains `GetWebIdentityToken`. Earlier versions (3.7.503.x) exist on NuGet but predate the feature despite what their version strings suggest. This package pins the correct floor for you.

## Usage

```csharp
using Confluent.Kafka;
using Confluent.Kafka.OAuthBearer.Aws;

var kafkaConfig = new ConsumerConfig
{
    BootstrapServers = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism    = SaslMechanism.OAuthBearer,
    GroupId          = "my-group",
    // NOTE: do NOT set SaslOauthbearerMethod — that selects the
    // librdkafka-native OAuth path and bypasses this managed provider.
};

var awsConfig = new AwsOAuthBearerConfig
{
    Region   = "us-east-1",
    Audience = "https://confluent.cloud/oidc",
    Duration = TimeSpan.FromHours(1),
};

using var consumer = new ConsumerBuilder<string, string>(kafkaConfig)
    .UseAwsOAuthBearer(awsConfig)
    .Build();
```

The same `UseAwsOAuthBearer(...)` extension is available on `ProducerBuilder<,>`, `ConsumerBuilder<,>`, and `AdminClientBuilder`. An overload accepting a pre-built `AwsStsTokenProvider` lets you share one provider across multiple clients and dispose it deterministically:

```csharp
using var provider = new AwsStsTokenProvider(awsConfig);

using var producer = new ProducerBuilder<Null, string>(kafkaConfig)
    .UseAwsOAuthBearer(provider).Build();

using var consumer = new ConsumerBuilder<string, string>(kafkaConfig)
    .UseAwsOAuthBearer(provider).Build();
```

## Credential resolution

`AwsOAuthBearerConfig.Credentials` is optional. When left `null`, the AWS SDK's default credential chain is used (env vars → assume-role-with-web-identity → ECS container credentials → IMDSv2 → profile). This is resolved **lazily** on the first token fetch, so constructor-time misconfiguration of the credential chain surfaces at first refresh rather than at `builder.Build()`.

## Prerequisites (administrative, not client-side)

- The AWS account has run `aws iam enable-outbound-web-identity-federation` once.
- The IAM identity the client runs under has `sts:GetWebIdentityToken` permission.
- The broker trusts the AWS-hosted JWKS endpoint for this account's issuer.

If any of these is missing, the first token refresh returns a failure visible in the Kafka client's error log (e.g. `OutboundWebIdentityFederationDisabled`, `AccessDenied`). The client does not retry misconfiguration — fix the IAM / broker side.

## Don't set `SaslOauthbearerMethod`

`Confluent.Kafka` also supports a **librdkafka-native** OAUTHBEARER path selected via the `SaslOauthbearerMethod` config property (e.g. `Oidc`, `AzureIMDS`). That path is for users who prefer librdkafka to fetch tokens itself, and it bypasses the managed refresh handler this package installs.

If `SaslOauthbearerMethod` is set, `UseAwsOAuthBearer(...)` has no effect. Leave it unset (the default) when using this package.

## Reference

- Design doc: [DESIGN_AWS_OAUTHBEARER.md](https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/DESIGN_AWS_OAUTHBEARER.md)
- Implementation plan: [IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md](https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md)
- AWS IAM Outbound Identity Federation announcement (2025-11-19).
