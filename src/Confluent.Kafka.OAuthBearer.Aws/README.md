# Confluent.Kafka.OAuthBearer.Aws

Optional package for [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka/) that
adds AWS IAM-based authentication to Kafka clients via the OAUTHBEARER SASL mechanism. When
enabled, the package mints short-lived JSON Web Tokens via AWS STS's `GetWebIdentityToken`
API and hands them to librdkafka as the SASL bearer credential.

This package is opt-in and config-activated. Adding the `<PackageReference>` and setting
two config keys is enough — no code changes at the integration site for the common case.
Users who don't reference this package see zero AWS dependencies in their dependency graph.

## Quick start

```xml
<ItemGroup>
  <PackageReference Include="Confluent.Kafka" Version="2.15.0" />
  <PackageReference Include="Confluent.Kafka.OAuthBearer.Aws" Version="2.15.0" />
</ItemGroup>
```

### Minimum configuration

Two required keys in `SaslOauthbearerConfig`: `region` and `audience`. Defaults
apply for everything else (300s lifetime, ES384 signing, no extensions, no tags).

```csharp
using Confluent.Kafka;

var cfg = new ConsumerConfig
{
    BootstrapServers = "pkc-xxxx.aws.confluent.cloud:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism    = SaslMechanism.OAuthBearer,
    GroupId          = "my-group",

    SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam,
    SaslOauthbearerConfig = "region=us-east-1 audience=https://confluent.cloud/oidc",
};

using var consumer = new ConsumerBuilder<string, string>(cfg).Build();
consumer.Subscribe("my-topic");
```

### All options

Every supported key in `SaslOauthbearerConfig`. The grammar is whitespace-separated
`key=value` pairs (no quoting, no escaping); see [Configuration](#configuration) for
each key's semantics.

```csharp
using Confluent.Kafka;

var cfg = new ConsumerConfig
{
    BootstrapServers = "pkc-xxxx.aws.confluent.cloud:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism    = SaslMechanism.OAuthBearer,
    GroupId          = "my-group",

    SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam,
    SaslOauthbearerConfig =
        "region=us-east-1 " +
        "audience=https://confluent.cloud/oidc " +
        "duration_seconds=900 " +
        "signing_algorithm=ES384 " +
        "sts_endpoint=https://sts-fips.us-east-1.amazonaws.com " +
        "principal_name=my-explicit-principal " +
        "extension_logicalCluster=lkc-abc " +
        "extension_identityPoolId=pool-xyz " +
        "tag_team=platform " +
        "tag_environment=prod",
};

using var consumer = new ConsumerBuilder<string, string>(cfg).Build();
consumer.Subscribe("my-topic");
```

## Configuration

`SaslOauthbearerConfig` accepts a whitespace-separated list of `key=value` pairs.

### Required

| Key | Description |
|---|---|
| `region` | AWS region for the STS call (e.g. `us-east-1`, `eu-north-1`). Required — no IMDS sniffing or silent default. |
| `audience` | OIDC audience the relying-party broker expects. Must match the IAM role's trust-policy condition. |

### Optional

| Key | Default | Description |
|---|---|---|
| `duration_seconds` | `300` | Requested token lifetime, 60–3600 seconds. |
| `signing_algorithm` | `ES384` | JWT signing algorithm. Either `ES384` or `RS256`. |
| `sts_endpoint` | _(SDK default)_ | Override STS endpoint URL. Use for FIPS (`sts-fips.us-east-1.amazonaws.com`) or VPC endpoints. |
| `principal_name` | _(JWT `sub` claim)_ | Override the OAUTHBEARER principal. Defaults to extracting `sub` from the minted JWT (the role ARN). |
| `extension_<NAME>` | _(none)_ | RFC 7628 §3.1 SASL extensions forwarded to the broker. Repeatable. |
| `tag_<NAME>` | _(none)_ | Custom tag claims added to the minted JWT (max 50). Repeatable. |

## Prerequisites

The IAM role used by your application (instance profile, IRSA, or assumed role) must:

1. Be trusted to call `sts:GetWebIdentityToken` for the requested `audience`.
2. Belong to an AWS account where outbound web identity federation is enabled.

If either is missing, the first token refresh fails and the error surfaces via librdkafka's
OAUTHBEARER error event with the original AWS exception message.

## Common pitfalls

### Don't set `SaslOauthbearerMethod=Oidc`

The AWS path is incompatible with `sasl.oauthbearer.method=oidc`. Setting both engages
librdkafka's OIDC subsystem and bypasses the AWS handler. The package detects the conflict
and throws `InvalidOperationException` at `Build()`.

```csharp
SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,   // ← do NOT combine with AwsIam
```

Leave `SaslOauthbearerMethod` unset (default).

### Don't leave `SaslOauthbearerConfig` populated when removing the marker

`SaslOauthbearerConfig` uses an AWS-specific grammar (`region=...`, `audience=...`).
If you remove the `AwsIam` marker but leave `SaslOauthbearerConfig` populated, librdkafka
falls back to its built-in unsecured-JWT path — which expects a different grammar and
rejects the AWS keys with `Unrecognized sasl.oauthbearer.config beginning at: region=...`.

When disabling AWS authentication, clear *both* the marker and `SaslOauthbearerConfig`.

## Versioning

This package versions in lockstep with `Confluent.Kafka` core. Always reference matching versions.

## AWS SDK requirement

Targets `AWSSDK.SecurityToken >= 3.7.504`. Pulled in transitively when you reference this package.

## Further reading

- [AWS STS GetWebIdentityToken API reference](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetWebIdentityToken.html)
- [RFC 7628 — SASL OAUTHBEARER](https://www.rfc-editor.org/rfc/rfc7628)
