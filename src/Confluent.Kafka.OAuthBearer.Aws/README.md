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
apply for everything else (300s lifetime, ES384 signing, no tags). SASL extensions,
when needed, are set on the typed `SaslOauthbearerExtensions` property (see below).

```csharp
using Confluent.Kafka;

var cfg = new ConsumerConfig
{
    BootstrapServers = "pkc-xxxx.aws.confluent.cloud:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism    = SaslMechanism.OAuthBearer,
    GroupId          = "my-group",

    SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
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

    SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
    SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam,
    SaslOauthbearerConfig =
        "region=us-east-1 " +
        "audience=https://confluent.cloud/oidc " +
        "duration_seconds=900 " +
        "signing_algorithm=ES384 " +
        "sts_endpoint=https://sts-fips.us-east-1.amazonaws.com " +
        "principal_name=my-explicit-principal " +
        "aws_debug=console " +
        "tag_team=platform " +
        "tag_environment=prod",
    SaslOauthbearerExtensions =
        "logicalCluster=lkc-abc," +
        "identityPoolId=pool-xyz",
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
| `signing_algorithm` | `ES384` | JWT signing algorithm. Either `ES384` or `RS256`. AWS STS requires this field — we default for ergonomics. See [Algorithm choice](#algorithm-choice) below. |
| `sts_endpoint` | _(SDK default)_ | Override STS endpoint URL. Use for FIPS (`sts-fips.us-east-1.amazonaws.com`) or VPC endpoints. |
| `principal_name` | _(JWT `sub` claim)_ | Override the OAUTHBEARER principal. Defaults to extracting `sub` from the minted JWT (the role ARN). |
| `aws_debug` | `none` | Opt in to AWS SDK diagnostic logging. One of `none`, `console`, `log4net`, `systemdiagnostics`. See [AWS SDK diagnostic logging](#aws-sdk-diagnostic-logging) below. |
| `tag_<NAME>` | _(none)_ | Custom tag claims added to the minted JWT (max 50). Repeatable. |

### Algorithm choice

`signing_algorithm` controls the JWT's signature type at the AWS STS layer.
We default to **ES384** because elliptic-curve signatures produce smaller tokens
(~96 bytes vs RSA's ~256 bytes) with equivalent security. Pick `RS256` only if
your broker's JWKS endpoint specifically requires RSA keys (rare in modern setups).

`SigningAlgorithm` is a required field on the underlying AWS STS API — omitting
it from `SaslOauthbearerConfig` doesn't omit it from the wire, it just selects
our default.

### AWS SDK diagnostic logging

`aws_debug` routes the **AWS SDK's** internal diagnostic logs (credential-chain
resolution, IMDS calls, STS request/response error paths, etc.) to a sink of
your choice. Off by default.

| Value | Maps to | Where logs go |
|---|---|---|
| `none` *(default)* | `LoggingOptions.None` | Library never mutates `AWSConfigs.LoggingConfig.LogTo`. |
| `console` | `LoggingOptions.Console` | `Console.Out`. Useful for container-runtime debugging (CloudWatch, `docker logs`). |
| `log4net` | `LoggingOptions.Log4Net` | log4net appenders configured by your app. |
| `systemdiagnostics` | `LoggingOptions.SystemDiagnostics` | `System.Diagnostics.Trace` listeners. |

Values are case-insensitive. Unknown values fail at `Build()` time with an
`ArgumentException`.

**Side-effect note.** `AWSConfigs.LoggingConfig.LogTo` is **process-wide**.
Setting `aws_debug` to anything other than `none` affects the AWS SDK's logging
for every client in the process, not only the STS client this library uses. The
library only mutates the global when you explicitly opt in — if you don't set
`aws_debug` (or set it to `none`), any value you may have configured elsewhere
in your application is preserved.

```csharp
// Quietest production setup — library does not touch AWS SDK logging.
"region=us-east-1 audience=https://confluent.cloud/oidc"

// Validation / debugging — verbose AWS SDK output to stdout.
"region=us-east-1 audience=https://confluent.cloud/oidc aws_debug=console"
```

### SASL extensions

RFC 7628 §3.1 SASL extensions are forwarded verbatim to the broker (separately from
the JWT). Configure them through the typed `SaslOauthbearerExtensions` property —
not inside `SaslOauthbearerConfig` — as a comma-separated list of `key=value` pairs:

```csharp
SaslOauthbearerExtensions = "logicalCluster=lkc-abc,identityPoolId=pool-xyz",
```

This matches the cross-language convention used by the Python, Go, and JavaScript
bindings (e.g. for the existing AzureIMDS flow).

## Prerequisites

The IAM role used by your application (instance profile, IRSA, or assumed role) must:

1. Be trusted to call `sts:GetWebIdentityToken` for the requested `audience`.
2. Belong to an AWS account where outbound web identity federation is enabled.

If either is missing, the first token refresh fails and the error surfaces via librdkafka's
OAUTHBEARER error event with the original AWS exception message.

## Common pitfalls

### `SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc` is required

The AWS IAM autowire runs as a high-level-client refresh callback **inside**
librdkafka's OIDC subsystem — parallel to how Azure IMDS works. Without
`method=oidc`, the configuration is rejected at `Build()` with
`InvalidOperationException`.

Always set:

```csharp
SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc,
SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam,
```

If you forget `method=oidc`, the error message points at the missing setting.

## Versioning

This package versions in lockstep with `Confluent.Kafka` core. Always reference matching versions.

## AWS SDK requirement

Targets `AWSSDK.SecurityToken >= 3.7.504`. Pulled in transitively when you reference this package.

## Further reading

- [AWS STS GetWebIdentityToken API reference](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetWebIdentityToken.html)