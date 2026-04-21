# Design: AWS STS `GetWebIdentityToken` OAUTHBEARER Provider — .NET

**Status:** Draft — pending naming decision and first implementation
**Owner:** prashah@confluent.io
**Last updated:** 2026-04-21

## 1. Context

AWS shipped **IAM Outbound Identity Federation** (GA 2025-11-19), exposing a new STS API, `GetWebIdentityToken`. AWS principals mint short-lived OIDC JWTs that external OIDC-compatible services (e.g. Confluent Cloud) can verify against an AWS-hosted JWKS. AWS acts as the OIDC IdP; the external service is the relying party.

Confluent customers running Kafka clients on AWS (EC2, EKS, ECS/Fargate, Lambda) want to authenticate to OIDC-gated Kafka endpoints using their AWS identity, without long-lived secrets.

A parallel effort in librdkafka implements this in C (reachable from every Confluent Kafka client). This document describes a **complementary, .NET-managed integration** — a new optional NuGet — that lets users ship today without waiting for or upgrading `librdkafka.redist`, and that leverages the AWS SDK for .NET's mature credential chain (env / IMDSv2 / ECS / EKS IRSA / EKS Pod Identity / SSO / profile).

The Go-side equivalent is documented in `DESIGN_AWS_OAUTHBEARER.md` at the `confluent-kafka-go` repo root. This doc mirrors it; API shapes are aligned where idiomatic.

## 2. Decision

Add an **optional, separately-packaged NuGet** at:

```
src/Confluent.Kafka.OAuthBearer.Aws/  →  NuGet: Confluent.Kafka.OAuthBearer.Aws
```

that wraps the AWS SDK for .NET to call `sts:GetWebIdentityToken` and feeds the result into the existing `SetOAuthBearerTokenRefreshHandler` delegate. Users wire it into their `Producer`/`Consumer`/`AdminClient` builder with one extension method.

The new package declares `AWSSDK.SecurityToken` as a `PackageReference`. **Users who never install this NuGet see zero change to the `Confluent.Kafka` dependency graph** — the core client csproj at [src/Confluent.Kafka/Confluent.Kafka.csproj](src/Confluent.Kafka/Confluent.Kafka.csproj) is not modified.

This mirrors the pattern already established in this repo by [src/Confluent.SchemaRegistry.Encryption.Aws/](src/Confluent.SchemaRegistry.Encryption.Aws/) and its Azure/Gcp/HcVault siblings: KMS-agnostic core + one optional per-cloud NuGet, SDK deps isolated to the opted-in package.

## 3. Rejected alternatives

### 3a. Add the AWS SDK dependency to `Confluent.Kafka` directly, gated by `#if`

Would force `AWSSDK.SecurityToken` into every downstream user's `packages.lock.json`, SBOM, and restored output. Even if `#if NET_AWS` excludes the code from compilation, the `PackageReference` itself is unconditional unless wrapped in `<ItemGroup Condition="...">`, and conditionals on a public NuGet's deps are fragile. Rejected on the same grounds as the Go "build tags" option: preserves zero dep-graph cost for non-opting users only when the SDK lives in a separate publishable unit.

### 3b. librdkafka-native path only

Relies on the parallel librdkafka C-layer work (`sasl.oauthbearer.method=aws_sts_web_identity`) landing and users upgrading `librdkafka.redist`. Users stuck on pinned versions, or who want the AWS SDK's role-chaining / SSO / profile support, get nothing. Both paths should coexist — this doc covers the managed one.

### 3c. Plugin-style registration via assembly scanning or DI

`MEF`/reflection-based discovery in `Confluent.Kafka` that looks for IOAuthBearerProvider implementations. Elegant but solves a problem that doesn't exist — the builder already exposes a `Set…Handler` method. The extension-method approach is simpler and has no runtime discovery overhead.

## 4. Verification performed (2026-04-21)

### 4a. `GetWebIdentityToken` is available in `AWSSDK.SecurityToken`

Both v3 and v4 major-version lines shipped the API **on the same day (2025-11-19)**. Pinned via `gh api` commit search on `aws/aws-sdk-net`:

| SDK line | First version with GetWebIdentityToken | Commit | `AWSSDK.Core` floor |
|---|---|---|---|
| **v4** (branch `main`) | `AWSSDK.SecurityToken 4.0.4.1` | [4b10094](https://github.com/aws/aws-sdk-net/commit/4b10094) (2025-11-19) | `4.0.3.1` |
| **v3.7** (branch `aws-sdk-net-v3.7`) | `AWSSDK.SecurityToken 3.7.503.2` | [138d312](https://github.com/aws/aws-sdk-net/commit/138d312) (2025-11-19) | `3.7.500.45` |

Versions confirmed by reading `generator/ServiceModels/_sdk-versions.json` at each commit. File `sdk/src/Services/SecurityToken/Generated/Model/GetWebIdentityTokenRequest.cs` is present at both refs. Latest-as-of-today: v4.0.6 (2026-04-17), v3.7.504.47 (2026-03-26).

**Target for this package: `AWSSDK.SecurityToken >= 3.7.503.2`** on the v3 line — same major version as [src/Confluent.SchemaRegistry.Encryption.Aws/Confluent.SchemaRegistry.Encryption.Aws.csproj](src/Confluent.SchemaRegistry.Encryption.Aws/Confluent.SchemaRegistry.Encryption.Aws.csproj#L31), supports `net462` (which `Confluent.Kafka` still targets), widely deployed. v4 considered and deferred (§11 open item 1).

### 4b. Method signature and request/response shape

From the v4 API docs page and source generation at commit 4b10094:

```csharp
// namespace Amazon.SecurityToken
public virtual GetWebIdentityTokenResponse GetWebIdentityToken(GetWebIdentityTokenRequest request);
public virtual Task<GetWebIdentityTokenResponse> GetWebIdentityTokenAsync(
    GetWebIdentityTokenRequest request, CancellationToken cancellationToken = default);

// namespace Amazon.SecurityToken.Model
public class GetWebIdentityTokenRequest {
    public List<string> Audience { get; set; }       // Audience.member.N on the wire
    public string SigningAlgorithm { get; set; }     // "ES384" or "RS256"
    public int? DurationSeconds { get; set; }        // 60–3600
}
public class GetWebIdentityTokenResponse {
    public string WebIdentityToken { get; set; }     // the JWT
    public DateTime? Expiration { get; set; }        // SDK parses RFC3339 w/ fractional secs
}
```

Sync `GetWebIdentityToken` is BCL-only (net462); NetStandard / .NET Core / .NET 8 only have the async form. We will use `GetWebIdentityTokenAsync` exclusively so the code compiles on every target.

### 4c. Builders expose the hooks we need

All three client builders in [src/Confluent.Kafka/](src/Confluent.Kafka/) already provide `SetOAuthBearerTokenRefreshHandler`:

| Type | Method | Source |
|---|---|---|
| `ProducerBuilder<K,V>` | `SetOAuthBearerTokenRefreshHandler(Action<IProducer<K,V>, string>)` | [ProducerBuilder.cs:284](src/Confluent.Kafka/ProducerBuilder.cs#L284) |
| `ConsumerBuilder<K,V>` | `SetOAuthBearerTokenRefreshHandler(Action<IConsumer<K,V>, string>)` | [ConsumerBuilder.cs:244](src/Confluent.Kafka/ConsumerBuilder.cs#L244) |
| `AdminClientBuilder` | `SetOAuthBearerTokenRefreshHandler(Action<IProducer<Null,Null>, string>)` | [AdminClientBuilder.cs:160](src/Confluent.Kafka/AdminClientBuilder.cs#L160) |

Handlers report results via `IClient` extension methods at [ClientExtensions.cs:42/60](src/Confluent.Kafka/ClientExtensions.cs#L42):

```csharp
client.OAuthBearerSetToken(tokenValue, lifetimeMs, principalName, extensions);
client.OAuthBearerSetTokenFailure(error);
```

`IProducer<K,V>`, `IConsumer<K,V>`, `IAdminClient` all implement `IClient`, so the same glue binds all three.

## 5. Architecture

### 5a. Directory layout

```
confluent-kafka-dotnet/
├── src/
│   ├── Confluent.Kafka/                              ← unchanged
│   └── Confluent.Kafka.OAuthBearer.Aws/              ← NEW
│       ├── Confluent.Kafka.OAuthBearer.Aws.csproj
│       ├── Confluent.Kafka.OAuthBearer.Aws.snk
│       ├── AwsOAuthBearerConfig.cs
│       ├── AwsStsTokenProvider.cs
│       ├── AwsOAuthBearerExtensions.cs   ← builder.UseAwsOAuthBearer(...)
│       └── JwtSubjectExtractor.cs        ← ~30 LoC base64url + JSON
├── test/
│   └── Confluent.Kafka.OAuthBearer.Aws.UnitTests/    ← NEW
│       ├── *.csproj
│       ├── AwsStsTokenProviderTests.cs
│       ├── JwtSubjectExtractorTests.cs
│       └── UseAwsOAuthBearerTests.cs
└── examples/
    └── OAuthBearerAws/                               ← NEW, optional
        ├── OAuthBearerAws.csproj
        └── Program.cs
```

The `test/*.UnitTests$` naming is required — [Makefile](Makefile#L11) auto-discovers it as `make test` fodder.

### 5b. Package csproj (copy of Encryption.Aws with AWSSDK dep swapped)

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <ProjectTypeGuids>{FAE04EC0-301F-11D3-BF4B-00C04F79EFBC}</ProjectTypeGuids>
    <Authors>Confluent Inc.</Authors>
    <Description>AWS STS GetWebIdentityToken OAUTHBEARER provider for Confluent.Kafka</Description>
    <Copyright>Copyright 2026 Confluent Inc.</Copyright>
    <PackageId>Confluent.Kafka.OAuthBearer.Aws</PackageId>
    <PackageTags>Kafka;Confluent;OAUTHBEARER;OAuth;AWS;STS;IAM</PackageTags>
    <Title>Confluent.Kafka.OAuthBearer.Aws</Title>
    <AssemblyName>Confluent.Kafka.OAuthBearer.Aws</AssemblyName>

    <!-- Inherits net6.0;net8.0 from src/Directory.Build.props -->
    <TargetFrameworks>$(TargetFrameworks);netstandard2.1;net462</TargetFrameworks>
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <SignAssembly>true</SignAssembly>
    <AssemblyOriginatorKeyFile>Confluent.Kafka.OAuthBearer.Aws.snk</AssemblyOriginatorKeyFile>

    <PackageProjectUrl>https://github.com/confluentinc/confluent-kafka-dotnet/</PackageProjectUrl>
    <PackageLicenseExpression>Apache-2.0</PackageLicenseExpression>
    <RepositoryUrl>https://github.com/confluentinc/confluent-kafka-dotnet.git</RepositoryUrl>
    <RepositoryType>git</RepositoryType>
    <PackageIcon>confluent-logo.png</PackageIcon>
    <PackageReadmeFile>README.md</PackageReadmeFile>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\Confluent.Kafka\Confluent.Kafka.csproj" />
  </ItemGroup>

  <ItemGroup>
    <!-- v3.7 line; v3.7.503.2 is the first release with GetWebIdentityToken. -->
    <PackageReference Include="AWSSDK.SecurityToken" Version="3.7.503.2" />
  </ItemGroup>

  <ItemGroup>
    <None Include="..\..\confluent-logo.png" Pack="true" PackagePath="\" />
    <None Include="..\..\README.md" Pack="true" PackagePath="\" />
  </ItemGroup>
</Project>
```

Target-framework rationale: match `Confluent.SchemaRegistry.Encryption.Aws` exactly (`netstandard2.1;net462;net6.0;net8.0`). `AWSSDK.SecurityToken` v3.7 supports all four. `net462` is retained because `Confluent.Kafka` still targets it; dropping it in a sibling package would force .NET Framework users to stay off this integration.

### 5c. Public API surface

```csharp
namespace Confluent.Kafka.OAuthBearer.Aws;

/// <summary>
/// Parameters for calling sts:GetWebIdentityToken to mint an OAUTHBEARER token.
/// </summary>
public sealed class AwsOAuthBearerConfig
{
    /// <summary>AWS region, e.g. "us-east-1". Required. No silent default.</summary>
    public string Region { get; set; }

    /// <summary>OIDC audience claim the relying party expects. Required.</summary>
    public string Audience { get; set; }

    /// <summary>"ES384" (default) or "RS256".</summary>
    public string SigningAlgorithm { get; set; } = "ES384";

    /// <summary>Token lifetime requested from STS. 60s–3600s. Default 300s.</summary>
    public TimeSpan Duration { get; set; } = TimeSpan.FromSeconds(300);

    /// <summary>Optional STS endpoint override (FIPS, VPC endpoint).</summary>
    public string StsEndpointOverride { get; set; }

    /// <summary>
    /// Explicit AWS credentials. If null, <see cref="FallbackCredentialsFactory"/>
    /// is used (env → assume-role-with-web-identity → ECS/Pod-Identity → IMDSv2 → profile).
    /// </summary>
    public Amazon.Runtime.AWSCredentials Credentials { get; set; }

    /// <summary>
    /// Optional SASL extensions communicated to the broker (RFC 7628 §3.1).
    /// e.g. { "logicalCluster": "lkc-abc", "identityPoolId": "pool-xyz" }
    /// </summary>
    public IDictionary<string, string> SaslExtensions { get; set; }

    /// <summary>
    /// Optional: override the principal name written into OAuthBearerSetToken.
    /// When null, the provider extracts the JWT "sub" claim (bare role ARN).
    /// </summary>
    public string PrincipalNameOverride { get; set; }
}

/// <summary>
/// Fetches OAUTHBEARER tokens via sts:GetWebIdentityToken.
/// Thread-safe; share one instance across all clients in a process.
/// </summary>
public sealed class AwsStsTokenProvider : IDisposable
{
    public AwsStsTokenProvider(AwsOAuthBearerConfig config);

    /// <summary>
    /// Mint a fresh JWT. Throws on configuration / STS / parsing failure — the
    /// caller (UseAwsOAuthBearer) converts exceptions into OAuthBearerSetTokenFailure.
    /// </summary>
    public Task<AwsOAuthBearerToken> GetTokenAsync(CancellationToken ct = default);

    public void Dispose();
}

public readonly struct AwsOAuthBearerToken
{
    public string TokenValue { get; }
    public long LifetimeMs { get; }         // DateTimeOffset.ToUnixTimeMilliseconds
    public string PrincipalName { get; }
    public IDictionary<string, string> Extensions { get; }
}

/// <summary>Builder extensions that wire an AwsStsTokenProvider into OAUTHBEARER refresh.</summary>
public static class AwsOAuthBearerBuilderExtensions
{
    public static ProducerBuilder<TK, TV> UseAwsOAuthBearer<TK, TV>(
        this ProducerBuilder<TK, TV> builder, AwsOAuthBearerConfig config);

    public static ConsumerBuilder<TK, TV> UseAwsOAuthBearer<TK, TV>(
        this ConsumerBuilder<TK, TV> builder, AwsOAuthBearerConfig config);

    public static AdminClientBuilder UseAwsOAuthBearer(
        this AdminClientBuilder builder, AwsOAuthBearerConfig config);

    // Overloads accepting a pre-built AwsStsTokenProvider, for callers who
    // want to share one provider across multiple clients.
    public static ProducerBuilder<TK, TV> UseAwsOAuthBearer<TK, TV>(
        this ProducerBuilder<TK, TV> builder, AwsStsTokenProvider provider);
    // ... (Consumer, AdminClient overloads)
}
```

### 5d. `GetTokenAsync` internal flow

```csharp
internal async Task<AwsOAuthBearerToken> GetTokenAsync(CancellationToken ct)
{
    var resp = await _sts.GetWebIdentityTokenAsync(new GetWebIdentityTokenRequest
    {
        Audience         = new List<string> { _cfg.Audience },
        SigningAlgorithm = _cfg.SigningAlgorithm,
        DurationSeconds  = (int)_cfg.Duration.TotalSeconds,
    }, ct).ConfigureAwait(false);

    var jwt        = resp.WebIdentityToken;
    var lifetimeMs = new DateTimeOffset(resp.Expiration.Value, TimeSpan.Zero)
                        .ToUnixTimeMilliseconds();
    var principal  = _cfg.PrincipalNameOverride
                     ?? JwtSubjectExtractor.ExtractSub(jwt);

    return new AwsOAuthBearerToken(jwt, lifetimeMs, principal, _cfg.SaslExtensions);
}
```

All SigV4 signing, credential resolution (IMDSv2 token hop, EKS IRSA file rotation, ECS agent handshake, SSO refresh), retries, clock-skew correction, and XML parsing are delegated to `AWSSDK.SecurityToken`. This is the main argument for going managed instead of native: thousands of engineering hours of AWS-SDK work reused for free.

### 5e. Bridging async → the sync handler delegate

The builder delegate type is `Action<IClient, string>`, not `Func<…, Task>`. We block at the edge:

```csharp
internal static class AwsOAuthBearerHandler
{
    public static Action<IClient, string> Create(AwsStsTokenProvider provider)
    {
        return (client, _oauthConfigString) =>
        {
            try
            {
                var t = provider.GetTokenAsync().GetAwaiter().GetResult();
                client.OAuthBearerSetToken(t.TokenValue, t.LifetimeMs, t.PrincipalName, t.Extensions);
            }
            catch (Exception ex)
            {
                client.OAuthBearerSetTokenFailure(ex.ToString());
            }
        };
    }
}
```

`.GetAwaiter().GetResult()` is safe here because the refresh callback runs on librdkafka's background thread — there is no `SynchronizationContext` to deadlock against, and librdkafka serializes refresh calls per client. There is no ASP.NET request thread involved.

**Do not** expose an async builder method on the public API (`UseAwsOAuthBearerAsync`) — the underlying delegate is sync, and an async wrapper would be misleading.

### 5f. JWT `sub` extraction

Live STS response (librdkafka memory, §"Probe B"): `sub` is the bare role ARN like `arn:aws:iam::708975691912:role/ktrue-iam-sts-test-role`. Extraction is ~30 LoC: split on `.`, pad the base64url segment to a multiple of 4, decode, parse JSON, read `sub`. No JWT library needed — we are not validating the signature (AWS minted it; trust is upstream).

## 6. User-side integration pattern

```csharp
using Confluent.Kafka;
using Confluent.Kafka.OAuthBearer.Aws;

var cfg = new ConsumerConfig
{
    BootstrapServers  = "pkc-xxxx.us-east-1.aws.confluent.cloud:9092",
    SecurityProtocol  = SecurityProtocol.SaslSsl,
    SaslMechanism     = SaslMechanism.OAuthBearer,
    GroupId           = "my-group",
    AutoOffsetReset   = AutoOffsetReset.Earliest,
    // NOTE: do NOT set SaslOauthbearerMethod — that selects the librdkafka-native path.
};

var awsCfg = new AwsOAuthBearerConfig
{
    Region   = "us-east-1",
    Audience = "https://confluent.cloud/oidc",
    Duration = TimeSpan.FromHours(1),
    SaslExtensions = new Dictionary<string, string>
    {
        ["logicalCluster"] = "lkc-abc123",
        ["identityPoolId"] = "pool-xyz",
    },
};

using var consumer = new ConsumerBuilder<string, string>(cfg)
    .UseAwsOAuthBearer(awsCfg)
    .Build();

consumer.Subscribe("my-topic");
while (!ct.IsCancellationRequested)
{
    var cr = consumer.Consume(ct);
    // ...
}
```

No timers, no background tasks, no manual poll loops for tokens. librdkafka emits the refresh event; our delegate services it; token lifetime drives the next refresh.

## 7. Local development

Unlike Go, .NET has no `go.work` equivalent needed — the solution file [Confluent.Kafka.sln](Confluent.Kafka.sln) already resolves project references via `<ProjectReference>`. The new project plugs into the solution in the same slot as every existing sibling. `dotnet build Confluent.Kafka.sln -c Release` and `make build` pick it up automatically; the appveyor release pipeline gains one line:

```yaml
- cmd: IF "%APPVEYOR_REPO_TAG%" == "true" (dotnet pack src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj -c %CONFIGURATION%)
- cmd: IF NOT "%APPVEYOR_REPO_TAG%" == "true" (dotnet pack src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj -c %CONFIGURATION% --version-suffix ci-%APPVEYOR_BUILD_NUMBER%)
```

and one `artifacts` entry. Pattern is already visible in [appveyor.yml](appveyor.yml) for every existing package.

## 8. Release and versioning policy

**Policy:** lockstep with the main module — same pattern as all existing `Confluent.SchemaRegistry.*` packages.

- `VersionPrefix` is set repo-wide in [src/Directory.Build.props](src/Directory.Build.props). Bumping that one property bumps every package in lockstep.
- The new package's csproj has no `<Version>` — it inherits.
- Users matching `Confluent.Kafka v2.15.0` automatically get `Confluent.Kafka.OAuthBearer.Aws v2.15.0`.
- No compatibility matrix — same-minor pairs always work.
- Skew concern: `Confluent.Kafka.OAuthBearer.Aws` only uses stable public API from `Confluent.Kafka` (`IClient`, `ProducerBuilder<,>`, `SetOAuthBearerTokenRefreshHandler`, `OAuthBearerSetToken`/`OAuthBearerSetTokenFailure`). Cross-version compatibility inside a 2.x line is already a repo invariant.

## 9. Testing strategy

Three layers, all local:

1. **Unit — token provider** (`AwsStsTokenProviderTests.cs`)
   Inject a mock `IAmazonSecurityTokenService` (AWS SDK ships this interface). Return canned `GetWebIdentityTokenResponse` fixtures. Assert we:
   - pass `Audience`, `SigningAlgorithm`, `DurationSeconds` through correctly;
   - surface STS exceptions as provider exceptions;
   - map `Expiration` → `lifetimeMs` via `DateTimeOffset(UTC).ToUnixTimeMilliseconds()`;
   - use `PrincipalNameOverride` when set; fall back to JWT `sub` otherwise.

2. **Unit — JWT subject extractor** (`JwtSubjectExtractorTests.cs`)
   Fixtures for bare role ARN, assumed-role ARN, missing `sub`, malformed base64url (with and without padding), three-segment vs two-segment tokens, oversized payloads. No JWT library.

3. **Unit — builder wiring** (`UseAwsOAuthBearerTests.cs`)
   Verify `UseAwsOAuthBearer` calls `SetOAuthBearerTokenRefreshHandler` exactly once and that the handler invokes `OAuthBearerSetToken` on success / `OAuthBearerSetTokenFailure` on thrown exception. Uses an `IClient` fake — no librdkafka.

4. **Integration (opt-in)** — `RUN_AWS_STS_REAL=1` gated xUnit `[SkippableFact]` (already used pattern in [test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj](test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj#L24)). Runs on the EC2 test box (`eu-north-1`, role `ktrue-iam-sts-test-role`) used by the librdkafka probe work. Off by default in CI; run manually or on a scheduled pipeline.

CI wiring: `make test` in [Makefile](Makefile#L11) auto-discovers any `test/*UnitTests` directory — no CI changes beyond adding the directory.

## 10. Operational notes

- **Region must be explicit.** No silent default, no IMDS sniffing. Misconfigured region → startup `ArgumentException` from the `AwsStsTokenProvider` constructor. Matches librdkafka-side decision.
- **`DurationSeconds` bounds:** 60–3600 (AWS-enforced). Default 300s. Validate at construction.
- **Enablement prerequisite:** the AWS account must have run `aws iam enable-outbound-web-identity-federation` once. First `GetWebIdentityToken` on a non-enabled account returns `OutboundWebIdentityFederationDisabledException` — surface the message verbatim in `OAuthBearerSetTokenFailure`.
- **FIPS / VPC endpoints:** supported via `Config.StsEndpointOverride` → passed to `AmazonSecurityTokenServiceClient` via `AmazonSecurityTokenServiceConfig.ServiceURL`.
- **Lambda:** `FallbackCredentialsFactory` reads the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` env vars Lambda injects. Lambda role needs `sts:GetWebIdentityToken` permission; VPC-bound Lambdas need egress to the STS regional endpoint. Operational, not code.
- **EKS IRSA:** `FallbackCredentialsFactory` resolves `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`. SDK handles token-file rotation (re-read per credential refresh).
- **Credential caching:** `AmazonSecurityTokenServiceClient` caches credentials via the SDK's internal resolver; we do not cache at the provider level. librdkafka drives the next token refresh via expiration-based event emission — extra caching here would be redundant and could mask expiration.

## 11. Open items

1. **v3 vs v4 AWS SDK major version.** Proposal: start with v3.7 (matches `Confluent.SchemaRegistry.Encryption.Aws`, supports `net462`). If the encryption packages migrate to v4, re-evaluate. v4 drops `net462` in favor of `net472`, which would force dropping `net462` from this package too.

2. **Package naming.** Proposal `Confluent.Kafka.OAuthBearer.Aws`. Alternatives: `Confluent.Kafka.Auth.Aws`, `Confluent.Kafka.Oidc.Aws`. Locks at first publish — decide before v1. Preferring `OAuthBearer` because it matches existing config property names (`SaslOauthbearerMethod`, `SaslOauthbearerConfig`) and leaves room for sibling `Confluent.Kafka.OAuthBearer.Azure` / `.Gcp`.

3. **Should the provider expose a hook for request/response logging?** Would let users plug in their own `ILogger` without leaking the AWS SDK. Low-priority; defer until first real-user request.

4. **Eager vs lazy credential resolution.** Aligning with the Go decision (§11 item 3 in the Go doc): lean eager. The `AwsStsTokenProvider` constructor calls `FallbackCredentialsFactory.GetCredentials()` and throws synchronously on failure, so misconfiguration surfaces at `builder.Build()` rather than on first broker contact.

5. **Publish `Confluent.Kafka.OAuthBearer` base package?** The Encryption family has `Confluent.SchemaRegistry.Encryption` as a shared base. A base `Confluent.Kafka.OAuthBearer` is only worthwhile if Azure/GCP siblings land — skip until then, ship AWS alone first.

6. **CHANGELOG + README integration.** Add new package to the `## Referencing` bullet list in [README.md](README.md#L36) and a `CHANGELOG.md` entry at first release. One line each.

## 12. References

- AWS IAM Outbound Identity Federation announcement (2025-11-19).
- AWS SDK for .NET v4 API docs: `SecurityTokenServiceClient.GetWebIdentityToken` ([link](https://docs.aws.amazon.com/sdkfornet/v4/apidocs/items/SecurityToken/MSecurityTokenServiceGetWebIdentityTokenGetWebIdentityTokenRequest.html)).
- aws-sdk-net commits [4b10094](https://github.com/aws/aws-sdk-net/commit/4b10094) (v4, 4.0.4.1) and [138d312](https://github.com/aws/aws-sdk-net/commit/138d312) (v3.7, 3.7.503.2).
- Go-side design: `DESIGN_AWS_OAUTHBEARER.md` in `confluent-kafka-go` — complementary, API-aligned.
- librdkafka-native design: `DESIGN_AWS_OAUTHBEARER_V1.md` in `librdkafka` — identical wire protocol, shared probe results on EC2 `eu-north-1`.
- Existing optional-package template: [src/Confluent.SchemaRegistry.Encryption.Aws/](src/Confluent.SchemaRegistry.Encryption.Aws/).
- OAUTHBEARER hook sites: [ProducerBuilder.cs:284](src/Confluent.Kafka/ProducerBuilder.cs#L284), [ConsumerBuilder.cs:244](src/Confluent.Kafka/ConsumerBuilder.cs#L244), [AdminClientBuilder.cs:160](src/Confluent.Kafka/AdminClientBuilder.cs#L160), [ClientExtensions.cs:42](src/Confluent.Kafka/ClientExtensions.cs#L42).
- Existing managed-path example (custom token provider): [examples/OAuthConsumer/Program.cs:84-99](examples/OAuthConsumer/Program.cs#L84-L99).
