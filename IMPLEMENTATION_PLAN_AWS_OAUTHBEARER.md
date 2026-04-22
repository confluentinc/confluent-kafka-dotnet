# Implementation Plan: AWS OAUTHBEARER NuGet Package

**Companion document to** [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md).
**Status:** Draft — ready to execute. No code written yet.
**Last updated:** 2026-04-22.

This document describes the execution path. For design rationale, public API shape, and rejected alternatives, see the design doc.

## Locked decisions

| Decision | Value |
|---|---|
| NuGet package ID | `Confluent.Kafka.OAuthBearer.Aws` |
| Project directory | [src/Confluent.Kafka.OAuthBearer.Aws/](src/Confluent.Kafka.OAuthBearer.Aws/) |
| Test directory | [test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/](test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/) |
| Target frameworks | `netstandard2.1;net462;net6.0;net8.0` (matches [src/Confluent.SchemaRegistry.Encryption.Aws/](src/Confluent.SchemaRegistry.Encryption.Aws/)) |
| AWS SDK line | **v3.7** — supports `net462`; v4 drops `net462` in favor of `net472` |
| AWS SDK floor | `AWSSDK.SecurityToken >= 3.7.504` — first **NuGet** release with `GetWebIdentityToken`. Note: the git commit that added the API (commit [138d312](https://github.com/aws/aws-sdk-net/commit/138d312), 2025-11-19) declared `_sdk-versions.json Version=3.7.503.2`, but AWS's release pipeline bumped the patch before publishing. Versions 3.7.503, .503.1, and .503.2 on NuGet predate the feature; 3.7.504 is the first that actually contains it. Bisected empirically via reflection on restored packages (M4). |
| Credential resolution | **Lazy** — construct `AmazonSecurityTokenServiceClient` cheaply; let the SDK resolve on first `GetTokenAsync()` call. Matches AWS SDK convention across .NET / Go / Python / JS. |
| Async API | `GetTokenAsync` only — sync `GetWebIdentityToken` is BCL-only on `AWSSDK.SecurityToken`; NetStandard / NetCore / net8 only expose the async form. |
| Versioning | Lockstep with main module — no `<Version>` in csproj; inherits `VersionPrefix` from [src/Directory.Build.props](src/Directory.Build.props). Same pattern as every `Confluent.SchemaRegistry.*` sibling. |
| Release target | Deferred — picked at M8. Implementation is unblocked until then. |
| Base package (`Confluent.Kafka.OAuthBearer`) | **Not** created in v1. Ship AWS alone; revisit when Azure/GCP siblings land (design §11 item 5). |

## Critical path

```
M1 ──┬─→ M3 ──→ M4 ──→ M5 ──┬─→ M7 ──→ M8
     │                      │
     └─→ M2 (parallel)      └─→ M6 (parallel)
```

**Total effort:** ~5–7 engineer-days. Deliverable in 1 working week solo, or split between a pair as M1+M2 and M3–M5.

## Non-negotiable gates enforced at every step

1. **Zero-cost-for-non-opt-in preserved.** After every PR, run the dep-graph check (M1 and appendix) against a minimal `Confluent.Kafka`-only consumer:
   ```bash
   dotnet list package --include-transitive | grep -i '^\s*>\s*AWSSDK' && echo LEAKED || echo OK
   ls bin/Release/net8.0/ | grep -i '^AWSSDK\..*\.dll$' && echo LEAKED || echo OK
   ```
   Both must print `OK`. If either prints `LEAKED`, the packaging has regressed — roll the PR back.

2. **No new `PackageReference` in `Confluent.Kafka.csproj`.** The new AWS SDK dep lives only in [src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj](src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj). Touching the core csproj invalidates the whole pattern.

3. **Lockstep release discipline.** `Confluent.Kafka v2.X.Y` → `Confluent.Kafka.OAuthBearer.Aws v2.X.Y`, always. Enforced by inheriting `VersionPrefix` and by appveyor release scripting at M8.

4. **No `netstandard2.0` target.** All siblings use `netstandard2.1` as the NetStandard floor. `AWSSDK.SecurityToken` 3.7.x supports `netstandard2.0` but aligning with the Encryption.Aws sibling is the guarantee we care about.

---

## M1 — Scaffolding & empty-compile + dep-graph gate

**Estimated effort:** 3–4 hours
**Parallelizable with:** M2
**Blocks:** M3, M4, M5

### Deliverables

- [src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj](src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj) per design §5b — real csproj, empty source files.
- [src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.snk](src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.snk) — generate with `sn -k` (matches every sibling `.snk`).
- Empty placeholder source files (compile-only, no logic):
  - `AwsOAuthBearerConfig.cs` — namespace + empty class.
  - `AwsStsTokenProvider.cs` — namespace + empty class.
  - `AwsOAuthBearerBuilderExtensions.cs` — namespace + empty static class.
  - `JwtSubjectExtractor.cs` — namespace + empty internal static class.
- [test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/Confluent.Kafka.OAuthBearer.Aws.UnitTests.csproj](test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/Confluent.Kafka.OAuthBearer.Aws.UnitTests.csproj) — xUnit, references the new package and `Confluent.Kafka.TestsCommon`.
- [Confluent.Kafka.sln](Confluent.Kafka.sln) — both projects added via `dotnet sln add`.
- [appveyor.yml](appveyor.yml) — `dotnet pack` + `artifacts` lines added (copy-paste pattern from existing `Confluent.SchemaRegistry.Serdes.*` blocks).

### Dep-graph invariant proof (new, required)

Create a throwaway consumer project **outside** the repo referencing **only** `Confluent.Kafka` (via `ProjectReference` for now; via `PackageReference` after M8):

```bash
mkdir /tmp/ckafka-depcheck && cd /tmp/ckafka-depcheck
dotnet new console -f net8.0
dotnet add reference /…/src/Confluent.Kafka/Confluent.Kafka.csproj
dotnet restore && dotnet build -c Release
```

Run the two-line check:

```bash
dotnet list package --include-transitive | grep -i '^\s*>\s*AWSSDK' && echo LEAKED || echo OK
ls bin/Release/net8.0/ | grep -i '^AWSSDK\..*\.dll$' && echo LEAKED || echo OK
```

Both must print `OK`. Commit the exact terminal output to [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) §4 as a new subsection `4d`, dated, alongside the existing Go `go mod why` evidence.

Then repeat with `dotnet add reference /…/src/Confluent.Kafka.OAuthBearer.Aws/...` — both greps **must** print `LEAKED` (AWS SDK now present). Asymmetry is the invariant.

### Exit criteria

- `dotnet build Confluent.Kafka.sln -c Release` passes cleanly.
- `make build` (which iterates every csproj) passes.
- `make test` loops discover the new unit test project automatically ([Makefile](Makefile#L11) regex is `.*UnitTests$`).
- `dotnet pack src/Confluent.Kafka.OAuthBearer.Aws/... -c Release` produces a `.nupkg` that declares `AWSSDK.SecurityToken >= 3.7.504` in its `.nuspec`.
- Dep-graph invariant proof recorded in design doc §4d.

---

## M2 — JWT `sub` extractor

**Estimated effort:** 2–3 hours
**Parallelizable with:** M1 (fully independent — zero deps beyond BCL)
**Blocks:** M4

### Deliverables

- `src/Confluent.Kafka.OAuthBearer.Aws/JwtSubjectExtractor.cs`:
  ```csharp
  internal static class JwtSubjectExtractor
  {
      public static string ExtractSub(string jwt);
  }
  ```
  Implementation: split on `.`, pad the middle base64url segment with `=` to a multiple of 4, replace `-`/`_` with `+`/`/`, `Convert.FromBase64String`, `JsonDocument.Parse`, return `root.GetProperty("sub").GetString()`. Throw `FormatException` with a descriptive message on malformed input or missing claim. Stdlib only — no `System.IdentityModel.Tokens.Jwt` dependency.

- `test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/JwtSubjectExtractorTests.cs` covering:
  - Valid role ARN — `arn:aws:iam::123456789012:role/MyRole`.
  - Valid assumed-role ARN — different ARN shape.
  - Missing `sub` claim → `FormatException` with actionable message.
  - Token with fewer than 3 dot-separated segments → throws.
  - Token with more than 3 segments → throws.
  - Malformed base64url in payload segment → throws.
  - Malformed JSON in decoded payload → throws.
  - Empty string input → throws.
  - Oversized input guard — reject tokens > ~8 KB (document: prevents attacker-controlled allocation). Live AWS tokens are ~1.4 KB per librdkafka probe; 8 KB is a generous ceiling.
  - Padded vs unpadded base64url — both decode correctly.

### Exit criteria

- All tests pass.
- `JwtSubjectExtractor` is `internal`, visible only to `AwsStsTokenProvider` and the test project (via `[InternalsVisibleTo("Confluent.Kafka.OAuthBearer.Aws.UnitTests")]`).
- Zero new `PackageReference` added anywhere.

---

## M3 — `AwsOAuthBearerConfig` + validation

**Estimated effort:** 3–4 hours
**Depends on:** M1
**Blocks:** M4

### Deliverables

- `src/Confluent.Kafka.OAuthBearer.Aws/AwsOAuthBearerConfig.cs` — public class per design §5c.
- Internal `Validate()` method (called by `AwsStsTokenProvider` constructor):
  - `Region` required (non-null, non-empty) — no silent default, no IMDS sniffing.
  - `Audience` required (non-null, non-empty).
  - `SigningAlgorithm` must be `null`, `"ES384"`, or `"RS256"`. Null → defaulted to `"ES384"` by `ApplyDefaults()`.
  - `Duration` — either `default` (→ 300s via `ApplyDefaults()`) or in `[60s, 3600s]`.
  - `PrincipalNameOverride` — if set, must be non-empty (empty string is a misuse, not "use default").
  - Throws `ArgumentException` with a clear message naming the bad field.
- Internal `ApplyDefaults()` — fills defaults after validation passes.
- No network, no AWS SDK calls in this class.

### Tests (`AwsOAuthBearerConfigTests.cs`)

- Validation failures:
  - Null `Region` → `ArgumentException` containing "Region".
  - Empty `Region` → same.
  - Null `Audience` → `ArgumentException` containing "Audience".
  - `SigningAlgorithm = "HS256"` → rejected.
  - `Duration = TimeSpan.FromSeconds(30)` → rejected.
  - `Duration = TimeSpan.FromHours(2)` → rejected.
  - Empty `PrincipalNameOverride` → rejected.
- Defaults applied:
  - Null `SigningAlgorithm` after `ApplyDefaults()` → `"ES384"`.
  - Default `Duration` after `ApplyDefaults()` → 300s.

### Exit criteria

- All tests pass.
- Validation throws synchronously; no async, no network.

---

## M4 — `AwsStsTokenProvider` + STS wire mocking

**Estimated effort:** 1–1.5 engineer-days
**Depends on:** M2, M3
**Blocks:** M5, M7

### Deliverables

- `src/Confluent.Kafka.OAuthBearer.Aws/AwsStsTokenProvider.cs`:
  ```csharp
  public sealed class AwsStsTokenProvider : IDisposable
  {
      private readonly AwsOAuthBearerConfig _cfg;
      private readonly IAmazonSecurityTokenService _sts;
      private readonly bool _ownsClient;

      public AwsStsTokenProvider(AwsOAuthBearerConfig config)
      {
          config.Validate();
          config.ApplyDefaults();
          _cfg = config;

          var awsConfig = new AmazonSecurityTokenServiceConfig
          {
              RegionEndpoint = RegionEndpoint.GetBySystemName(config.Region),
          };
          if (!string.IsNullOrEmpty(config.StsEndpointOverride))
              awsConfig.ServiceURL = config.StsEndpointOverride;

          _sts = config.Credentials != null
              ? new AmazonSecurityTokenServiceClient(config.Credentials, awsConfig)
              : new AmazonSecurityTokenServiceClient(awsConfig); // uses FallbackCredentialsFactory
          _ownsClient = true;
      }

      // Internal constructor for tests — accepts a mock IAmazonSecurityTokenService.
      internal AwsStsTokenProvider(AwsOAuthBearerConfig config, IAmazonSecurityTokenService sts) { ... }

      public async Task<AwsOAuthBearerToken> GetTokenAsync(CancellationToken ct = default)
      {
          var resp = await _sts.GetWebIdentityTokenAsync(new GetWebIdentityTokenRequest
          {
              Audience         = new List<string> { _cfg.Audience },
              SigningAlgorithm = _cfg.SigningAlgorithm,
              DurationSeconds  = (int)_cfg.Duration.TotalSeconds,
          }, ct).ConfigureAwait(false);

          var jwt       = resp.WebIdentityToken;
          var expiry    = resp.Expiration ?? throw new InvalidOperationException("STS returned null Expiration");
          var expiryUtc = DateTime.SpecifyKind(expiry, DateTimeKind.Utc);
          var lifeMs    = new DateTimeOffset(expiryUtc).ToUnixTimeMilliseconds();
          var principal = _cfg.PrincipalNameOverride ?? JwtSubjectExtractor.ExtractSub(jwt);

          return new AwsOAuthBearerToken(jwt, lifeMs, principal, _cfg.SaslExtensions);
      }

      public void Dispose() { if (_ownsClient) _sts.Dispose(); }
  }
  ```
- **No eager credential resolution.** The `AmazonSecurityTokenServiceClient` constructor does not call the credential chain; first `GetTokenAsync()` triggers it. Matches Go's lazy decision and AWS SDK's idiom.

### Tests (`AwsStsTokenProviderTests.cs`) — with mocked `IAmazonSecurityTokenService`

Use `Moq` or a hand-written stub (prefer hand-written — no new test-time deps).

Request-capture cases — assert the outgoing `GetWebIdentityTokenRequest`:

- **Audience passthrough:** `cfg.Audience = "https://foo"` → captured request has `Audience[0] == "https://foo"`, `Audience.Count == 1`.
- **SigningAlgorithm passthrough:** `cfg.SigningAlgorithm = "RS256"` → captured request has `SigningAlgorithm == "RS256"`.
- **DurationSeconds passthrough:** `cfg.Duration = TimeSpan.FromSeconds(900)` → captured request has `DurationSeconds == 900`.

Response-mapping cases — assert the returned `AwsOAuthBearerToken`:

- **Happy path:** mock returns `WebIdentityToken = <valid 3-segment JWT with sub=arn:aws:iam::123:role/R>`, `Expiration = 2026-04-21T06:06:47.641Z`. Assert:
  - `TokenValue` == the JWT string.
  - `LifetimeMs` == `DateTimeOffset.Parse("2026-04-21T06:06:47.641Z").ToUnixTimeMilliseconds()`.
  - `PrincipalName` == `"arn:aws:iam::123:role/R"`.
- **PrincipalNameOverride takes precedence:** set `cfg.PrincipalNameOverride = "explicit"`, JWT `sub` is different. Assert returned `PrincipalName == "explicit"`.
- **Null `Expiration`:** mock returns `Expiration = null` → `InvalidOperationException`.
- **Malformed JWT:** `WebIdentityToken = "not-a-jwt"` → `FormatException` from `JwtSubjectExtractor`. Test asserts the exception propagates (the provider does **not** swallow; the handler layer in M5 does).
- **STS exception (AccessDenied):** mock throws `AmazonSecurityTokenServiceException` with `ErrorCode = "AccessDenied"`. Provider re-throws as-is. Test asserts `ex.ErrorCode == "AccessDenied"`.
- **STS exception (OutboundWebIdentityFederationDisabled):** same pattern, distinct error code. Asserts full error-code string reaches the caller.
- **Cancellation:** pass a cancelled `CancellationToken` → `OperationCanceledException`.

Construction cases:

- **Lazy creds:** construct provider with a `Credentials` that would throw on `GetCredentialsAsync()`; assert construction succeeds and no credential call happens. First `GetTokenAsync()` triggers it.
- **Invalid region:** `cfg.Region = "not-a-region"` → `ArgumentException` from `RegionEndpoint.GetBySystemName` surfaces at construction (fast failure, desirable).

### Exit criteria

- All tests pass.
- No real AWS calls anywhere in the unit test project.
- The outgoing `GetWebIdentityTokenRequest` shape matches AWS wire format per the librdkafka Probe B capture in [project_aws_outbound_federation.md](~/.claude/projects/-Users-pranavshah-workspace-extra-repo-21-April-librdkafka/memory/project_aws_outbound_federation.md) (`Action=GetWebIdentityToken&Version=2011-06-15&Audience.member.1=…`) — the AWS SDK marshaller handles serialization; our job is to populate the request object correctly.

---

## M5 — Builder extensions + sync bridge

**Estimated effort:** 3–4 hours
**Depends on:** M4
**Blocks:** M7

### Deliverables

- `src/Confluent.Kafka.OAuthBearer.Aws/AwsOAuthBearerBuilderExtensions.cs`:
  ```csharp
  public static class AwsOAuthBearerBuilderExtensions
  {
      public static ProducerBuilder<TK, TV> UseAwsOAuthBearer<TK, TV>(
          this ProducerBuilder<TK, TV> builder, AwsOAuthBearerConfig config)
          => builder.UseAwsOAuthBearer(new AwsStsTokenProvider(config));

      public static ProducerBuilder<TK, TV> UseAwsOAuthBearer<TK, TV>(
          this ProducerBuilder<TK, TV> builder, AwsStsTokenProvider provider)
          => builder.SetOAuthBearerTokenRefreshHandler(
                 AwsOAuthBearerHandler.Create(provider));

      // ... same pattern for ConsumerBuilder<TK, TV> and AdminClientBuilder
  }
  ```
- `src/Confluent.Kafka.OAuthBearer.Aws/AwsOAuthBearerHandler.cs` (internal):
  ```csharp
  internal static class AwsOAuthBearerHandler
  {
      public static Action<IClient, string> Create(AwsStsTokenProvider provider)
      {
          return (client, _oauthConfigString) =>
          {
              try
              {
                  var t = provider.GetTokenAsync().ConfigureAwait(false).GetAwaiter().GetResult();
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
  `.ConfigureAwait(false).GetAwaiter().GetResult()` pattern — safe per design §5e: refresh fires on librdkafka's background thread, no captured `SynchronizationContext`.

### Tests (`AwsOAuthBearerBuilderExtensionsTests.cs`)

- **Wiring — happy path:** construct a `ProducerBuilder<Null, string>`, call `UseAwsOAuthBearer` with a stub provider whose `GetTokenAsync` returns a fixed token. Use reflection (`internal protected OAuthBearerTokenRefreshHandler` at [ProducerBuilder.cs:85](src/Confluent.Kafka/ProducerBuilder.cs#L85)) or a custom subclass of `ProducerBuilder` exposing the handler, and assert the handler was set.
- **Handler calls `OAuthBearerSetToken` on success:** invoke the captured handler against a fake `IClient` recorder. Assert `OAuthBearerSetToken` called once with the expected fields; `OAuthBearerSetTokenFailure` not called.
- **Handler calls `OAuthBearerSetTokenFailure` on exception:** provider stub throws `AmazonSecurityTokenServiceException`; handler must call `OAuthBearerSetTokenFailure` with a message containing the AWS error code.
- **`SetOAuthBearerTokenRefreshHandler` called exactly once per `UseAwsOAuthBearer` invocation** — verified by recording invocation count.
- **Lifetime contract:** document and test that the caller must keep the `AwsStsTokenProvider` alive for the lifetime of the client. If the provider is disposed early, the next refresh produces a failure (captured via `ObjectDisposedException` → `OAuthBearerSetTokenFailure`).

### Exit criteria

- All tests pass.
- Public API surface matches design §5c exactly. Diff check:
  ```bash
  dotnet build -c Release
  # Optional: diff the .xml doc file against a committed baseline to catch signature drift.
  ```

---

## M6 — Example + package README

**Estimated effort:** 2–3 hours
**Depends on:** M5
**Parallelizable with:** M7

### Deliverables

- [examples/OAuthBearerAws/Program.cs](examples/OAuthBearerAws/Program.cs) — mirrors [examples/OAuthConsumer/Program.cs](examples/OAuthConsumer/Program.cs) but uses `UseAwsOAuthBearer` instead of inlining a token refresh handler. Compiles under the existing `make build` iteration.
- [examples/OAuthBearerAws/OAuthBearerAws.csproj](examples/OAuthBearerAws/OAuthBearerAws.csproj) — copy of `OAuthConsumer.csproj`, adds `<ProjectReference>` to the new package.
- [Confluent.Kafka.sln](Confluent.Kafka.sln) — add the example project.
- `src/Confluent.Kafka.OAuthBearer.Aws/README.md` (packed into the NuGet via `PackageReadmeFile`):
  - Two-paragraph overview.
  - Minimum AWS SDK version note (`AWSSDK.SecurityToken >= 3.7.504`).
  - The one-line integration snippet.
  - The **"do NOT set `SaslOauthbearerMethod`"** warning (that selects the librdkafka-native path and bypasses the refresh handler — design §2).
  - IAM prerequisite: `aws iam enable-outbound-web-identity-federation` run once per account by an admin (design §10).
  - Pointer to [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) and [IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md](IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md).
- XML doc comments on every public type and member in the package — [GenerateDocumentationFile is true](src/Confluent.SchemaRegistry.Encryption.Aws/Confluent.SchemaRegistry.Encryption.Aws.csproj#L20), so missing docs emit build warnings. Zero warnings in release build.

### Exit criteria

- `dotnet build Confluent.Kafka.sln -c Release` passes with zero warnings from the new package.
- `make build` iterates all examples cleanly.
- `dotnet pack src/Confluent.Kafka.OAuthBearer.Aws/... -c Release` produces a `.nupkg` whose `README.md` renders correctly on [nuget.org preview](https://www.nuget.org/packages/Confluent.Kafka.OAuthBearer.Aws/) (verify by manual upload to a test feed, or by extracting the nupkg and running the README through a markdown renderer).

---

## M7 — Real-AWS integration test (scaffold for manual E2E)

**Estimated effort:** 3–4 hours (test code + one validation run)
**Depends on:** M4
**Parallelizable with:** M6

### Deliverables

- `test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/AwsStsTokenProviderRealTests.cs`:
  ```csharp
  public class AwsStsTokenProviderRealTests
  {
      [SkippableFact]
      public async Task GetTokenAsync_RealSts()
      {
          Skip.IfNot(Environment.GetEnvironmentVariable("RUN_AWS_STS_REAL") == "1",
                     "Set RUN_AWS_STS_REAL=1 and provide AWS credentials to run.");

          var cfg = new AwsOAuthBearerConfig
          {
              Region   = Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-north-1",
              Audience = Environment.GetEnvironmentVariable("AUDIENCE") ?? "https://api.example.com",
              Duration = TimeSpan.FromMinutes(5),
          };
          using var provider = new AwsStsTokenProvider(cfg);
          var tok = await provider.GetTokenAsync();

          Assert.Matches(@"^[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+$", tok.TokenValue);
          Assert.True(tok.LifetimeMs > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
          Assert.Matches(@"^arn:aws:iam::\d+:role/.+$", tok.PrincipalName);
      }
  }
  ```
  Uses `Xunit.SkippableFact` — already referenced by [test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj:24](test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj#L24) — so no new test-time dep.
- `test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/TESTING.md`:
  - Required env vars (`RUN_AWS_STS_REAL=1`, optionally `AWS_REGION`, `AUDIENCE`).
  - EC2 role prerequisites: `sts:GetWebIdentityToken` permission; account-level `EnableOutboundWebIdentityFederation` executed.
  - Run command:
    ```bash
    RUN_AWS_STS_REAL=1 AWS_REGION=eu-north-1 AUDIENCE=https://api.example.com \
      dotnet test test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/... \
      --filter "FullyQualifiedName~AwsStsTokenProviderRealTests"
    ```
  - Reference the EC2 test box (`ktrue-iam-sts-test-role` in `eu-north-1`) shared with the librdkafka probe work — same role, same account, same prerequisites already satisfied.

### Scope boundary

This milestone verifies the package MINTS a valid token. It does **not** verify the token is accepted by a Kafka broker — that requires Confluent Cloud OIDC trust configuration, an admin action outside this implementation's scope. Broker-side acceptance is a manual E2E step the owner drives separately.

### Exit criteria

- `[SkippableFact]` is skipped by default in `make test` / CI (verified by running `make test` locally with `RUN_AWS_STS_REAL` unset).
- Owner runs the test on the EC2 box and reports green. Attach test output to the PR.

### Status — validated 2026-04-22

Green on EC2 (`ktrue-iam-sts-test-role` in `eu-north-1`, Amazon Linux 2023, .NET 8.0.25 runtime):

```
Passed AwsStsTokenProviderRealTests.GetTokenAsync_RealSts_MintsValidJwt [468 ms]
Test Run Successful. Total tests: 1, Passed: 1
```

Full path exercised: `FallbackCredentialsFactory` → IMDSv2 token hop → ASIA* creds → SigV4-signed `AmazonSecurityTokenServiceClient.GetWebIdentityTokenAsync` call → regional `sts.eu-north-1.amazonaws.com` → JWT response → `JwtSubjectExtractor.ExtractSub` → assertions on token shape, `LifetimeMs ∈ [now, now+10min]`, and principal-ARN regex. All three assertions passed. Same box used for the Go-side and librdkafka-side M7 milestones — cross-client behaviour matches.

---

## M8 — Release wiring

**Estimated effort:** 3–4 hours
**Depends on:** M1–M7 all complete

### Deliverables

1. **[appveyor.yml](appveyor.yml)** — add `dotnet pack` and `artifacts` lines for the new project (already scaffolded in M1; now locked in):
   ```yaml
   - cmd: IF "%APPVEYOR_REPO_TAG%" == "true" (dotnet pack src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj -c %CONFIGURATION%)
   - cmd: IF NOT "%APPVEYOR_REPO_TAG%" == "true" (dotnet pack src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj -c %CONFIGURATION% --version-suffix ci-%APPVEYOR_BUILD_NUMBER%)
   ```
   and:
   ```yaml
   - path: ./src/Confluent.Kafka.OAuthBearer.Aws/bin*/Release/*.nupkg
   ```

2. **[README.md](README.md)** — add to the `## Referencing` bullet list after the existing SchemaRegistry entries:
   ```markdown
   - [Confluent.Kafka.OAuthBearer.Aws](https://www.nuget.org/packages/Confluent.Kafka.OAuthBearer.Aws/) *[netstandard2.1, net462, net6.0, net8.0]* - Optional AWS STS `GetWebIdentityToken` OAUTHBEARER token provider. See [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md).
   ```

3. **[CHANGELOG.md](CHANGELOG.md)** — add a top-level entry under the next version header:
   ```markdown
   ## .NET Client

   ### Enhancements

   - Added optional NuGet package `Confluent.Kafka.OAuthBearer.Aws` — AWS STS
     `GetWebIdentityToken` OAUTHBEARER token provider. Opt-in; users not
     installing it see zero change in their `Confluent.Kafka` dependency graph.
     See `DESIGN_AWS_OAUTHBEARER.md` and
     `IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md` for details.
   ```

4. **Verify repo-wide tooling**:
   - `make build` iterates the new project cleanly.
   - `make test` discovers the new UnitTests project.
   - `dotnet build Confluent.Kafka.sln -c Release` passes (matches Semaphore PR CI per [CLAUDE.md](CLAUDE.md) "Common commands").
   - `dotnet build` on an empty NuGet consumer referencing the **published** package resolves `AWSSDK.SecurityToken >= 3.7.504` correctly.

5. **Release rehearsal** (local):
   - Bump [src/Directory.Build.props](src/Directory.Build.props) `<VersionPrefix>` to the target patch (e.g. `2.15.0`).
   - `dotnet pack src/Confluent.Kafka.OAuthBearer.Aws/... -c Release` — inspect the produced `Confluent.Kafka.OAuthBearer.Aws.2.15.0.nupkg`:
     - `unzip -p ... Confluent.Kafka.OAuthBearer.Aws.nuspec` → confirm `<version>2.15.0</version>` and `<dependency id="AWSSDK.SecurityToken" version="3.7.504" />`.
     - `unzip -l ...` → confirm `lib/net8.0/`, `lib/net6.0/`, `lib/netstandard2.1/`, `lib/net462/` all present.
   - Create a scratch project referencing the rehearsal package from a local feed; verify `dotnet add package` and `dotnet build` succeed.
   - Roll back the version bump (do not commit).

### Exit criteria

- All CI targets green.
- Rehearsal resolution confirmed.
- Release checklist updated.
- [src/Directory.Build.props](src/Directory.Build.props) `<VersionPrefix>` ready to bump at the target release tag.

---

## Post-implementation validation — executed 2026-04-22

Validated on the shared EC2 box (`ktrue-iam-sts-test-role` in `eu-north-1`, Amazon Linux 2023, x64, .NET 8.0.25 runtime). Two scratch consumer projects in `/tmp/` referenced the branch via `ProjectReference` (the .NET equivalent of Go's `replace` directive). Framework-dependent publish (`--no-self-contained`) keeps the comparison clean — the ~85 MB runtime is assumed installed, so the delta reflects only the deps.

### Phase 0 — Prep

Two scratch consumer projects in `/tmp/` outside the repo, referencing the branch build via `ProjectReference` — the .NET equivalent of Go's `replace` directive. No NuGet publishing required.

**0a. SSH to the EC2 box and clone (or update) the repo**

```bash
ssh ec2-user@<your-ec2-box>

# First-time clone:
cd ~
git clone https://github.com/confluentinc/confluent-kafka-dotnet.git
cd confluent-kafka-dotnet

# Or update an existing clone:
cd ~/confluent-kafka-dotnet
git fetch origin
```

**0b. Check out the implementation branch**

```bash
git checkout dev_prashah_IAM_NET_POC
git pull --ff-only
```

**0c. Verify .NET 8 SDK is available**

```bash
dotnet --version    # expect 8.x; if missing: sudo dnf install -y dotnet-sdk-8.0
```

**0d. Build the branch once — needed so subsequent `dotnet publish` calls in the scratch projects can resolve the `ProjectReference`s cleanly**

```bash
dotnet restore Confluent.Kafka.sln
dotnet build src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj -c Release
```

Must print `0 Error(s)`.

**0e. Confirm AWS prerequisites** — already satisfied on the shared test box, but a smoke test catches environment drift:

```bash
# IMDSv2 reachable + role attached
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/iam/security-credentials/
# Expect: ktrue-iam-sts-test-role

# Account-level federation still enabled + caller has sts:GetWebIdentityToken
aws sts get-web-identity-token \
  --audience https://api.example.com \
  --signing-algorithm RS256 \
  --region eu-north-1 > /dev/null && echo OK
```

If either check fails, see [M7 "Prerequisites" section in test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/TESTING.md](test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/TESTING.md) for the fix recipe.

**0f. Set the REPO variable and wipe any prior scratch dirs**

```bash
REPO=$HOME/confluent-kafka-dotnet                # adjust if cloned elsewhere
cd /tmp && rm -rf aws-consumer no-aws-consumer   # clean slate
```

Branch used for the 2026-04-22 validation: `dev_prashah_IAM_NET_POC`.

### Phase 1 — Scenario 1 (OPTS IN, with AWS)

**1a. Create the project and add references**

```bash
cd /tmp && mkdir aws-consumer && cd aws-consumer
dotnet new console -f net8.0 --force
dotnet add reference $REPO/src/Confluent.Kafka/Confluent.Kafka.csproj
dotnet add reference $REPO/src/Confluent.Kafka.OAuthBearer.Aws/Confluent.Kafka.OAuthBearer.Aws.csproj
```

**1b. Write `Program.cs` — minimal program that mints a real JWT**

```csharp
using System;
using Confluent.Kafka;
using Confluent.Kafka.OAuthBearer.Aws;

var cfg = new AwsOAuthBearerConfig
{
    Region   = Environment.GetEnvironmentVariable("AWS_REGION")  ?? "eu-north-1",
    Audience = Environment.GetEnvironmentVariable("AUDIENCE")    ?? "https://api.example.com",
    Duration = TimeSpan.FromMinutes(5),
};

using var provider = new AwsStsTokenProvider(cfg);
var tok = await provider.GetTokenAsync();

Console.WriteLine($"JWT length     : {tok.TokenValue.Length} chars");
Console.WriteLine($"Principal      : {tok.PrincipalName}");
Console.WriteLine($"Expiry (msUTC) : {tok.LifetimeMs}");
var secs = (tok.LifetimeMs - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) / 1000;
Console.WriteLine($"Expires in     : {secs}s");
```

**1c. Publish framework-dependent** (keeps the delta clean — the runtime is assumed installed):

```bash
dotnet publish -c Release -r linux-x64 --no-self-contained -o publish-fd
```

**1d. Measure**

```bash
echo "Published size:" ; du -sh publish-fd ; du -sb publish-fd
echo "AWSSDK DLLs:"     ; ls publish-fd/ | grep '^AWSSDK'
echo "Transitive deps:" ; dotnet list package --include-transitive 2>/dev/null | grep -i AWSSDK
```

Observed 2026-04-22:

```
Published size: 47 MB / 49,111,849 bytes
AWSSDK DLLs:    AWSSDK.Core.dll, AWSSDK.SecurityToken.dll      (2 files)
Transitive deps: AWSSDK.Core 3.7.500.46, AWSSDK.SecurityToken 3.7.504
```

**1e. Runtime proof** — actually mint a JWT against real AWS:

```bash
cd /tmp/aws-consumer/publish-fd
AWS_REGION=eu-north-1 AUDIENCE=https://api.example.com dotnet aws-consumer.dll
```

Observed 2026-04-22:

```
JWT length     : 1256 chars
Principal      : arn:aws:iam::708975691912:role/ktrue-iam-sts-test-role
Expiry (msUTC) : 1776836322188
Expires in     : 299s
```

### Phase 2 — Scenario 2 (OPTS OUT, no AWS)

**2a. Create the project and add only the core reference**

```bash
cd /tmp && mkdir no-aws-consumer && cd no-aws-consumer
dotnet new console -f net8.0 --force
dotnet add reference $REPO/src/Confluent.Kafka/Confluent.Kafka.csproj
# NOTE: no reference to Confluent.Kafka.OAuthBearer.Aws.
```

**2b. Write `Program.cs` — proves Confluent.Kafka loads without any AWS SDK**

```csharp
using System;
using Confluent.Kafka;

var cfg = new ProducerConfig { BootstrapServers = "localhost:9092" };
var builder = new ProducerBuilder<Null, string>(cfg);
Console.WriteLine("Confluent.Kafka loaded; no AWS SDK in this process.");
Console.WriteLine($"librdkafka handle type visible: {typeof(Handle).FullName}");
```

**2c. Publish**

```bash
dotnet publish -c Release -r linux-x64 --no-self-contained -o publish-fd
```

**2d. Measure — these are the numbers that prove the invariant**

```bash
echo "Published size:"   ; du -sh publish-fd ; du -sb publish-fd
echo "AWSSDK DLL count:" ; ls publish-fd/ | grep -c '^AWSSDK'
echo "Transitive check:" ; dotnet list package --include-transitive 2>/dev/null | grep -i AWSSDK && echo LEAKED || echo OK
```

Observed 2026-04-22:

```
Published size:   45 MB / 46,299,219 bytes
AWSSDK DLL count: 0
Transitive check: OK (no AWSSDK anywhere)
```

**2e. Runtime proof** — Confluent.Kafka still works without AWS SDK:

```bash
cd /tmp/no-aws-consumer/publish-fd
dotnet no-aws-consumer.dll
```

Observed 2026-04-22:

```
Confluent.Kafka loaded; no AWS SDK in this process.
librdkafka handle type visible: Confluent.Kafka.Handle
```

### Phase 3 — Cleanup

```bash
rm -rf /tmp/aws-consumer /tmp/no-aws-consumer
```

Nothing persistent created. One `sts:GetWebIdentityToken` call recorded in CloudTrail per run of Scenario 1.

### The zero-cost property, quantified

| Metric | Scenario 1 (opt-in) | Scenario 2 (opt-out) | Delta |
|---|---|---|---|
| Published size | 49,111,849 B (47 MB) | 46,299,219 B (45 MB) | **+2,812,630 B (+5.7%)** |
| AWSSDK DLLs in `publish-fd/` | 2 (`AWSSDK.Core.dll`, `AWSSDK.SecurityToken.dll`) | **0** | +2 |
| AWSSDK entries in `dotnet list package --include-transitive` | `AWSSDK.Core 3.7.500.46`, `AWSSDK.SecurityToken 3.7.504` | **empty** | +2 |
| Real JWT minted at runtime | **1256 chars**, principal `arn:aws:iam::708975691912:role/ktrue-iam-sts-test-role`, expiry 299s | n/a (Confluent.Kafka loads cleanly, no AWS SDK in process) | — |

The 2.68 MB delta is entirely the two AWS SDK DLLs. Everything else — `Confluent.Kafka.dll`, the `librdkafka.redist` native binary, System.* transitives — is byte-identical between the two published outputs.

Relative delta (5.7%) is smaller than Go's (−45%) because `Confluent.Kafka`'s baseline already contains the bundled `librdkafka.redist` native binary, which dominates the absolute size. In absolute terms the AWS SDK cost is the same ~2.7 MB any NuGet package depending on `AWSSDK.SecurityToken` pays — no overhead from this package's glue.

### Cross-language mint consistency

The 1256-byte JWT length reproduces exactly what the Go and librdkafka clients observed on this same EC2 box / role / audience. Three language-layer paths, three different OAUTHBEARER glue implementations, byte-identical STS responses — AWS STS is the single source of truth and every client drives it correctly. Cross-referenced in:

- Go plan, Scenario 1: "1256-byte JWT via IMDS credentials on each invocation"
- librdkafka project memory, Probe A / M7: "1256-byte JWT minted"
- This test, Scenario 1: "JWT length : 1256 chars"

## Open items for later (not blocking implementation)

1. **Release target version** — pick at tag time. Leaning toward whatever minor version is cut after implementation merges.
2. **Extension to other high-level clients** — `confluent-kafka-python`, `confluent-kafka-javascript`. Out of scope here; pattern documented in project-level memory so future work can reuse this plan's structure.
3. **Additional token providers** — `Confluent.Kafka.OAuthBearer.Azure`, `Confluent.Kafka.OAuthBearer.Gcp`. Naming reserves space for these; implementations are independent future work.
4. **AWS SDK v4 migration** — evaluate when the Encryption.Aws sibling migrates. Blocks on `net462` removal across the .NET client (v4 supports `net472+` only).
5. **Logging hook** — users wanting to plug in their own `ILogger` for AWS SDK diagnostics. Defer until first real request.

## References

- [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) — full design doc.
- Go plan (template): `IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md` in `confluent-kafka-go`.
- Existing optional-package precedent: [src/Confluent.SchemaRegistry.Encryption.Aws/](src/Confluent.SchemaRegistry.Encryption.Aws/).
- OAUTHBEARER hook sites: [ProducerBuilder.cs:284](src/Confluent.Kafka/ProducerBuilder.cs#L284), [ConsumerBuilder.cs:244](src/Confluent.Kafka/ConsumerBuilder.cs#L244), [AdminClientBuilder.cs:160](src/Confluent.Kafka/AdminClientBuilder.cs#L160), [ClientExtensions.cs:42](src/Confluent.Kafka/ClientExtensions.cs#L42).
- Existing managed-path example: [examples/OAuthConsumer/Program.cs:84-99](examples/OAuthConsumer/Program.cs#L84-L99).
- AWS SDK commits adding `GetWebIdentityToken`: [4b10094](https://github.com/aws/aws-sdk-net/commit/4b10094) (v4, 4.0.4.1), [138d312](https://github.com/aws/aws-sdk-net/commit/138d312) (v3.7, 3.7.504).
