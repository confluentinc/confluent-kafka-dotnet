# Implementation plan: Reflection-based OAUTHBEARER autowire for AWS

**Status:** Draft — validation-phase plan (Emanuele's §2 only; no hybrid, no typed package, no librdkafka coordination).
**Owner:** prashah@confluent.io
**Last updated:** 2026-04-28
**Supersedes:** [IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md](IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md) — typed-package M1–M8 (not adopted in this plan).
**Related:**
- [DESIGN_OAUTHBEARER_AUTOWIRE_REFLECTION.md §2](DESIGN_OAUTHBEARER_AUTOWIRE_REFLECTION.md) — Emanuele's proposal this plan implements.
- [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) — original typed-config design (kept for §4 verification data only).

**Scope of this plan:** initial validation of the reflection autowire approach end-to-end on .NET, with **zero librdkafka changes**. We use the same marker key Python ships with — `sasl.oauthbearer.metadata.authentication.type=aws_iam` — but extend the existing `SaslOauthbearerMetadataAuthenticationType` enum **on the .NET side only**, and conditionally strip that key/value pair before the config reaches `rd_kafka_new` (because librdkafka doesn't yet know `aws_iam`). When librdkafka eventually adds the value natively, the `.NET`-side strip becomes a no-op and the path migrates seamlessly.

## 1. Goal & non-goals

**Goal.** Ship a config-only, autowired AWS OAUTHBEARER experience in `Confluent.Kafka` that matches Python's UX. User adds two `<PackageReference>`s (`Confluent.Kafka` + `AWSSDK.SecurityToken`), sets a marker key in the existing `ConsumerConfig` / `ProducerConfig` / `AdminClientConfig`, calls `.Build()`. No `using`, no extension method, no `AwsOAuthBearerConfig` construction, no separate Confluent-published optional NuGet.

**Non-goals.**
- No `Confluent.Kafka.OAuthBearer.Aws` NuGet — eliminated. Architectural bet: AWS glue lives in `Confluent.Kafka` core.
- No typed `AWSCredentials` injection (`AssumeRoleAWSCredentials`, custom credential providers). String-only configuration via `sasl.oauthbearer.config`. Power users with custom-credential needs are out of scope for this plan; if demand materializes, revisit with a follow-up.
- No rich typed `SaslExtensions` — string-encoded inside `sasl.oauthbearer.config`.
- No Azure / GCP / Vault path. The user has confirmed Azure is handled in librdkafka (C-side, already shipped — see existing `SaslOauthbearerMetadataAuthenticationType.AzureIMDS` enum in [Config_gen.cs:209](src/Confluent.Kafka/Config_gen.cs#L209)). This plan is AWS-only and stays AWS-only. Concern 1 from the design doc collapses: there is no precedent being set for "every cloud goes into core" — Azure is already off the table.

## 2. UX target

```xml
<ItemGroup>
  <PackageReference Include="Confluent.Kafka" Version="..." />
  <PackageReference Include="AWSSDK.SecurityToken" Version="3.7.504" />
</ItemGroup>
```

```csharp
var cfg = new ConsumerConfig
{
    BootstrapServers      = "pkc-xxxx.aws.confluent.cloud:9092",
    SecurityProtocol      = SecurityProtocol.SaslSsl,
    SaslMechanism         = SaslMechanism.OAuthBearer,
    GroupId               = "my-group",
    SaslOauthbearerConfig = "region=us-east-1 audience=https://confluent.cloud/oidc duration_seconds=3600",
    SaslOauthbearerMetadataAuthenticationType = SaslOauthbearerMetadataAuthenticationType.AwsIam,
};

using var consumer = new ConsumerBuilder<string, string>(cfg).Build();
consumer.Subscribe("my-topic");
```

That's the entire integration site — same key as Python (`sasl.oauthbearer.metadata.authentication.type=aws_iam`) wired through the existing typed enum.

## 3. Marker key — extend the .NET enum, strip conditionally before native

**Marker key:** `sasl.oauthbearer.metadata.authentication.type=aws_iam` (identical to Python). On the wire — that is, in the user's config dictionary — the value-string is `aws_iam`. The .NET typed enum exposes this as `SaslOauthbearerMetadataAuthenticationType.AwsIam`.

The enum currently lives in auto-generated [Config_gen.cs:209-220](src/Confluent.Kafka/Config_gen.cs#L209-L220) and contains only `None` and `AzureIMDS` (mirroring librdkafka's two values). We add `AwsIam` **on the .NET side only** via [src/ConfigGen/Program.cs](src/ConfigGen/Program.cs):

1. Add the `aws_iam` value to the librdkafka-extracted enum list for `sasl.oauthbearer.metadata.authentication.type` via a small `DotnetOnlyEnumValues` injection dictionary (parallel to the existing `AdditionalEnums` mechanism at [Program.cs:24](src/ConfigGen/Program.cs#L24)).
2. Add the wire-↔-CLR substitute mapping `{ "aws_iam", "AwsIam" }` to [`EnumNameToConfigValueSubstitutes`](src/Confluent.Kafka/Config.cs) — same shape as the existing `{ "azure_imds", "AzureIMDS" }` entry at [ConfigGen/Program.cs:323](src/ConfigGen/Program.cs#L323).
3. Re-run ConfigGen. The output `Config_gen.cs` now has `None`, `AzureIMDS`, **and** `AwsIam` in the enum, with the on-wire value of `AwsIam` correctly serialising as `aws_iam` via [`Config.SetObject`](src/Confluent.Kafka/Config.cs#L176-L195).

**Strip mechanism — conditional on the value, not the key.** librdkafka knows the key `sasl.oauthbearer.metadata.authentication.type` (it has a C-side handler for `azure_imds`), but does not yet know the value `aws_iam` and would reject it at `rd_kafka_new`. So we strip **only when the value is `aws_iam`**:

| Config dict entry | Action before native | Reason |
|---|---|---|
| key absent | pass through | no marker, no autowire |
| `...metadata.authentication.type=none` | pass through | librdkafka knows it |
| `...metadata.authentication.type=azure_imds` | pass through | librdkafka handles Azure IMDS C-side |
| `...metadata.authentication.type=aws_iam` | **strip** + activate .NET autowire | librdkafka would reject; we handle it |

The strip is a one-liner in `Producer.cs` / `Consumer.cs` / `AdminClient.cs` config flow, modelled on the existing `.Where(...)` patterns at [Producer.cs:593-595](src/Confluent.Kafka/Producer.cs#L593-L595) and [Consumer.cs:667](src/Confluent.Kafka/Consumer.cs#L667). Pseudocode:

```csharp
config = config.Where(prop => !(
    prop.Key == "sasl.oauthbearer.metadata.authentication.type" &&
    prop.Value == "aws_iam"));
```

When librdkafka eventually ships native `aws_iam` support, this single line gets removed and the rest of the .NET path (reflection autowire) keeps working unchanged — the autowire activation is upstream of the strip.

### 3a. Why this over a `dotnet.`-prefixed key

| Property | `sasl.oauthbearer.metadata.authentication.type=aws_iam` (this plan) | `dotnet.oauthbearer.aws.enable=true` (alternative) |
|---|---|---|
| Cross-language consistency with Python | Aligned from day one | Diverges |
| Requires librdkafka PR + release | No | No |
| ConfigGen change | Small additive (one dict entry + one substitute mapping) | Adds new constant + new typed setter |
| Strip rule | Value-conditional (one tuple test) | Key-only (existing `.Where(prop.Key != X)` shape) |
| Future migration cost when librdkafka ships native `aws_iam` | Zero — strip becomes a no-op and disappears in a follow-up | Config-key rename + deprecation cycle |
| Public API surface added | Just a new enum value | New typed property on `ClientConfig` |

The conditional-strip is marginally fancier than the existing key-only strips, but it pays back immediately: the user-visible API matches Python forever, and the librdkafka-coordination question becomes a quiet cleanup later instead of a user-facing rename.

## 4. What lives in `Confluent.Kafka` core

Single new internal namespace `Confluent.Kafka.Internal.OAuthBearer.Aws` (internal, not public API).

```
src/Confluent.Kafka/
├── Internal/
│   └── OAuthBearer/
│       └── Aws/
│           ├── AwsAutoWire.cs            ~120 LoC — entry point + reflection cache
│           ├── AwsOAuthBearerConfig.cs    ~60 LoC — internal struct, parses sasl.oauthbearer.config
│           ├── JwtSubjectExtractor.cs    ~100 LoC — verbatim copy from existing typed package
│           └── StsReflectionShim.cs      ~150 LoC — type/method/property lookups, Invoke wrappers
└── (existing files, builders modified — see §6)
```

Total estimated addition to core: ~430 LoC of new code, plus ~30 LoC of dispatch in three builders. No new public API.

### 4a. Why `internal`, not `public`

The shim and config struct are implementation detail. Exposing them risks being asked to maintain a public reflection API (`OAuthBearerProviderRegistry`, etc.) — Section 5b in [DESIGN_OAUTHBEARER_AUTOWIRE.md](DESIGN_OAUTHBEARER_AUTOWIRE.md) already rejected that. Keeping the entire path `internal` preserves the option to remove or restructure later.

### 4b. Tests

- `test/Confluent.Kafka.UnitTests/OAuthBearer/Aws/*Tests.cs` — uses `InternalsVisibleTo` (already present in the csproj for existing internal tests).

No `test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/` — that directory only made sense for the typed-package design.

## 5. Reflection target inventory

Hard-coded type-name and method-name strings core takes on as a maintenance liability:

| Symbol | Kind | Purpose |
|---|---|---|
| `AWSSDK.SecurityToken` | Assembly simple name | `Assembly.Load` target |
| `Amazon.SecurityToken.AmazonSecurityTokenServiceClient` | Type | STS client |
| `Amazon.SecurityToken.AmazonSecurityTokenServiceConfig` | Type | STS client config |
| `Amazon.SecurityToken.Model.GetWebIdentityTokenRequest` | Type | Request DTO |
| `Amazon.SecurityToken.Model.GetWebIdentityTokenResponse` | Type | Response DTO |
| `GetWebIdentityTokenAsync(GetWebIdentityTokenRequest, CancellationToken)` | Method | The STS call |
| `Audience`, `SigningAlgorithm`, `DurationSeconds` | Request properties | Set on request |
| `WebIdentityToken`, `Expiration` | Response properties | Read from response |
| `Amazon.RegionEndpoint` + `GetBySystemName(string)` | Type + static method | Region resolution |
| `Amazon.Runtime.FallbackCredentialsFactory` + `GetCredentials()` | Type + static method | Default credential chain |
| `RegionEndpoint`, `ServiceURL` | `AmazonSecurityTokenServiceConfig` properties | STS client config |

All resolved once on first invocation, cached in static fields. Resolution failures throw `MissingMethodException` / `TypeLoadException` wrapped in a friendly `InvalidOperationException` that names the AWS SDK version floor.

Floor pinned by **runtime version check**: read `AssemblyName.Version` of the loaded `AWSSDK.SecurityToken`, compare against `3.7.504`. Reject older with a clear message naming the upgrade command. (No build-time `<PackageReference>` floor — that's the cost of going package-less.)

## 6. `Build()` integration in the three builders

Each of [ProducerBuilder.cs:117-137](src/Confluent.Kafka/ProducerBuilder.cs#L117-L137), [ConsumerBuilder.cs:90-121](src/Confluent.Kafka/ConsumerBuilder.cs#L90-L121), [AdminClient.cs:1581](src/Confluent.Kafka/AdminClient.cs#L1581) gains the same dispatch sketch in its `ConstructBaseConfig` (or AdminClient's equivalent):

```csharp
// Pseudocode — adapted to each builder's generic shape.
oAuthBearerTokenRefreshHandler = ResolveOAuthBearerHandler(producer);

private Action<string> ResolveOAuthBearerHandler(IProducer<TKey, TValue> producer)
{
    // Explicit handler always wins.
    if (this.OAuthBearerTokenRefreshHandler != null)
    {
        return cfg => this.OAuthBearerTokenRefreshHandler(producer, cfg);
    }

    // Else: marker triggers reflection autowire.
    var auto = AwsAutoWire.TryCreateHandler(this.Config);
    if (auto != null)
    {
        return cfg => auto(producer, cfg);
    }

    return null; // No handler — librdkafka's default unsecured-JWT path applies.
}
```

`AwsAutoWire.TryCreateHandler(IEnumerable<KeyValuePair<string,string>> cfg)`:

1. Look for `sasl.oauthbearer.metadata.authentication.type=aws_iam`. If the key is absent or its value is anything other than `aws_iam` (e.g. `azure_imds`, `none`), return `null` immediately (zero AWS work; pass-through to librdkafka unchanged).
2. `Assembly.Load("AWSSDK.SecurityToken")` — `FileNotFoundException` → friendly `InvalidOperationException`:
   ```
   Config 'sasl.oauthbearer.metadata.authentication.type=aws_iam' requires
   AWSSDK.SecurityToken (>= 3.7.504) to be referenced. Add:
       <PackageReference Include="AWSSDK.SecurityToken" Version="3.7.504" />
   ```
3. Resolve all reflection targets (§5). Cache `Type` / `MethodInfo` / `PropertyInfo` references in static fields.
4. Parse `sasl.oauthbearer.config` into typed args (region, audience, signing algorithm, duration, sts endpoint override, principal name override, sasl extensions key=value pairs).
5. Pre-construct an `AmazonSecurityTokenServiceClient` instance and store it in a process-wide cache keyed by parsed config — one client per unique (region, endpoint override, credentials-resolution-mode) tuple. Sharing across all `Confluent.Kafka` clients in a process keeps STS connection pooling effective.
6. Return an `Action<IClient, string>` that invokes STS via the cached reflection targets, parses the response, calls `OAuthBearerSetToken` / `OAuthBearerSetTokenFailure`.

Steps 2–5 happen on the `Build()` thread, eagerly. Step 6 runs every refresh (5–60 min cadence) on librdkafka's background thread.

### 6a-pre. Stripping `aws_iam` before librdkafka sees it

The key `sasl.oauthbearer.metadata.authentication.type` is recognised by librdkafka, but the **value** `aws_iam` is not — `rd_kafka_new` would reject it with `Invalid value for configuration property "sasl.oauthbearer.metadata.authentication.type"`. Strip the entry only when the value matches `aws_iam`, leaving `none` / `azure_imds` untouched:

- Add a `.Where(prop => !IsAwsIamMarker(prop))` filter alongside the existing strips at [Producer.cs:593-595](src/Confluent.Kafka/Producer.cs#L593-L595), [Consumer.cs:667](src/Confluent.Kafka/Consumer.cs#L667), and the equivalent point in [AdminClient.cs](src/Confluent.Kafka/AdminClient.cs).
- `IsAwsIamMarker` is a single helper:
   ```csharp
   private static bool IsAwsIamMarker(KeyValuePair<string, string> prop)
       => prop.Key == "sasl.oauthbearer.metadata.authentication.type"
          && string.Equals(prop.Value, "aws_iam", StringComparison.OrdinalIgnoreCase);
   ```

`AwsAutoWire.TryCreateHandler` reads the original (un-stripped) config because it runs in `ConstructBaseConfig` before the strip; the strip only matters for the dictionary that flows to native code.

When librdkafka eventually adds native `aws_iam` support, this strip becomes a no-op (librdkafka would accept the value too) and gets removed in cleanup. The .NET reflection path keeps minting tokens unchanged.

### 6a. `sasl.oauthbearer.config` grammar

Space-separated `key=value` pairs (matches librdkafka's existing parsing of this string for OIDC/SASL extensions):

```
region=us-east-1 audience=https://confluent.cloud/oidc duration_seconds=3600 \
sts_endpoint=https://sts-fips.us-east-1.amazonaws.com signing_algorithm=ES384 \
principal_name=my-principal extension_logicalCluster=lkc-abc \
extension_identityPoolId=pool-xyz
```

Recognised keys:

| Key | Required | Type | Notes |
|---|---|---|---|
| `region` | yes | string | No silent default. |
| `audience` | yes | string | Single value. |
| `duration_seconds` | no | int 60–3600 | Default 300. |
| `signing_algorithm` | no | `ES384`\|`RS256` | Default `ES384`. |
| `sts_endpoint` | no | URL | FIPS / VPC override. |
| `principal_name` | no | string | Override JWT `sub`. |
| `extension_<name>` | no | string | Each becomes a `SaslExtensions` entry under `<name>`. |

Validation errors throw at `Build()` (synchronous) — same surface as the typed package's `AwsOAuthBearerConfig.Validate()`.

### 6b. Threading

- `AwsAutoWire.TryCreateHandler` is called once per `Build()`, on the user thread.
- Static reflection cache is populated under a `lock`. After first population, fast-path is lock-free reads.
- The returned handler runs on librdkafka's refresh thread. Bridges async via `.GetAwaiter().GetResult()` — same justification as [DESIGN_AWS_OAUTHBEARER.md §5e](DESIGN_AWS_OAUTHBEARER.md#5e-bridging-async--the-sync-handler-delegate).
- The shared `AmazonSecurityTokenServiceClient` instance is thread-safe per AWS SDK contract.

## 7. Optional-deps invariant

Same proof as [DESIGN_AWS_OAUTHBEARER.md §4d](DESIGN_AWS_OAUTHBEARER.md#4d-dep-graph-invariant-proof-2026-04-22-m1-exit-gate), reduced to the simpler shape:

| Scenario | `dotnet list package --include-transitive` AWSSDK rows | `bin/Release/net8.0/AWSSDK*.dll` count | `Build()` outcome |
|---|---|---|---|
| Opt-out (no `AWSSDK.SecurityToken` reference, no marker) | 0 | 0 | No-op; AWS path never enters memory. |
| Opt-out, marker set | 0 | 0 | Friendly `InvalidOperationException` from `Assembly.Load`. |
| Opt-in (`AWSSDK.SecurityToken` referenced + marker set) | 2 (`AWSSDK.Core` + `AWSSDK.SecurityToken`) | 2 | Reflection handler wired, token mints. |
| Opt-in but marker absent | 2 (referenced for unrelated reason) | 2 | No-op; `Assembly.Load` never fires. |

`Confluent.Kafka.csproj` adds **zero** `PackageReference` entries to AWS. `dotnet list package --include-transitive` on a project that only references `Confluent.Kafka` shows no AWS rows. This is the same invariant that holds today.

Verified at M1 exit (see §10).

## 8. Error model

| Stage | Failure | Surface |
|---|---|---|
| `Build()`, marker set, assembly missing | `FileNotFoundException` from `Assembly.Load` | `InvalidOperationException` with install command, thrown synchronously from `Build()`. |
| `Build()`, assembly old (< 3.7.504) | `Version` check fails | `InvalidOperationException` naming exact required version, thrown synchronously. |
| `Build()`, reflection target missing (`MissingMethodException` / `TypeLoadException`) | One of §5 lookups returns null | `InvalidOperationException` naming the missing symbol; advise upgrading `AWSSDK.SecurityToken`. Thrown synchronously. |
| `Build()`, `sasl.oauthbearer.config` invalid | Parse failure | `ArgumentException` from the parser, naming the offending key. |
| Refresh, STS rejects (`AccessDenied`, `OutboundWebIdentityFederationDisabled`) | AWS SDK throws | Caught, fed to `OAuthBearerSetTokenFailure(ex.ToString())`. |
| Refresh, JWT malformed | `JwtSubjectExtractor.ExtractSub` throws `FormatException` | Caught, fed to `OAuthBearerSetTokenFailure`. |

All `Build()`-time errors fail loudly and synchronously. All refresh-time errors flow through the same `OAuthBearerSetTokenFailure` channel that user-supplied handlers use today.

## 9. Testing strategy

Three layers, all in `test/Confluent.Kafka.UnitTests/`.

### 9a. Unit — config parser
`AwsOAuthBearerConfigParserTests.cs`. Fixtures for required-key omission, malformed `duration_seconds`, signing algorithm whitelist, extension keys, principal name override, ordering invariance. ~25 cases.

### 9b. Unit — JWT subject extractor
`JwtSubjectExtractorTests.cs`. **Reuse the verbatim test fixtures from the existing typed-package test file** in [test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/JwtSubjectExtractorTests.cs](../21_April/confluent-kafka-dotnet/test/Confluent.Kafka.OAuthBearer.Aws.UnitTests/JwtSubjectExtractorTests.cs). Coverage stays identical.

### 9c. Unit — reflection shim
`StsReflectionShimTests.cs`. Two flavours:
- **Real-AWS-SDK harness:** test project references `AWSSDK.SecurityToken 3.7.504` and uses an in-process mock `IAmazonSecurityTokenService` (the SDK ships this interface). Verifies the shim's reflection lookups bind to real types and that `MethodInfo.Invoke` round-trips request → mock response → parsed token. ~10 cases.
- **Synthetic-mismatch harness:** load a tiny stub `AWSSDK.SecurityToken.dll` built at test-time with a deliberately wrong method signature. Verifies the friendly `InvalidOperationException` fires with the correct symbol name. ~3 cases.

### 9d. Unit — Build() dispatch
`AwsAutoWireDispatchTests.cs`. Three scenarios from §7. Asserts:
- explicit `SetOAuthBearerTokenRefreshHandler` always wins,
- marker absent → no `Assembly.Load` call (probed via a custom `AssemblyLoadContext` instrumented to record loads),
- marker present + assembly missing → `InvalidOperationException` from `Build()`.

### 9e. Integration (real STS, opt-in)
`Confluent.Kafka.IntegrationTests/AwsOAuthBearerReflectionIntegrationTests.cs`, gated by `RUN_AWS_STS_REAL=1`. Same EC2 box / role as the librdkafka probe (`eu-north-1`, `ktrue-iam-sts-test-role`). Verifies the reflection path mints a JWT with the same `sub` and Audience as the typed-package path did. Off by default in CI; run before each release.

### 9f. Optional-deps invariant test
A throwaway `net8.0` console project under `test/integration/` referencing only `Confluent.Kafka`. CI script runs `dotnet list package --include-transitive | grep AWSSDK` and asserts zero rows. Same script on a project that adds `AWSSDK.SecurityToken` asserts exactly two rows. Mirrors the M1 exit gate from the typed-package plan, repurposed.

## 10. Milestones

Single track — no parallel typed-package work, no librdkafka coordination. Six milestones; estimate **~10–12 engineer-days end-to-end**.

| # | Deliverable | Estimate | Exit gate |
|---|---|---|---|
| **R1** | Scaffold `src/Confluent.Kafka/Internal/OAuthBearer/Aws/` with empty types. Extend `SaslOauthbearerMetadataAuthenticationType` with `AwsIam` via [ConfigGen/Program.cs](src/ConfigGen/Program.cs) (inject the value, add the `{ "aws_iam", "AwsIam" }` substitute), regenerate [Config_gen.cs](src/Confluent.Kafka/Config_gen.cs), then add the value-conditional strip helper to `Producer.cs` / `Consumer.cs` / `AdminClient.cs`. Verify optional-deps invariant test (§9f) passes. | 1 day. | `dotnet build` clean; invariant test green; setting `SaslOauthbearerMetadataAuthenticationType = AwsIam` on a config does not cause `rd_kafka_new` to reject (proven by a unit test that goes through the full config pipeline up to native handoff); `AzureIMDS` and `None` still pass through to librdkafka unchanged. |
| **R2** | Implement `AwsOAuthBearerConfig` parser + `JwtSubjectExtractor` (port verbatim from [21_April typed package](../21_April/confluent-kafka-dotnet/src/Confluent.Kafka.OAuthBearer.Aws/)). Unit tests §9a + §9b passing. | 1.5 days. | `make test` passes. |
| **R3** | Implement `StsReflectionShim` — type/method/property lookups, request/response marshalling via reflection, version floor check. Unit tests §9c passing. | 3 days. | Real-AWS-SDK harness green; synthetic-mismatch harness green. |
| **R4** | Implement `AwsAutoWire.TryCreateHandler` + dispatch in `ProducerBuilder` / `ConsumerBuilder` / `AdminClientBuilder`. Process-wide STS client cache. Unit tests §9d passing. | 2 days. | All three builders pass autowire; explicit-handler-wins precedence verified. |
| **R5** | Real-AWS integration test (§9e) on EC2 box. Document the marker key + config grammar in [README.md](README.md). Add `CHANGELOG.md` entry. | 2 days. | JWT minted on EC2, decoded `sub` matches role ARN, broker auth succeeds against test cluster. |
| **R6** | Code review + merge. | Buffer (~1 day). | Merged. |

### 10b. Post-validation follow-ups (not in this plan)
- Open a librdkafka PR adding `aws_iam` to the C-side enum for `sasl.oauthbearer.metadata.authentication.type` (no-op handler, just accept the value). Once shipped, **remove** the .NET-side strip helper added in R1; the .NET reflection autowire continues to mint tokens, only now librdkafka also accepts the value at config-validation time. No user-visible change. Pure cleanup.
- AWS SDK v3.7 → v4 migration plan (open question 1 in [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md)).
- Revisit `AWSCredentials` injection / rich `SaslExtensions` if customer demand surfaces.

### 10a. Deferred (not in scope for first ship)
- AWS SDK v4 dual-version support (open question 1 in [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md)). v3.7 only at first ship.
- A typed `Credentials` injection escape hatch — explicitly out of scope per §1 non-goals; revisit if customer demand surfaces.
- Documentation of how to write a custom (non-AWS) reflection autowire — not exposed.

## 11. Documentation impact

- [README.md](README.md) — new "AWS OAUTHBEARER (autowire)" section. ~30 lines: prereq packages, marker key, config grammar table, single example.
- [CHANGELOG.md](CHANGELOG.md) — one-line entry under the next version.
- No new design doc — this plan **is** the design once approved. [DESIGN_AWS_OAUTHBEARER.md](DESIGN_AWS_OAUTHBEARER.md) and [IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md](IMPLEMENTATION_PLAN_AWS_OAUTHBEARER.md) become historical (mark them `**Status: superseded by IMPLEMENTATION_PLAN_OAUTHBEARER_REFLECTION_AUTOWIRE.md**` at the top, leave the bodies for archeology).

## 12. Rollback plan

If post-merge a critical issue surfaces (e.g. AWS SDK ships a 3.7.x patch that renames a property and breaks reflection):

- Hotfix path: revert the marker dispatch in [ProducerBuilder.cs](src/Confluent.Kafka/ProducerBuilder.cs) / [ConsumerBuilder.cs](src/Confluent.Kafka/ConsumerBuilder.cs) / [AdminClientBuilder.cs](src/Confluent.Kafka/AdminClientBuilder.cs) so `TryCreateHandler` is never invoked. Marker becomes a silent no-op. Ship a patch release.
- Reverting only those three call sites avoids removing all the supporting code (parser, JWT extractor, shim) — keeps the surface area to roll forward small once the SDK drift is fixed.

## 13. Resolved & open questions

**Resolved (vs. the §2f Concerns in the design doc):**
- **Concern 1** (cloud-glue-in-core precedent) — collapses. Azure is in librdkafka C-side; this plan is AWS-only forever. No precedent set.

**Still open:**
- **Concern 2** (reflection fragility across SDK versions) — mitigated by the runtime version floor check (§5) and synthetic-mismatch tests (§9c), but not eliminated. Acknowledged maintenance cost.
- **Concern 3** (no `AWSCredentials` injection / rich `SaslExtensions`) — accepted. Scoped out per §1 non-goals.
- **Q3 in the design doc** (AWS SDK v3.7 → v4) — defer to a follow-up plan once `Confluent.SchemaRegistry.Encryption.Aws` migrates.
- **Q4 in the design doc** (cross-language marker key) — **resolved at validation time**. Use `sasl.oauthbearer.metadata.authentication.type=aws_iam` from day one (matches Python). The librdkafka enum extension is a *cleanup follow-up* once validation succeeds, not a prerequisite.

## 14. Summary

| Question | Answer |
|---|---|
| New NuGet? | No. |
| New public API in core? | No (everything `internal`). |
| New `PackageReference` in `Confluent.Kafka.csproj`? | No. |
| Cross-cloud reach? | AWS-only — Azure already handled in librdkafka C-side. |
| User integration cost? | Two `<PackageReference>`s + two config keys. |
| Estimated engineering effort? | ~10–12 days. No librdkafka coordination at validation time. |
| Marker key | `sasl.oauthbearer.metadata.authentication.type=aws_iam` — same as Python from day one. Enum value `AwsIam` added to the .NET-only side of `SaslOauthbearerMetadataAuthenticationType`; the value (not the key) is stripped before native handoff so librdkafka doesn't reject it. Strip becomes a no-op once librdkafka adds `aws_iam` natively. |
| Maintenance liability accepted? | Reflection-bound to AWS SDK type/method/property names; runtime version floor catches drift. |
