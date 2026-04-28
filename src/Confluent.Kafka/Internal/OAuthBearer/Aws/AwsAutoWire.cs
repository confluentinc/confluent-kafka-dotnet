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
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Confluent.Kafka.Internal.OAuthBearer.Aws
{
    /// <summary>
    ///     Reflection-based AWS OAUTHBEARER autowire — entry point.
    /// </summary>
    /// <remarks>
    ///     Activation marker: a config entry of the form
    ///     <c>sasl.oauthbearer.metadata.authentication.type=aws_iam</c>. The value
    ///     <c>aws_iam</c> is recognised by the .NET enum
    ///     (<see cref="SaslOauthbearerMetadataAuthenticationType.AwsIam"/>) but not
    ///     by librdkafka, so it is stripped by the producer/consumer config
    ///     pipeline before native handoff. <see cref="IsMarker"/> is the predicate
    ///     used by both the strip filter and <see cref="TryCreateHandler"/>.
    /// </remarks>
    public static class AwsAutoWire
    {
        /// <summary>Config key that activates the AWS reflection autowire path.</summary>
        public const string MarkerKey = "sasl.oauthbearer.metadata.authentication.type";

        /// <summary>On-wire marker value selecting AWS IAM authentication.</summary>
        public const string MarkerValue = "aws_iam";

        private const string SaslOauthbearerConfigKey = "sasl.oauthbearer.config";

        // Caches for shim and STS clients. Resolved once per process and reused
        // across every Confluent.Kafka client (producer/consumer/admin) the
        // process builds.
        private static StsReflectionShim s_cachedShim;
        private static readonly object s_shimLock = new object();
        private static readonly Dictionary<string, object> s_stsClientCache = new Dictionary<string, object>();
        private static readonly object s_stsClientLock = new object();

        /// <summary>
        ///     Returns <c>true</c> when the supplied config entry is the AWS IAM
        ///     activation marker. Used by Producer/Consumer config processing to
        ///     strip the entry before passing to librdkafka.
        /// </summary>
        public static bool IsMarker(KeyValuePair<string, string> prop)
            => prop.Key == MarkerKey
               && string.Equals(prop.Value, MarkerValue, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        ///     Inspects <paramref name="config"/> for the AWS IAM activation
        ///     marker. If absent, returns <c>null</c> and no AWS work is done.
        ///     If present, parses <c>sasl.oauthbearer.config</c>, binds the
        ///     reflection shim against <c>AWSSDK.SecurityToken</c>, constructs
        ///     (or fetches from cache) an STS client, and returns a refresh
        ///     handler suitable for plugging into the OAUTHBEARER refresh hook.
        /// </summary>
        /// <returns>
        ///     <c>null</c> when the marker is absent (caller falls through to
        ///     librdkafka's default). A non-null
        ///     <see cref="Action{IClient, String}"/> otherwise — the caller
        ///     bridges it to its concrete client type.
        /// </returns>
        /// <exception cref="ArgumentException">
        ///     Marker present but <c>sasl.oauthbearer.config</c> is absent or
        ///     malformed.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        ///     Marker present but <c>AWSSDK.SecurityToken</c> is not loadable, is
        ///     below the version floor, or is missing required types/methods.
        /// </exception>
        public static Action<IClient, string> TryCreateHandler(
            IEnumerable<KeyValuePair<string, string>> config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            // Snapshot once — caller may pass an enumerable that re-iterates lazily.
            var snapshot = config as IList<KeyValuePair<string, string>> ?? config.ToList();

            if (!snapshot.Any(IsMarker))
            {
                return null;
            }

            var rawSaslConfig = snapshot
                .Where(kv => kv.Key == SaslOauthbearerConfigKey)
                .Select(kv => kv.Value)
                .FirstOrDefault();

            if (string.IsNullOrEmpty(rawSaslConfig))
            {
                throw new ArgumentException(
                    $"Config '{MarkerKey}={MarkerValue}' is set but '{SaslOauthbearerConfigKey}' " +
                    "is missing or empty. The autowire path requires region and audience to be " +
                    "supplied via sasl.oauthbearer.config (e.g. \"region=us-east-1 audience=https://...\").");
            }

            var parsed = AwsOAuthBearerConfig.Parse(rawSaslConfig);
            var shim = GetOrLoadShim();
            var stsClient = GetOrCreateStsClient(shim, parsed.Region, parsed.StsEndpointOverride);
            var durationSeconds = (int)parsed.Duration.TotalSeconds;

            return (client, _ignoredOAuthConfigString) =>
            {
                try
                {
                    // librdkafka invokes refresh callbacks on its own background
                    // thread with no SynchronizationContext, so blocking here is
                    // safe.
                    var result = shim.GetWebIdentityTokenAsync(
                        stsClient,
                        parsed.Audience,
                        parsed.SigningAlgorithm,
                        durationSeconds,
                        CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();

                    var expiryUtc = DateTime.SpecifyKind(result.Expiration, DateTimeKind.Utc);
                    var lifetimeMs = new DateTimeOffset(expiryUtc).ToUnixTimeMilliseconds();
                    var principal = parsed.PrincipalNameOverride
                                    ?? JwtSubjectExtractor.ExtractSub(result.Jwt);

                    client.OAuthBearerSetToken(
                        result.Jwt, lifetimeMs, principal, parsed.SaslExtensions);
                }
                catch (Exception ex)
                {
                    client.OAuthBearerSetTokenFailure(ex.ToString());
                }
            };
        }

        /// <summary>
        ///     Test seam: drops the cached shim and STS clients. Subsequent
        ///     <see cref="TryCreateHandler"/> calls re-resolve. Production code
        ///     should never call this — caches are process-scoped by design.
        /// </summary>
        internal static void ResetCachesForTests()
        {
            lock (s_shimLock) { s_cachedShim = null; }
            lock (s_stsClientLock) { s_stsClientCache.Clear(); }
        }

        private static StsReflectionShim GetOrLoadShim()
        {
            var s = Volatile.Read(ref s_cachedShim);
            if (s != null) return s;

            lock (s_shimLock)
            {
                if (s_cachedShim == null)
                {
                    s_cachedShim = StsReflectionShim.LoadOrThrow();
                }
                return s_cachedShim;
            }
        }

        private static object GetOrCreateStsClient(
            StsReflectionShim shim, string region, string stsEndpointOverride)
        {
            // String-keyed cache to keep the dependency surface trivial across
            // net462 / netstandard2.0 (avoids ValueTuple).
            var key = (region ?? "") + "|" + (stsEndpointOverride ?? "");

            lock (s_stsClientLock)
            {
                if (s_stsClientCache.TryGetValue(key, out var existing))
                {
                    return existing;
                }
                var client = shim.CreateStsClient(region, stsEndpointOverride);
                s_stsClientCache[key] = client;
                return client;
            }
        }
    }
}
