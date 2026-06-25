// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;

namespace Confluent.Kafka.OAuthBearer.Aws.Internal
{
    /// <summary>
    ///     Fetches OAUTHBEARER tokens via AWS STS <c>GetWebIdentityToken</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         The underlying <see cref="IAmazonSecurityTokenService"/>
    ///         is thread-safe.
    ///     </para>
    ///     <para>
    ///         Credential resolution is lazy during construction and happens
    ///         during the first call to <see cref="GetTokenAsync"/>.
    ///     </para>
    /// </remarks>
    internal sealed class AwsStsTokenProvider : IDisposable
    {
        private readonly AwsOAuthBearerConfig _cfg;
        private readonly IAmazonSecurityTokenService _sts;
        private readonly bool _ownsClient;

        /// <summary>
        ///     Owns a fresh <see cref="AmazonSecurityTokenServiceClient"/> for the
        ///     parsed config's <see cref="AwsOAuthBearerConfig.Region"/>. The config
        ///     must already be validated by <see cref="AwsOAuthBearerConfig.Parse"/>.
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
        internal AwsStsTokenProvider(AwsOAuthBearerConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            _cfg = config;

            if (config.AwsDebug != LoggingOptions.None)
            {
                AWSConfigs.LoggingConfig.LogTo = config.AwsDebug;
            }

            var awsConfig = new AmazonSecurityTokenServiceConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(config.Region),
            };
            if (!string.IsNullOrEmpty(config.StsEndpointOverride))
            {
                awsConfig.ServiceURL = config.StsEndpointOverride;
            }

            _sts = new AmazonSecurityTokenServiceClient(awsConfig);
            _ownsClient = true;
        }

        /// <summary>
        ///     Test-only constructor that injects an STS client. The caller is
        ///     responsible for the client's lifetime.
        /// </summary>
        internal AwsStsTokenProvider(AwsOAuthBearerConfig config, IAmazonSecurityTokenService sts)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (sts == null) throw new ArgumentNullException(nameof(sts));
            _cfg = config;
            _sts = sts;
            _ownsClient = false;
        }

        /// <summary>
        ///     Calls STS <c>GetWebIdentityToken</c> and returns a fresh token
        ///     record.
        /// </summary>
        /// <exception cref="AmazonSecurityTokenServiceException">
        ///     STS rejected the call (e.g. <c>AccessDenied</c>,
        ///     <c>OutboundWebIdentityFederationDisabled</c>).
        /// </exception>
        /// <exception cref="FormatException">
        ///     The returned JWT is malformed (thrown by <see cref="JwtExtractor"/>).
        /// </exception>
        /// <exception cref="OperationCanceledException">
        ///     <paramref name="ct"/> was cancelled.
        /// </exception>
        internal async Task<OAuthBearerToken> GetTokenAsync(CancellationToken ct = default)
        {
            var request = new GetWebIdentityTokenRequest
            {
                Audience = new List<string> { _cfg.Audience },
                SigningAlgorithm = _cfg.SigningAlgorithm,
                DurationSeconds = (int)_cfg.Duration.TotalSeconds,
            };

            if (_cfg.Tags != null)
            {
                request.Tags = new List<Tag>();
                foreach (var kv in _cfg.Tags)
                {
                    request.Tags.Add(new Tag { Key = kv.Key, Value = kv.Value });
                }
            }

            var response = await _sts.GetWebIdentityTokenAsync(request, ct)
                .ConfigureAwait(false);

            if (response.Expiration == default(DateTime))
            {
                throw new InvalidOperationException(
                    "STS response missing Expiration; cannot compute token lifetime.");
            }

            // AWS returns UTC; SpecifyKind normalizes regardless of the SDK's
            // DateTimeKind, so DateTimeOffset conversion is well-defined.
            var expiryUtc = DateTime.SpecifyKind(response.Expiration, DateTimeKind.Utc);
            var lifetimeMs = new DateTimeOffset(expiryUtc).ToUnixTimeMilliseconds();

            var principal = JwtExtractor.ExtractSub(response.WebIdentityToken);

            return new OAuthBearerToken(
                response.WebIdentityToken, lifetimeMs, principal, _cfg.SaslExtensions);
        }

        /// <summary>
        ///     Disposes the underlying STS client if this provider owns it
        ///     (i.e. was constructed via the public constructor).
        /// </summary>
        public void Dispose()
        {
            if (_ownsClient)
            {
                _sts.Dispose();
            }
        }
    }
}
