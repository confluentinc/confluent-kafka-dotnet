using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;

namespace Confluent.Kafka.OAuthBearer.Aws
{
    /// <summary>
    ///     Fetches OAUTHBEARER tokens via AWS STS <c>GetWebIdentityToken</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Thread-safe. The underlying <see cref="IAmazonSecurityTokenService"/>
    ///         is safe for concurrent use per AWS SDK conventions, and all per-call
    ///         state is stack-local.
    ///     </para>
    ///     <para>
    ///         Credential resolution is lazy — the constructor does not call the
    ///         AWS credential chain. The first <see cref="GetTokenAsync"/> call
    ///         triggers resolution. This matches AWS SDK conventions across
    ///         languages and avoids startup latency when the caller uses an
    ///         endpoint-probing provider (IMDSv2, SSO).
    ///     </para>
    /// </remarks>
    public sealed class AwsStsTokenProvider : IDisposable
    {
        private readonly AwsOAuthBearerConfig _cfg;
        private readonly IAmazonSecurityTokenService _sts;
        private readonly bool _ownsClient;

        /// <summary>
        ///     Constructs a provider that owns a fresh
        ///     <see cref="AmazonSecurityTokenServiceClient"/>.
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
        /// <exception cref="ArgumentException">
        ///     <paramref name="config"/> fails <see cref="AwsOAuthBearerConfig.Validate"/>.
        /// </exception>
        public AwsStsTokenProvider(AwsOAuthBearerConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            config.Validate();
            config.ApplyDefaults();
            _cfg = config;

            var awsConfig = new AmazonSecurityTokenServiceConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(config.Region),
            };
            if (!string.IsNullOrEmpty(config.StsEndpointOverride))
            {
                awsConfig.ServiceURL = config.StsEndpointOverride;
            }

            _sts = config.Credentials != null
                ? new AmazonSecurityTokenServiceClient(config.Credentials, awsConfig)
                : new AmazonSecurityTokenServiceClient(awsConfig);
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
            config.Validate();
            config.ApplyDefaults();
            _cfg = config;
            _sts = sts;
            _ownsClient = false;
        }

        /// <summary>
        ///     Calls STS <c>GetWebIdentityToken</c> and returns a fresh token
        ///     record. Throws on validation / STS / JWT-parse failures; the
        ///     caller is responsible for turning exceptions into
        ///     <c>OAuthBearerSetTokenFailure</c> if invoked from a refresh
        ///     handler.
        /// </summary>
        /// <exception cref="AmazonSecurityTokenServiceException">
        ///     STS rejected the call (e.g. <c>AccessDenied</c>,
        ///     <c>OutboundWebIdentityFederationDisabled</c>).
        /// </exception>
        /// <exception cref="FormatException">
        ///     The returned JWT is malformed (thrown by <see cref="JwtSubjectExtractor"/>).
        /// </exception>
        /// <exception cref="OperationCanceledException">
        ///     <paramref name="ct"/> was cancelled.
        /// </exception>
        public async Task<AwsOAuthBearerToken> GetTokenAsync(CancellationToken ct = default)
        {
            var request = new GetWebIdentityTokenRequest
            {
                Audience = new List<string> { _cfg.Audience },
                SigningAlgorithm = _cfg.SigningAlgorithm,
                DurationSeconds = (int)_cfg.Duration.TotalSeconds,
            };

            var response = await _sts.GetWebIdentityTokenAsync(request, ct)
                .ConfigureAwait(false);

            // AWS returns UTC; SpecifyKind normalizes regardless of the SDK's
            // DateTimeKind, so DateTimeOffset conversion is well-defined.
            var expiryUtc = DateTime.SpecifyKind(response.Expiration, DateTimeKind.Utc);
            var lifetimeMs = new DateTimeOffset(expiryUtc).ToUnixTimeMilliseconds();

            var principal = _cfg.PrincipalNameOverride
                            ?? JwtSubjectExtractor.ExtractSub(response.WebIdentityToken);

            return new AwsOAuthBearerToken(
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
