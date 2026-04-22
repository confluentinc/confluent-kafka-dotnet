using System;
using System.Collections.Generic;
using Amazon.Runtime;

namespace Confluent.Kafka.OAuthBearer.Aws
{
    /// <summary>
    ///     Parameters for minting an OAUTHBEARER token via AWS STS
    ///     <c>GetWebIdentityToken</c>. Consumed by <see cref="AwsStsTokenProvider"/>.
    /// </summary>
    /// <remarks>
    ///     All fields are plain properties. The provider calls
    ///     <see cref="Validate"/> then <see cref="ApplyDefaults"/> at construction;
    ///     misconfiguration surfaces synchronously as an
    ///     <see cref="ArgumentException"/> rather than on the first token fetch.
    /// </remarks>
    public sealed class AwsOAuthBearerConfig
    {
        internal const string DefaultSigningAlgorithm = "ES384";
        internal static readonly TimeSpan DefaultDuration = TimeSpan.FromSeconds(300);
        internal static readonly TimeSpan MinDuration = TimeSpan.FromSeconds(60);
        internal static readonly TimeSpan MaxDuration = TimeSpan.FromSeconds(3600);

        /// <summary>
        ///     AWS region to target (e.g. <c>"us-east-1"</c>). Required; no silent
        ///     default, no IMDS sniffing. STS is a regional service and misrouting
        ///     is a common and noisy failure, so region is explicit by contract.
        /// </summary>
        public string Region { get; set; }

        /// <summary>
        ///     OIDC audience claim the relying party expects. Required. Passed
        ///     through to <c>GetWebIdentityTokenRequest.Audience[0]</c>.
        /// </summary>
        public string Audience { get; set; }

        /// <summary>
        ///     Signing algorithm the STS-minted JWT will use. Defaults to
        ///     <c>"ES384"</c>. The only other allowed value is <c>"RS256"</c>.
        /// </summary>
        public string SigningAlgorithm { get; set; }

        /// <summary>
        ///     Requested token lifetime. Must be between 60 seconds and 3600
        ///     seconds inclusive (AWS-enforced). Defaults to 300 seconds when
        ///     unset or zero.
        /// </summary>
        public TimeSpan Duration { get; set; }

        /// <summary>
        ///     Optional STS endpoint URL. Use for FIPS endpoints
        ///     (<c>sts-fips.us-east-1.amazonaws.com</c>) or VPC endpoints. If
        ///     unset, the AWS SDK resolves the regional endpoint from
        ///     <see cref="Region"/>.
        /// </summary>
        public string StsEndpointOverride { get; set; }

        /// <summary>
        ///     Explicit AWS credentials to sign the STS call with. If <c>null</c>,
        ///     the AWS SDK's default credential chain is used
        ///     (<c>FallbackCredentialsFactory</c>): env vars &#8594;
        ///     assume-role-with-web-identity &#8594; ECS container credentials
        ///     &#8594; IMDSv2 &#8594; profile.
        /// </summary>
        public AWSCredentials Credentials { get; set; }

        /// <summary>
        ///     Optional SASL extensions that the broker receives alongside the
        ///     token (RFC 7628 §3.1). Typical Confluent Cloud keys are
        ///     <c>logicalCluster</c> and <c>identityPoolId</c>.
        /// </summary>
        public IDictionary<string, string> SaslExtensions { get; set; }

        /// <summary>
        ///     When set, this value is used as the OAUTHBEARER principal name
        ///     instead of the JWT's <c>sub</c> claim. Leave <c>null</c> to
        ///     extract <c>sub</c> (the bare role ARN) from the minted token.
        /// </summary>
        public string PrincipalNameOverride { get; set; }

        /// <summary>
        ///     Throws <see cref="ArgumentException"/> if any field is invalid.
        ///     Does not mutate; safe to call repeatedly.
        /// </summary>
        internal void Validate()
        {
            if (string.IsNullOrEmpty(Region))
            {
                throw new ArgumentException(
                    $"{nameof(AwsOAuthBearerConfig)}.{nameof(Region)} is required.");
            }
            if (string.IsNullOrEmpty(Audience))
            {
                throw new ArgumentException(
                    $"{nameof(AwsOAuthBearerConfig)}.{nameof(Audience)} is required.");
            }
            if (SigningAlgorithm != null
                && SigningAlgorithm != "ES384"
                && SigningAlgorithm != "RS256")
            {
                throw new ArgumentException(
                    $"{nameof(AwsOAuthBearerConfig)}.{nameof(SigningAlgorithm)} " +
                    $"must be \"ES384\" or \"RS256\"; got \"{SigningAlgorithm}\".");
            }
            if (Duration != default && (Duration < MinDuration || Duration > MaxDuration))
            {
                throw new ArgumentException(
                    $"{nameof(AwsOAuthBearerConfig)}.{nameof(Duration)} must be " +
                    $"between {MinDuration.TotalSeconds:F0}s and " +
                    $"{MaxDuration.TotalSeconds:F0}s inclusive; got " +
                    $"{Duration.TotalSeconds:F0}s.");
            }
            if (PrincipalNameOverride != null && PrincipalNameOverride.Length == 0)
            {
                throw new ArgumentException(
                    $"{nameof(AwsOAuthBearerConfig)}.{nameof(PrincipalNameOverride)} " +
                    "must be non-empty when set. Leave null to extract 'sub' from the JWT.");
            }
        }

        /// <summary>
        ///     Fills unset fields with their defaults. Call after <see cref="Validate"/>.
        /// </summary>
        internal void ApplyDefaults()
        {
            if (SigningAlgorithm == null)
            {
                SigningAlgorithm = DefaultSigningAlgorithm;
            }
            if (Duration == default)
            {
                Duration = DefaultDuration;
            }
        }
    }
}
