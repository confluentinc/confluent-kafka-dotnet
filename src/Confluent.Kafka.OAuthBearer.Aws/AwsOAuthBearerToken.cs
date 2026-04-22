using System.Collections.Generic;

namespace Confluent.Kafka.OAuthBearer.Aws
{
    /// <summary>
    ///     A minted OAUTHBEARER token ready to be handed to
    ///     <c>IClient.OAuthBearerSetToken</c>.
    /// </summary>
    public readonly struct AwsOAuthBearerToken
    {
        /// <summary>
        ///     The JWT itself — passed verbatim to the broker as the OAUTHBEARER
        ///     <c>Bearer</c> value.
        /// </summary>
        public string TokenValue { get; }

        /// <summary>
        ///     Expiry expressed as Unix milliseconds since epoch. librdkafka uses
        ///     this to schedule the next refresh.
        /// </summary>
        public long LifetimeMs { get; }

        /// <summary>
        ///     Kafka principal name. Either the JWT's <c>sub</c> claim (bare
        ///     role ARN for AWS-minted tokens) or the user-supplied
        ///     <c>PrincipalNameOverride</c> from the config.
        /// </summary>
        public string PrincipalName { get; }

        /// <summary>
        ///     Optional SASL extensions forwarded to the broker. <c>null</c> when
        ///     no extensions are configured.
        /// </summary>
        public IDictionary<string, string> Extensions { get; }

        /// <summary>Constructs a token record. All fields are set here and immutable afterwards.</summary>
        public AwsOAuthBearerToken(
            string tokenValue,
            long lifetimeMs,
            string principalName,
            IDictionary<string, string> extensions)
        {
            TokenValue = tokenValue;
            LifetimeMs = lifetimeMs;
            PrincipalName = principalName;
            Extensions = extensions;
        }
    }
}
