using System.Collections.Generic;

namespace Confluent.Kafka
{
    /// <summary>
    ///     IClient extension methods
    /// </summary>
    public static class ClientExtensions
    {
        /// <summary>
        ///     Set SASL/OAUTHBEARER token and metadata.
        ///     The SASL/OAUTHBEARER token refresh callback or
        ///     event handler should invoke this method upon
        ///     success. The extension keys must not include
        ///     the reserved key "`auth`", and all extension
        ///     keys and values must conform to the required
        ///     format as per https://tools.ietf.org/html/rfc7628#section-3.1:
        /// </summary>
        /// <param name="client">
        ///     the instance of a <see cref="IClient"/>
        /// </param>
        /// <param name="tokenValue">
        ///     the mandatory token value to set, often (but
        ///     not necessarily) a JWS compact serialization
        ///     as per https://tools.ietf.org/html/rfc7515#section-3.1.
        /// </param>
        /// <param name="lifetimeMs">
        ///     when the token expires, in terms of the number
        ///     of milliseconds since the epoch.
        /// </param>
        /// <param name="principalName">
        ///     the mandatory Kafka principal name associated
        ///     with the token.
        /// </param>
        /// <param name="extensions">
        ///     optional SASL extensions dictionary, to be
        ///     communicated to the broker as additional key-value
        ///     pairs during the initial client response as per
        ///     https://tools.ietf.org/html/rfc7628#section-3.1.
        /// </param>
        /// <seealso cref="OAuthBearerSetTokenFailure"/>
        public static void OAuthBearerSetToken(this IClient client, string tokenValue, long lifetimeMs, string principalName, IDictionary<string, string> extensions = null)
        {
            client.Handle.LibrdkafkaHandle.OAuthBearerSetToken(tokenValue, lifetimeMs, principalName, extensions);
        }

        /// <summary>
        ///     SASL/OAUTHBEARER token refresh failure indicator.
        ///     The SASL/OAUTHBEARER token refresh callback or
        ///     event handler should invoke this method upon failure.
        /// </summary>
        /// <param name="client">
        ///     the instance of a <see cref="IClient"/>
        /// </param>
        /// <param name="error">
        ///     mandatory human readable error reason for failing
        ///     to acquire a token.
        /// </param>
        /// <seealso cref="OAuthBearerSetToken"/>
        public static void OAuthBearerSetTokenFailure(this IClient client, string error)
        {
            client.Handle.LibrdkafkaHandle.OAuthBearerSetTokenFailure(error);
        }
    }
}
