using System;
using System.Collections.Generic;

namespace Confluent.Kafka.OAuthBearer.Aws
{
    /// <summary>
    ///     Glue between <see cref="AwsStsTokenProvider"/> and Confluent.Kafka's
    ///     <c>SetOAuthBearerTokenRefreshHandler</c> delegate.
    /// </summary>
    internal static class AwsOAuthBearerHandler
    {
        /// <summary>
        ///     Builds a handler suitable for the <c>ProducerBuilder</c> /
        ///     <c>ConsumerBuilder</c> / <c>AdminClientBuilder</c> refresh hook.
        ///     The returned delegate resolves a token synchronously and calls
        ///     <c>OAuthBearerSetToken</c> / <c>OAuthBearerSetTokenFailure</c> on
        ///     the supplied client.
        /// </summary>
        public static Action<IClient, string> Create(AwsStsTokenProvider provider)
        {
            if (provider == null) throw new ArgumentNullException(nameof(provider));
            return (client, _oauthConfigString) => Invoke(provider, new ClientTokenSink(client));
        }

        /// <summary>
        ///     Testable core. Resolves a token and routes the result to
        ///     <paramref name="sink"/> — either a success or a failure.
        ///     Never throws; the sink absorbs both outcomes.
        /// </summary>
        /// <remarks>
        ///     <c>.GetAwaiter().GetResult()</c> is used deliberately. The refresh
        ///     delegate is sync (<see cref="Action{T1,T2}"/>), and librdkafka
        ///     fires it on its own background thread with no captured
        ///     <c>SynchronizationContext</c>, so this cannot deadlock.
        /// </remarks>
        internal static void Invoke(AwsStsTokenProvider provider, ITokenSink sink)
        {
            try
            {
                var t = provider.GetTokenAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                sink.SetToken(t.TokenValue, t.LifetimeMs, t.PrincipalName, t.Extensions);
            }
            catch (Exception ex)
            {
                sink.SetTokenFailure(ex.ToString());
            }
        }
    }

    /// <summary>
    ///     Abstraction of the <c>OAuthBearerSetToken</c> /
    ///     <c>OAuthBearerSetTokenFailure</c> extension-method pair. Exists so
    ///     the handler's success/failure routing can be unit-tested without a
    ///     real librdkafka <c>Handle</c>.
    /// </summary>
    internal interface ITokenSink
    {
        void SetToken(
            string tokenValue,
            long lifetimeMs,
            string principalName,
            IDictionary<string, string> extensions);

        void SetTokenFailure(string error);
    }

    /// <summary>Production <see cref="ITokenSink"/> — forwards to the <see cref="IClient"/> extension methods.</summary>
    internal sealed class ClientTokenSink : ITokenSink
    {
        private readonly IClient _client;

        public ClientTokenSink(IClient client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public void SetToken(
            string tokenValue, long lifetimeMs, string principalName,
            IDictionary<string, string> extensions)
            => _client.OAuthBearerSetToken(tokenValue, lifetimeMs, principalName, extensions);

        public void SetTokenFailure(string error)
            => _client.OAuthBearerSetTokenFailure(error);
    }
}
