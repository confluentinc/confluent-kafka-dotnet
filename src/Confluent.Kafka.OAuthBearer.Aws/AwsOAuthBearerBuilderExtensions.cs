using System;

namespace Confluent.Kafka.OAuthBearer.Aws
{
    /// <summary>
    ///     Extension methods that wire an <see cref="AwsStsTokenProvider"/> into
    ///     a Confluent.Kafka client builder's OAUTHBEARER token refresh handler.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Each builder's refresh handler can be set exactly once — calling
    ///         <c>UseAwsOAuthBearer</c> twice on the same builder, or combining
    ///         it with a manual <c>SetOAuthBearerTokenRefreshHandler</c> call,
    ///         will throw <see cref="InvalidOperationException"/> on the second
    ///         setter (Confluent.Kafka's own guard).
    ///     </para>
    ///     <para>
    ///         Lifetime: the overloads that accept an
    ///         <see cref="AwsOAuthBearerConfig"/> construct an internal
    ///         <see cref="AwsStsTokenProvider"/> whose lifetime is bound to the
    ///         handler delegate, and therefore to the client built from this
    ///         builder. The AWS STS client inside will be cleaned up when the
    ///         client is disposed and the delegate is garbage-collected.
    ///         Callers who need deterministic cleanup should construct an
    ///         <see cref="AwsStsTokenProvider"/> themselves, pass it via the
    ///         provider-accepting overload, and <c>Dispose</c> it after the
    ///         client is disposed.
    ///     </para>
    /// </remarks>
    public static class AwsOAuthBearerBuilderExtensions
    {
        // -------- ProducerBuilder --------

        /// <summary>
        ///     Installs an AWS STS <c>GetWebIdentityToken</c>-backed OAUTHBEARER
        ///     refresh handler on this producer builder.
        /// </summary>
        public static ProducerBuilder<TKey, TValue> UseAwsOAuthBearer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> builder,
            AwsOAuthBearerConfig config)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            return builder.UseAwsOAuthBearer(new AwsStsTokenProvider(config));
        }

        /// <summary>
        ///     Installs an AWS STS <c>GetWebIdentityToken</c>-backed OAUTHBEARER
        ///     refresh handler on this producer builder, using a caller-supplied
        ///     provider that the caller owns.
        /// </summary>
        public static ProducerBuilder<TKey, TValue> UseAwsOAuthBearer<TKey, TValue>(
            this ProducerBuilder<TKey, TValue> builder,
            AwsStsTokenProvider provider)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            var handler = AwsOAuthBearerHandler.Create(provider);
            return builder.SetOAuthBearerTokenRefreshHandler(
                (client, oauthCfg) => handler(client, oauthCfg));
        }

        // -------- ConsumerBuilder --------

        /// <summary>
        ///     Installs an AWS STS <c>GetWebIdentityToken</c>-backed OAUTHBEARER
        ///     refresh handler on this consumer builder.
        /// </summary>
        public static ConsumerBuilder<TKey, TValue> UseAwsOAuthBearer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> builder,
            AwsOAuthBearerConfig config)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            return builder.UseAwsOAuthBearer(new AwsStsTokenProvider(config));
        }

        /// <summary>
        ///     Installs an AWS STS <c>GetWebIdentityToken</c>-backed OAUTHBEARER
        ///     refresh handler on this consumer builder, using a caller-supplied
        ///     provider that the caller owns.
        /// </summary>
        public static ConsumerBuilder<TKey, TValue> UseAwsOAuthBearer<TKey, TValue>(
            this ConsumerBuilder<TKey, TValue> builder,
            AwsStsTokenProvider provider)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            var handler = AwsOAuthBearerHandler.Create(provider);
            return builder.SetOAuthBearerTokenRefreshHandler(
                (client, oauthCfg) => handler(client, oauthCfg));
        }

        // -------- AdminClientBuilder --------

        /// <summary>
        ///     Installs an AWS STS <c>GetWebIdentityToken</c>-backed OAUTHBEARER
        ///     refresh handler on this admin client builder.
        /// </summary>
        public static AdminClientBuilder UseAwsOAuthBearer(
            this AdminClientBuilder builder,
            AwsOAuthBearerConfig config)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            return builder.UseAwsOAuthBearer(new AwsStsTokenProvider(config));
        }

        /// <summary>
        ///     Installs an AWS STS <c>GetWebIdentityToken</c>-backed OAUTHBEARER
        ///     refresh handler on this admin client builder, using a
        ///     caller-supplied provider that the caller owns.
        /// </summary>
        public static AdminClientBuilder UseAwsOAuthBearer(
            this AdminClientBuilder builder,
            AwsStsTokenProvider provider)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            var handler = AwsOAuthBearerHandler.Create(provider);
            return builder.SetOAuthBearerTokenRefreshHandler(
                (client, oauthCfg) => handler(client, oauthCfg));
        }
    }
}
