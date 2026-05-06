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

namespace Confluent.Kafka.OAuthBearer.Aws
{
    /// <summary>
    ///     Reflection entry-point invoked by Confluent.Kafka core when the user
    ///     sets <c>sasl.oauthbearer.metadata.authentication.type=aws_iam</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <b>This is the only public type in the optional package.</b> Core's
    ///         dispatcher locates this type via
    ///         <c>Assembly.Load("Confluent.Kafka.OAuthBearer.Aws")</c> and resolves
    ///         <see cref="CreateHandler"/> by name + signature. The signature is a
    ///         frozen cross-package contract — bumping it requires a major
    ///         version increment of this package.
    ///     </para>
    ///     <para>
    ///         The marker key/value check is performed in core; <see cref="CreateHandler"/>
    ///         is invoked only when the caller has already decided to autowire the
    ///         AWS path. <see cref="CreateHandler"/> therefore unconditionally
    ///         attempts to build a handler and throws on any input it can't parse.
    ///     </para>
    /// </remarks>
    public static class AwsAutoWire
    {
        /// <summary>
        ///     Config key whose value <see cref="CreateHandler"/> parses for AWS
        ///     STS parameters.
        /// </summary>
        internal const string SaslOauthbearerConfigKey = "sasl.oauthbearer.config";

        /// <summary>
        ///     Config key that activates the AWS autowire path. Inspected by
        ///     core's dispatcher to decide whether to call this entry-point.
        /// </summary>
        internal const string MarkerKey = "sasl.oauthbearer.metadata.authentication.type";

        /// <summary>
        ///     On-wire marker value that selects AWS IAM authentication.
        /// </summary>
        internal const string MarkerValue = "aws_iam";

        /// <summary>
        ///     Builds an OAUTHBEARER refresh handler from the user's Kafka client
        ///     config dictionary. Called by Confluent.Kafka core via
        ///     <c>Assembly.Load</c> + reflected <c>MethodInfo</c>.
        /// </summary>
        /// <param name="kafkaConfig">
        ///     The full client config snapshot (must include
        ///     <c>sasl.oauthbearer.config</c>). Implementations must not assume
        ///     any other key is present.
        /// </param>
        /// <returns>
        ///     An <c>Action&lt;IClient, string&gt;</c> suitable for
        ///     <c>SetOAuthBearerTokenRefreshHandler</c>. The closure resolves a
        ///     fresh JWT via STS each time librdkafka fires the refresh event.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        ///     <paramref name="kafkaConfig"/> is null.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     <c>sasl.oauthbearer.config</c> is missing or empty, or its
        ///     contents fail
        ///     <see cref="AwsOAuthBearerConfig.Parse(string)"/> validation.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        ///     Eager AWS SDK initialisation failure (e.g. unknown region in
        ///     <c>RegionEndpoint.GetBySystemName</c>).
        /// </exception>
        public static Action<IClient, string> CreateHandler(
            IReadOnlyDictionary<string, string> kafkaConfig)
        {
            if (kafkaConfig == null) throw new ArgumentNullException(nameof(kafkaConfig));

            if (!kafkaConfig.TryGetValue(SaslOauthbearerConfigKey, out var raw)
                || string.IsNullOrEmpty(raw))
            {
                throw new ArgumentException(
                    $"Config '{MarkerKey}={MarkerValue}' is set but " +
                    $"'{SaslOauthbearerConfigKey}' is missing or empty. The autowire path " +
                    "requires region and audience to be supplied via " +
                    "sasl.oauthbearer.config (e.g. \"region=us-east-1 audience=https://...\").");
            }

            var parsed = AwsOAuthBearerConfig.Parse(raw);
            var provider = new AwsStsTokenProvider(parsed);
            return AwsOAuthBearerHandler.Create(provider);
        }
    }
}
