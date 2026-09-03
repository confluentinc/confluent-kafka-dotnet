// Copyright 2025 Confluent Inc.
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
using System.Net;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     A builder of <see cref="CachedSchemaRegistryClient" /> instances, covering
    ///     the client's entire input surface: its configuration, an optional
    ///     authentication header value provider, and an optional proxy.
    ///
    ///     Hand one of these to a serde builder via
    ///     <c>SetSchemaRegistryClientBuilder</c> when the client needs more than
    ///     configuration alone. For the common case where configuration is enough,
    ///     the serde builders accept a <see cref="SchemaRegistryConfig" /> directly.
    /// </summary>
    /// <example>
    ///     <code>
    ///     var clientBuilder = new CachedSchemaRegistryClientBuilder()
    ///         .SetConfig(schemaRegistryConfig)
    ///         .SetAuthenticationHeaderValueProvider(myProvider);
    ///
    ///     using var producer = new ProducerBuilder&lt;string, User&gt;(producerConfig)
    ///         .SetValueSerializerBuilder(new AvroSerializerBuilder&lt;User&gt;()
    ///             .SetSchemaRegistryClientBuilder(clientBuilder))
    ///         .Build();
    ///     </code>
    /// </example>
    public class CachedSchemaRegistryClientBuilder : ISchemaRegistryClientBuilder
    {
        private SchemaRegistryConfig config;
        private IAuthenticationHeaderValueProvider authenticationHeaderValueProvider;
        private IWebProxy proxy;

        /// <summary>
        ///     The configuration to construct the client with. Required.
        /// </summary>
        public CachedSchemaRegistryClientBuilder SetConfig(SchemaRegistryConfig config)
        {
            this.config = config;
            return this;
        }

        /// <summary>
        ///     The authentication header value provider to construct the client with.
        ///
        ///     Use this for an authentication scheme that cannot be expressed in
        ///     configuration alone, or for credentials that change over the lifetime
        ///     of the client: configuration selects among the built-in providers, but
        ///     only an instance can be supplied directly, and a client's provider
        ///     cannot be replaced once it is constructed.
        /// </summary>
        public CachedSchemaRegistryClientBuilder SetAuthenticationHeaderValueProvider(
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider)
        {
            this.authenticationHeaderValueProvider = authenticationHeaderValueProvider;
            return this;
        }

        /// <summary>
        ///     The proxy to construct the client with.
        /// </summary>
        public CachedSchemaRegistryClientBuilder SetWebProxy(IWebProxy proxy)
        {
            this.proxy = proxy;
            return this;
        }

        /// <inheritdoc />
        public ISchemaRegistryClient Build()
        {
            if (config == null)
            {
                throw new ArgumentException(
                    "A schema registry configuration must be specified.");
            }

            return new CachedSchemaRegistryClient(
                config, authenticationHeaderValueProvider, proxy);
        }
    }
}
