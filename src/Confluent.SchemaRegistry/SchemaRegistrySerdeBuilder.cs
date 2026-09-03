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
    ///     Common configuration for the builders of Schema Registry serializers and
    ///     deserializers - how the Schema Registry client itself is obtained.
    ///
    ///     There are two ways to supply the client:
    ///
    ///     - <see cref="SetSchemaRegistryClient" />, to use a client the application
    ///       constructed. Necessary when the application needs the client for
    ///       something else too, such as registering encryption keys or creating
    ///       associations before producing, and allows a single client to be shared
    ///       across serializers and clients. The application retains ownership: the
    ///       client is not disposed along with the serde.
    ///
    ///     - <see cref="SetSchemaRegistryConfig" />, optionally combined with
    ///       <see cref="SetAuthenticationHeaderValueProvider" /> and
    ///       <see cref="SetWebProxy" />, to have the builder construct a
    ///       <see cref="CachedSchemaRegistryClient" />. The resulting client is owned
    ///       by the serde, which is in turn owned by the producer or consumer that
    ///       built it, and is disposed along with it.
    ///
    ///     Exactly one of the two must be used.
    /// </summary>
    /// <typeparam name="TBuilder">
    ///     The concrete builder type, returned by the setters so that calls can be
    ///     chained.
    /// </typeparam>
    public abstract class SchemaRegistrySerdeBuilder<TBuilder>
        where TBuilder : SchemaRegistrySerdeBuilder<TBuilder>
    {
        /// <summary>
        ///     The Schema Registry client supplied by the application, if any.
        /// </summary>
        protected ISchemaRegistryClient schemaRegistryClient;

        /// <summary>
        ///     The configuration to construct a Schema Registry client from, if any.
        /// </summary>
        protected SchemaRegistryConfig schemaRegistryConfig;

        /// <summary>
        ///     The authentication header value provider to construct the Schema
        ///     Registry client with, if any.
        /// </summary>
        protected IAuthenticationHeaderValueProvider authenticationHeaderValueProvider;

        /// <summary>
        ///     The proxy to construct the Schema Registry client with, if any.
        /// </summary>
        protected IWebProxy proxy;

        /// <summary>
        ///     The rule registry to construct the serde with, if any.
        /// </summary>
        protected RuleRegistry ruleRegistry;

        /// <summary>
        ///     Use a Schema Registry client constructed by the application.
        ///
        ///     The client is not disposed along with the serde - its lifetime remains
        ///     the application's responsibility.
        /// </summary>
        public TBuilder SetSchemaRegistryClient(ISchemaRegistryClient schemaRegistryClient)
        {
            this.schemaRegistryClient = schemaRegistryClient;
            return (TBuilder)this;
        }

        /// <summary>
        ///     Construct the Schema Registry client from the given configuration.
        ///
        ///     The client is owned by the serde, and is disposed along with it.
        /// </summary>
        public TBuilder SetSchemaRegistryConfig(SchemaRegistryConfig schemaRegistryConfig)
        {
            this.schemaRegistryConfig = schemaRegistryConfig;
            return (TBuilder)this;
        }

        /// <summary>
        ///     Construct the Schema Registry client with the given authentication
        ///     header value provider.
        ///
        ///     Use this for an authentication scheme that cannot be expressed in
        ///     configuration alone, or for credentials that change over the lifetime
        ///     of the client - configuration selects among the built-in providers,
        ///     but only a provider instance can be supplied directly.
        ///
        ///     Only valid alongside <see cref="SetSchemaRegistryConfig" />: a client
        ///     supplied via <see cref="SetSchemaRegistryClient" /> already has its own
        ///     provider.
        /// </summary>
        public TBuilder SetAuthenticationHeaderValueProvider(
            IAuthenticationHeaderValueProvider authenticationHeaderValueProvider)
        {
            this.authenticationHeaderValueProvider = authenticationHeaderValueProvider;
            return (TBuilder)this;
        }

        /// <summary>
        ///     Construct the Schema Registry client with the given proxy.
        ///
        ///     Only valid alongside <see cref="SetSchemaRegistryConfig" />: a client
        ///     supplied via <see cref="SetSchemaRegistryClient" /> is already
        ///     constructed.
        /// </summary>
        public TBuilder SetWebProxy(IWebProxy proxy)
        {
            this.proxy = proxy;
            return (TBuilder)this;
        }

        /// <summary>
        ///     The rule registry to construct the serde with. Defaults to the global
        ///     instance.
        /// </summary>
        public TBuilder SetRuleRegistry(RuleRegistry ruleRegistry)
        {
            this.ruleRegistry = ruleRegistry;
            return (TBuilder)this;
        }

        /// <summary>
        ///     Whether the serde being built requires a Schema Registry client.
        ///
        ///     True for every serializer, and for deserializers that must look up
        ///     the writer schema. Overridden to false by the deserializers that can
        ///     work from the schema of the target type alone, for which supplying no
        ///     client is a valid choice.
        /// </summary>
        protected virtual bool RequiresSchemaRegistryClient
            => true;

        /// <summary>
        ///     Resolve the Schema Registry client to construct the serde with,
        ///     constructing one from configuration if the application did not supply
        ///     one.
        /// </summary>
        /// <param name="owned">
        ///     Whether the returned client was constructed here, and must therefore
        ///     be disposed along with the serde.
        /// </param>
        protected ISchemaRegistryClient ResolveSchemaRegistryClient(out bool owned)
        {
            if (schemaRegistryClient != null)
            {
                if (schemaRegistryConfig != null)
                {
                    throw new ArgumentException(
                        "Cannot specify both a schema registry client and a schema registry configuration; use one or the other.");
                }

                if (authenticationHeaderValueProvider != null)
                {
                    throw new ArgumentException(
                        "Cannot specify an authentication header value provider alongside a schema registry client; the client already has one.");
                }

                if (proxy != null)
                {
                    throw new ArgumentException(
                        "Cannot specify a proxy alongside a schema registry client; the client is already constructed.");
                }

                owned = false;
                return schemaRegistryClient;
            }

            if (schemaRegistryConfig == null)
            {
                if (authenticationHeaderValueProvider != null || proxy != null)
                {
                    throw new ArgumentException(
                        "An authentication header value provider or proxy was specified, but no schema registry configuration to construct a client from.");
                }

                if (RequiresSchemaRegistryClient)
                {
                    throw new ArgumentException(
                        "A schema registry client or a schema registry configuration must be specified.");
                }

                owned = false;
                return null;
            }

            owned = true;
            return new CachedSchemaRegistryClient(
                schemaRegistryConfig, authenticationHeaderValueProvider, proxy);
        }
    }
}
