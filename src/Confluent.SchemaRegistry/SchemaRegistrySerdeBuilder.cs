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


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Common configuration for the builders of Schema Registry serializers and
    ///     deserializers - how the Schema Registry client itself is obtained.
    ///
    ///     There are three ways to supply the client, exactly one of which must be
    ///     used:
    ///
    ///     - <see cref="SetSchemaRegistryClient" />, to use a client the application
    ///       constructed. Necessary when the application needs the client for
    ///       something else too, such as registering encryption keys or creating
    ///       associations before producing, and allows a single client to be shared
    ///       across serdes and clients. The application retains ownership: the client
    ///       is not disposed along with the serde.
    ///
    ///     - <see cref="SetSchemaRegistryConfig" />, to have the builder construct a
    ///       <see cref="CachedSchemaRegistryClient" /> from configuration. The
    ///       shortest form, and enough for most applications.
    ///
    ///     - <see cref="SetSchemaRegistryClientBuilder" />, to have the builder
    ///       construct a client that needs more than configuration alone - an
    ///       authentication header value provider or a proxy, neither of which can be
    ///       expressed in a string-to-string <see cref="SchemaRegistryConfig" />.
    ///
    ///     In the latter two cases the resulting client is owned by the serde, which
    ///     is in turn owned by the producer or consumer that built it, and is
    ///     disposed along with it.
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
        ///     The builder to construct a Schema Registry client with, if any.
        /// </summary>
        protected ISchemaRegistryClientBuilder schemaRegistryClientBuilder;

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
        ///     The client is owned by the serde, and is disposed along with it. Use
        ///     <see cref="SetSchemaRegistryClientBuilder" /> instead when the client
        ///     also needs an authentication header value provider or a proxy.
        /// </summary>
        public TBuilder SetSchemaRegistryConfig(SchemaRegistryConfig schemaRegistryConfig)
        {
            this.schemaRegistryConfig = schemaRegistryConfig;
            return (TBuilder)this;
        }

        /// <summary>
        ///     Construct the Schema Registry client with the given builder.
        ///
        ///     Use this when the client needs more than configuration alone, such as
        ///     an authentication header value provider or a proxy - refer to
        ///     <see cref="CachedSchemaRegistryClientBuilder" />. The client is owned
        ///     by the serde, and is disposed along with it.
        /// </summary>
        public TBuilder SetSchemaRegistryClientBuilder(
            ISchemaRegistryClientBuilder schemaRegistryClientBuilder)
        {
            this.schemaRegistryClientBuilder = schemaRegistryClientBuilder;
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
        ///     constructing one if the application did not supply one.
        /// </summary>
        /// <param name="owned">
        ///     Whether the returned client was constructed here, and must therefore
        ///     be disposed along with the serde.
        /// </param>
        protected ISchemaRegistryClient ResolveSchemaRegistryClient(out bool owned)
        {
            if (schemaRegistryConfig != null && schemaRegistryClientBuilder != null)
            {
                throw new ArgumentException(
                    "Cannot specify both a schema registry configuration and a schema registry client builder; use one or the other.");
            }

            if (schemaRegistryClient != null)
            {
                if (schemaRegistryConfig != null)
                {
                    throw new ArgumentException(
                        "Cannot specify both a schema registry client and a schema registry configuration; use one or the other.");
                }

                if (schemaRegistryClientBuilder != null)
                {
                    throw new ArgumentException(
                        "Cannot specify both a schema registry client and a schema registry client builder; use one or the other.");
                }

                owned = false;
                return schemaRegistryClient;
            }

            if (schemaRegistryClientBuilder != null)
            {
                owned = true;
                return schemaRegistryClientBuilder.Build();
            }

            if (schemaRegistryConfig == null)
            {
                if (RequiresSchemaRegistryClient)
                {
                    throw new ArgumentException(
                        "A schema registry client, configuration or client builder must be specified.");
                }

                owned = false;
                return null;
            }

            owned = true;
            return new CachedSchemaRegistryClient(schemaRegistryConfig);
        }
    }
}
