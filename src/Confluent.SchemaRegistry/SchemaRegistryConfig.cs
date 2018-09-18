// Copyright 2016-2018 Confluent Inc.
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

using System.Collections;
using System.Collections.Generic;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     <see cref="CachedSchemaRegistryClient" /> configuration properties.
    /// </summary>
    public class SchemaRegistryConfig : IEnumerable<KeyValuePair<string, string>>
    {
        /// <summary>
        ///     Configuration property names specific to the schema registry client.
        /// </summary>
        public static class PropertyNames
        {
            /// <summary>
            ///     A comma-separated list of URLs for schema registry instances that are
            ///     used to register or lookup schemas.
            /// </summary>
            public const string SchemaRegistryUrl = "schema.registry.url";

            /// <summary>
            ///     Specifies the timeout for requests to Confluent Schema Registry.
            ///
            ///     default: 30000
            /// </summary>
            public const string SchemaRegistryRequestTimeoutMs = "schema.registry.request.timeout.ms";

            /// <summary>
            ///     Specifies the maximum number of schemas CachedSchemaRegistryClient
            ///     should cache locally.
            ///
            ///     default: 1000
            /// </summary>
            public const string SchemaRegistryMaxCachedSchemas = "schema.registry.max.cached.schemas";

            /// <summary>
            ///     Specifies the configuration property(ies) that provide the basic authentication credentials.
            /// </summary>
            public const string SchemaRegistryBasicAuthCredentialsSource = "schema.registry.basic.auth.credentials.source";

            /// <summary>
            ///     Basic auth credentials in the form {username}:{password}.
            /// </summary>
            public const string SchemaRegistryBasicAuthUserInfo = "schema.registry.basic.auth.user.info";
        }

        /// <summary>
        ///     A comma-separated list of URLs for schema registry instances that are
        ///     used to register or lookup schemas.
        /// </summary>
        public string SchemaRegistryUrl { set { this.properties[SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl] = value.ToString(); } }

        /// <summary>
        ///     Specifies the timeout for requests to Confluent Schema Registry.
        /// 
        ///     default: 30000
        /// </summary>
        public int SchemaRegistryRequestTimeoutMs { set { this.properties[SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs] = value.ToString(); } }

        /// <summary>
        ///     Specifies the maximum number of schemas CachedSchemaRegistryClient
        ///     should cache locally.
        /// 
        ///     default: 1000
        /// </summary>
        public int SchemaRegistryMaxCachedSchemas { set { this.properties[SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas] = value.ToString(); } }

        /// <summary>
        ///     Basic auth credentials in the form {username}:{password}.
        /// </summary>
        public string SchemaRegistryBasicAuthUserInfo { set { this.properties[SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo] = value; }}

        /// <summary>
        ///     Set a configuration property using a string key / value pair.
        /// </summary>
        /// <param name="key">
        ///     The configuration property name.
        /// </param>
        /// <param name="val">
        ///     The property value.
        /// </param>
        public void Set(string key, string val)
        {
            this.properties[key] = val;
        }

        /// <summary>
        ///     Gets a configuration property value given a key. Returns null if 
        ///     the property has not been set.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        public string Get(string key)
        {
            if (this.properties.TryGetValue(key, out string val))
            {
                return val;
            }
            return null;
        }
        
        /// <summary>
        ///     The configuration properties.
        /// </summary>
        protected Dictionary<string, string> properties = new Dictionary<string, string>();

        /// <summary>
        ///     	Returns an enumerator that iterates through the property collection.
        /// </summary>
        /// <returns>
        ///         An enumerator that iterates through the property collection.
        /// </returns>
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => this.properties.GetEnumerator();

        /// <summary>
        ///     	Returns an enumerator that iterates through the property collection.
        /// </summary>
        /// <returns>
        ///         An enumerator that iterates through the property collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator() => this.properties.GetEnumerator();
    }
}
