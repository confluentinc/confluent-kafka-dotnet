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

using System;
using System.Collections;
using System.Collections.Generic;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Auth credentials source.
    /// </summary>
    public enum AuthCredentialsSource
    {
        /// <summary>
        ///     Credentials are specified via the `schema.registry.basic.auth.user.info` config property in the form username:password.
        ///     If `schema.registry.basic.auth.user.info` is not set, authentication is disabled.
        /// </summary>
        UserInfo,

        /// <summary>
        ///     Credentials are specified via the `sasl.username` and `sasl.password` configuration properties.
        /// </summary>
        SaslInherit
    }

    /// <summary>
    ///     Subject name strategy. Refer to: https://www.confluent.io/blog/put-several-event-types-kafka-topic/
    /// </summary>
    public enum SubjectNameStrategy
    {
        /// <summary>
        ///     (default): The subject name for message keys is &lt;topic&gt;-key, and &lt;topic&gt;-value for message values.
        ///     This means that the schemas of all messages in the topic must be compatible with each other.
        /// </summary>
        Topic,

        /// <summary>
        ///     The subject name is the fully-qualified name of the Avro record type of the message.
        ///     Thus, the schema registry checks the compatibility for a particular record type, regardless of topic.
        ///     This setting allows any number of different event types in the same topic.
        /// </summary>
        Record,

        /// <summary>
        ///     The subject name is &lt;topic&gt;-&lt;type&gt;, where &lt;topic&gt; is the Kafka topic name, and &lt;type&gt;
        ///     is the fully-qualified name of the Avro record type of the message. This setting also allows any number of event
        ///     types in the same topic, and further constrains the compatibility check to the current topic only.
        /// </summary>
        TopicRecord
    }

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
            ///     USER_INFO: Credentials are specified via the `schema.registry.basic.auth.user.info` config property in the form username:password.
            ///                If `schema.registry.basic.auth.user.info` is not set, authentication is disabled.
            ///     SASL_INHERIT: Credentials are specified via the `sasl.username` and `sasl.password` configuration properties.
            /// 
            ///     default: USER_INFO
            /// </summary>
            public const string SchemaRegistryBasicAuthCredentialsSource = "schema.registry.basic.auth.credentials.source";

            /// <summary>
            ///     Basic auth credentials in the form {username}:{password}.
            /// 
            ///     default: "" (no authentication).
            /// </summary>
            public const string SchemaRegistryBasicAuthUserInfo = "schema.registry.basic.auth.user.info";

            /// <summary>
            ///     Key subject name strategy.
            /// </summary>
            public const string SchemaRegistryKeySubjectNameStrategy = "schema.registry.key.subject.name.strategy";

            /// <summary>
            ///     Value subject name strategy.
            /// </summary>
            public const string SchemaRegistryValueSubjectNameStrategy = "schema.registry.value.subject.name.strategy";
        }

        /// <summary>
        ///     Specifies the configuration property(ies) that provide the basic authentication credentials.
        /// </summary>
        public AuthCredentialsSource? SchemaRegistryBasicAuthCredentialsSource
        {
            get
            {
                var r = Get(PropertyNames.SchemaRegistryBasicAuthCredentialsSource);
                if (r == null) { return null; }
                if (r == "USER_INFO") { return AuthCredentialsSource.UserInfo; }
                if (r == "SASL_INHERIT") { return AuthCredentialsSource.SaslInherit; }
                throw new ArgumentException($"Unknown ${PropertyNames.SchemaRegistryBasicAuthCredentialsSource} value: {r}.");
            }
            set
            {
                if (value == null) { this.properties.Remove(PropertyNames.SchemaRegistryBasicAuthCredentialsSource); }
                else if (value == AuthCredentialsSource.UserInfo) { this.properties[PropertyNames.SchemaRegistryBasicAuthCredentialsSource] = "USER_INFO"; }
                else if (value == AuthCredentialsSource.SaslInherit) { this.properties[PropertyNames.SchemaRegistryBasicAuthCredentialsSource] = "SASL_INHERIT"; }
                else { throw new NotImplementedException($"Unknown ${PropertyNames.SchemaRegistryBasicAuthCredentialsSource} value: {value}."); }
            }
        }

        /// <summary>
        ///     A comma-separated list of URLs for schema registry instances that are
        ///     used to register or lookup schemas.
        /// </summary>
        public string SchemaRegistryUrl
        {
            get { return Get(SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl); } 
            set { SetObject(SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl, value); }
        }

        /// <summary>
        ///     Specifies the timeout for requests to Confluent Schema Registry.
        /// 
        ///     default: 30000
        /// </summary>
        public int? SchemaRegistryRequestTimeoutMs
        {
            get { return GetInt(SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs, value.ToString()); }
        }

        /// <summary>
        ///     Specifies the maximum number of schemas CachedSchemaRegistryClient
        ///     should cache locally.
        /// 
        ///     default: 1000
        /// </summary>
        public int? SchemaRegistryMaxCachedSchemas
        {
            get { return GetInt(SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas, value.ToString()); }
        }

        /// <summary>
        ///     Basic auth credentials in the form {username}:{password}.
        /// </summary>
        public string SchemaRegistryBasicAuthUserInfo
        {
            get { return Get(SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo, value); }
        }

        /// <summary>
        ///     Key subject name strategy.
        ///     
        ///     default: SubjectNameStrategy.Topic
        /// </summary>
        public SubjectNameStrategy? SchemaRegistryKeySubjectNameStrategy
        {
            get
            {
                var r = Get(PropertyNames.SchemaRegistryKeySubjectNameStrategy);
                if (r == null) { return null; }
                else
                {
                    SubjectNameStrategy result;
                    if (!Enum.TryParse<SubjectNameStrategy>(r, out result))
                        throw new ArgumentException(
                            $"Unknown ${PropertyNames.SchemaRegistryKeySubjectNameStrategy} value: {r}.");
                    else
                        return result;
                }
            }
            set
            {
                if (value == null) { this.properties.Remove(PropertyNames.SchemaRegistryKeySubjectNameStrategy); }
                else { this.properties[PropertyNames.SchemaRegistryKeySubjectNameStrategy] = value.ToString(); }
            }
        }

        /// <summary>
        ///     Value subject name strategy.
        ///
        ///     default: SubjectNameStrategy.Topic
        /// </summary>
        public SubjectNameStrategy? SchemaRegistryValueSubjectNameStrategy
        {
            get
            {
                var r = Get(PropertyNames.SchemaRegistryValueSubjectNameStrategy);
                if (r == null) { return null; }
                else
                {
                    SubjectNameStrategy result;
                    if (!Enum.TryParse<SubjectNameStrategy>(r, out result))
                        throw new ArgumentException(
                            $"Unknown ${PropertyNames.SchemaRegistryValueSubjectNameStrategy} value: {r}.");
                    else
                        return result;
                }
            }
            set
            {
                if (value == null) { this.properties.Remove(PropertyNames.SchemaRegistryValueSubjectNameStrategy); }
                else { this.properties[PropertyNames.SchemaRegistryValueSubjectNameStrategy] = value.ToString(); }
            }
        }

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
            => SetObject(key, val);

        /// <summary>
        ///     Set a configuration property using a key / value pair (null checked).
        /// </summary>
        protected void SetObject(string name, object val)
        {
            if (val == null)
            {
                this.properties.Remove(name);
                return;
            }

            this.properties[name] = val.ToString();
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
        ///     Gets a configuration property int? value given a key.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        protected int? GetInt(string key)
        {
            var result = Get(key);
            if (result == null) { return null; }
            return int.Parse(result);
        }

        /// <summary>
        ///     Gets a configuration property bool? value given a key.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        protected bool? GetBool(string key)
        {
            var result = Get(key);
            if (result == null) { return null; }
            return bool.Parse(result);
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
