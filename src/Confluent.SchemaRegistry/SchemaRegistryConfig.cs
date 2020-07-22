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
            [Obsolete("Subject name strategies should now be configured using the serializer's configuration. In the future, this configuration property will be removed from SchemaRegistryConfig")]
            public const string SchemaRegistryKeySubjectNameStrategy = "schema.registry.key.subject.name.strategy";

            /// <summary>
            ///     Value subject name strategy.
            /// </summary>
            [Obsolete("Subject name strategies should now be configured using the serializer's configuration. In the future, this configuration property will be removed from SchemaRegistryConfig")]
            public const string SchemaRegistryValueSubjectNameStrategy = "schema.registry.value.subject.name.strategy";

            /// <summary>
            ///     File path to CA certificate(s) for verifying the Schema Registry's key. System CA certs will be used if not specified.
            /// </summary>
            public const string SslCaLocation = "schema.registry.ssl.ca.location";

            /// <summary>
            ///     SSL keystore (PKCS#12) location.
            /// </summary>
            public const string SslKeystoreLocation = "schema.registry.ssl.keystore.location";

            /// <summary>
            ///     SSL keystore (PKCS#12) password.
            /// </summary>
            public const string SslKeystorePassword = "schema.registry.ssl.keystore.password";

            /// <summary>
            ///     Enable SSL verification. Disabling SSL verification is insecure and should only be done for reasons
            ///     of convenience in test/dev environments.
            ///
            ///     default: true
            /// </summary>
            public const string EnableSslCertificateVerification = "schema.registry.enable.ssl.certificate.verification";
        }

        /// <summary>
        ///     Specifies the configuration property(ies) that provide the basic authentication credentials.
        /// </summary>
        public AuthCredentialsSource? BasicAuthCredentialsSource
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
        public string Url
        {
            get { return Get(SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl); } 
            set { SetObject(SchemaRegistryConfig.PropertyNames.SchemaRegistryUrl, value); }
        }


        /// <summary>
        ///     Specifies the timeout for requests to Confluent Schema Registry.
        /// 
        ///     default: 30000
        /// </summary>
        public int? RequestTimeoutMs
        {
            get { return GetInt(SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SchemaRegistryRequestTimeoutMs, value.ToString()); }
        }

        /// <summary>
        ///     File or directory path to CA certificate(s) for verifying the schema registry's key.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslCaLocation
        {
            get { return Get(SchemaRegistryConfig.PropertyNames.SslCaLocation); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SslCaLocation, value.ToString()); }
        }

        /// <summary>
        ///     Path to client's keystore (PKCS#12) used for authentication.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslKeystoreLocation
        {
            get { return Get(SchemaRegistryConfig.PropertyNames.SslKeystoreLocation); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SslKeystoreLocation, value.ToString()); }
        }

        /// <summary>
        ///     Client's keystore (PKCS#12) password.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public string SslKeystorePassword
        {
            get { return Get(SchemaRegistryConfig.PropertyNames.SslKeystorePassword); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SslKeystorePassword, value.ToString()); }
        }

        /// <summary>
        ///     Enable/Disable SSL server certificate verification. Only use in contained test/dev environments.
        ///
        ///     default: ''
        ///     importance: low
        /// </summary>
        public bool? EnableSslCertificateVerification
        {
            get { return GetBool(SchemaRegistryConfig.PropertyNames.EnableSslCertificateVerification); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.EnableSslCertificateVerification, value); }
        }

        /// <summary>
        ///     Specifies the maximum number of schemas CachedSchemaRegistryClient
        ///     should cache locally.
        /// 
        ///     default: 1000
        /// </summary>
        public int? MaxCachedSchemas
        {
            get { return GetInt(SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SchemaRegistryMaxCachedSchemas, value.ToString()); }
        }


        /// <summary>
        ///     Basic auth credentials in the form {username}:{password}.
        /// </summary>
        public string BasicAuthUserInfo
        {
            get { return Get(SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo); }
            set { SetObject(SchemaRegistryConfig.PropertyNames.SchemaRegistryBasicAuthUserInfo, value); }
        }


        /// <summary>
        ///     Key subject name strategy.
        ///     
        ///     default: SubjectNameStrategy.Topic
        /// </summary>
        [Obsolete("Subject name strategies should now be configured using the serializer's configuration. In the future, this configuration property will be removed from SchemaRegistryConfig")]
        public SubjectNameStrategy? KeySubjectNameStrategy
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
        [Obsolete("Subject name strategies should now be configured using the serializer's configuration. In the future, this configuration property will be removed from SchemaRegistryConfig")]
        public SubjectNameStrategy? ValueSubjectNameStrategy
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
