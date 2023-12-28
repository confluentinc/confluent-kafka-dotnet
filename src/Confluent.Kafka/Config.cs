// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Base functionality common to all configuration classes.
    /// </summary>
    public class Config : IEnumerable<KeyValuePair<string, string>>
    {
        private static Dictionary<string, string> EnumNameToConfigValueSubstitutes = new Dictionary<string, string>
        {
            { "saslplaintext", "sasl_plaintext" },
            { "saslssl", "sasl_ssl" },
            { "consistentrandom", "consistent_random" },
            { "murmur2random", "murmur2_random" },
            { "readcommitted", "read_committed" },
            { "readuncommitted", "read_uncommitted" },
            { "cooperativesticky", "cooperative-sticky" },
            { "usealldnsips", "use_all_dns_ips"},
            { "resolvecanonicalbootstrapserversonly", "resolve_canonical_bootstrap_servers_only"}
        };

        /// <summary>
        ///     Initialize a new empty <see cref="Config" /> instance.
        /// </summary>
        public Config() { this.properties = new Dictionary<string, string>(); }

        /// <summary>
        ///     Initialize a new <see cref="Config" /> instance based on
        ///     an existing <see cref="Config" /> instance.
        ///     This will change the values "in-place" i.e. operations on this class WILL modify the provided collection
        /// </summary>
        public Config(Config config) { this.properties = config.properties; }

        /// <summary>
        ///     Initialize a new <see cref="Config" /> wrapping
        ///     an existing key/value dictionary.
        ///     This will change the values "in-place" i.e. operations on this class WILL modify the provided collection
        /// </summary>
        public Config(IDictionary<string, string> config) { this.properties = config; }

        /// <summary>
        ///     Set a configuration property using a string key / value pair.
        /// </summary>
        /// <remarks>
        ///     Two scenarios where this is useful: 1. For setting librdkafka
        ///     plugin config properties. 2. You are using a different version of 
        ///     librdkafka to the one provided as a dependency of the Confluent.Kafka
        ///     package and the configuration properties have evolved.
        /// </remarks>
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
        ///     Gets a configuration property double? value given a key.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        protected double? GetDouble(string key)
        {
            var result = Get(key);
            if (result == null) { return null; }
            return double.Parse(result);
        }

        /// <summary>
        ///     Gets a configuration property enum value given a key.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <param name="type">
        ///     The enum type of the configuration property.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        protected object GetEnum(Type type, string key)
        {
            var result = Get(key);
            if (result == null) { return null; }
            if (EnumNameToConfigValueSubstitutes.Values.Count(v => v == result) > 0)
            {
                return Enum.Parse(type, EnumNameToConfigValueSubstitutes.First(v => v.Value == result).Key, ignoreCase: true);
            }
            return Enum.Parse(type, result, ignoreCase: true);
        }

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

            if (val is Enum)
            {
                var stringVal = val.ToString().ToLowerInvariant();
                if (EnumNameToConfigValueSubstitutes.TryGetValue(stringVal, out string substitute))
                {
                    this.properties[name] = substitute;
                }
                else
                {
                    this.properties[name] = stringVal;
                }
            }
            else
            {
                this.properties[name] = val.ToString();
            }
        }

        /// <summary>
        ///     The configuration properties.
        /// </summary>
        protected IDictionary<string, string> properties;

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

        /// <summary>
        ///     The maximum length of time (in milliseconds) before a cancellation request
        ///     is acted on. Low values may result in measurably higher CPU usage.
        /// 
        ///     default: 100
        ///     range: 1 &lt;= dotnet.cancellation.delay.max.ms &lt;= 10000
        ///     importance: low
        /// </summary>
        public int CancellationDelayMaxMs { set { this.SetObject(ConfigPropertyNames.CancellationDelayMaxMs, value); } }

        private const int DefaultCancellationDelayMaxMs = 100;

        internal static IEnumerable<KeyValuePair<string, string>> ExtractCancellationDelayMaxMs(
            IEnumerable<KeyValuePair<string, string>> config, out int cancellationDelayMaxMs)
        {
            var cancellationDelayMaxString = config
                .Where(prop => prop.Key == ConfigPropertyNames.CancellationDelayMaxMs)
                .Select(a => a.Value)
                .FirstOrDefault();

            if (cancellationDelayMaxString != null)
            {
                if (!int.TryParse(cancellationDelayMaxString, out cancellationDelayMaxMs))
                {
                    throw new ArgumentException(
                        $"{ConfigPropertyNames.CancellationDelayMaxMs} must be a valid integer value.");
                }
                if (cancellationDelayMaxMs < 1 || cancellationDelayMaxMs > 10000)
                {
                    throw new ArgumentOutOfRangeException(
                        $"{ConfigPropertyNames.CancellationDelayMaxMs} must be in the range 1 <= {ConfigPropertyNames.CancellationDelayMaxMs} <= 10000");
                }
            }
            else
            {
                cancellationDelayMaxMs = DefaultCancellationDelayMaxMs;
            }

            return config.Where(prop => prop.Key != ConfigPropertyNames.CancellationDelayMaxMs);
        }
    }
}
