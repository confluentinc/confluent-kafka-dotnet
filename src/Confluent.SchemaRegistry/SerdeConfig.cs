// Copyright 2024 Confluent Inc.
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
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Base functionality common to all configuration classes.
    /// </summary>
    public class SerdeConfig : Config
    {
        /// <summary>
        ///     Initialize a new empty <see cref="SerdeConfig" /> instance.
        /// </summary>
        public SerdeConfig() : base() { }

        /// <summary>
        ///     Initialize a new <see cref="SerdeConfig" /> instance based on
        ///     an existing <see cref="SerdeConfig" /> instance.
        ///     This will change the values "in-place" i.e. operations on this class WILL modify the provided collection
        /// </summary>
        public SerdeConfig(SerdeConfig config) : base(config) { }

        /// <summary>
        ///     Initialize a new <see cref="SerdeConfig" /> wrapping
        ///     an existing key/value dictionary.
        ///     This will change the values "in-place" i.e. operations on this class WILL modify the provided collection
        /// </summary>
        public SerdeConfig(IDictionary<string, string> config) : base(config) { }

        /// <summary>
        ///     Gets a configuration property as a dictionary value given a key.
        /// </summary>
        /// <param name="key">
        ///     The configuration property to get.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        protected IDictionary<string, string> GetDictionaryProperty(string key)
        {
            var result = Get(key);
            if (result == null) { return null; }

            string[] values = result.Split(',');
            return values
                .Select(value => value.Split('='))
                .ToDictionary(pair => pair[0], pair => pair[1]);
        }

        /// <summary>
        ///     Set a configuration property as a dictionary value
        /// </summary>
        /// <param name="key">
        ///     The configuration property name.
        /// </param>
        /// <param name="val">
        ///     The property value.
        /// </param>
        protected void SetDictionaryProperty(string key, IDictionary<string, string> value)
        {
            if (value == null)
            {
                SetObject(key, null);
                return;
            }
            
            var result = string.Join(",", value.Select(kv => $"{kv.Key}={kv.Value}"));
            SetObject(key, result);
        }

    }
}
