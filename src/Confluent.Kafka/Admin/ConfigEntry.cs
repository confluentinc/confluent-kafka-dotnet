

// Copyright 2018 Confluent Inc.
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

using System.Collections.Generic;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     A configuration object containing the configuration entries for a resource.
    /// </summary>
    public class ConfigEntry
    {
        /// <summary>
        ///     Whether or not the config value is the default or if it's been explicitly set.
        /// </summary>
        public bool IsDefault { get; set; }

        /// <summary>
        ///     Whether or not the config is read-only and cannot be updated.
        /// </summary>
        public bool IsReadOnly { get; set; }

        /// <summary>
        ///     Whether or not the config value is sensitive.
        /// </summary>
        public bool IsSensitive { get; set; }

        /// <summary>
        ///     Whether or not the config value is a synonym.
        /// </summary>
        public bool IsSynonym { get; set; }

        /// <summary>
        ///     The config name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     The config value.
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ConfigSource Source { get; set; }

        /// <summary>
        ///     
        /// </summary>
        public Dictionary<string, ConfigEntry> Synonyms { get; set; }
    }
}
