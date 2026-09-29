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
        ///     Configuration property names shared by all serializers and deserializers.
        /// </summary>
        public static class SharedPropertyNames
        {
            /// <summary>
            ///     Determines when inline validation rules run, relative to domain rule
            ///     transformations. One of DISABLED, BEFORE_DOMAIN_RULES or
            ///     AFTER_DOMAIN_RULES.
            ///
            ///     default: DISABLED
            /// </summary>
            public const string ValidationRulesExecution = "validation.rules.execution";

            /// <summary>
            ///     When true, validation stops at the first failed rule and reports only
            ///     that violation. When false, every node is visited and the full set of
            ///     violations is reported.
            ///
            ///     default: false
            /// </summary>
            public const string ValidationRulesFailFast = "validation.rules.fail.fast";
        }

        /// <summary>
        ///     Determines when inline validation rules run, relative to domain rule
        ///     transformations.
        ///
        ///     default: <see cref="Confluent.SchemaRegistry.ValidationRulesExecution.Disabled" />
        /// </summary>
        public ValidationRulesExecution ValidationRulesExecution
        {
            get
            {
                var result = Get(SharedPropertyNames.ValidationRulesExecution);
                if (result == null)
                {
                    return Confluent.SchemaRegistry.ValidationRulesExecution.Disabled;
                }

                switch (result.ToUpperInvariant())
                {
                    case "DISABLED":
                        return Confluent.SchemaRegistry.ValidationRulesExecution.Disabled;
                    case "BEFORE_DOMAIN_RULES":
                        return Confluent.SchemaRegistry.ValidationRulesExecution.BeforeDomainRules;
                    case "AFTER_DOMAIN_RULES":
                        return Confluent.SchemaRegistry.ValidationRulesExecution.AfterDomainRules;
                    default:
                        throw new ArgumentException(
                            $"Unknown {SharedPropertyNames.ValidationRulesExecution} value: {result}. " +
                            "Expected one of DISABLED, BEFORE_DOMAIN_RULES, AFTER_DOMAIN_RULES.");
                }
            }
            set
            {
                string str;
                switch (value)
                {
                    case Confluent.SchemaRegistry.ValidationRulesExecution.Disabled:
                        str = "DISABLED";
                        break;
                    case Confluent.SchemaRegistry.ValidationRulesExecution.BeforeDomainRules:
                        str = "BEFORE_DOMAIN_RULES";
                        break;
                    case Confluent.SchemaRegistry.ValidationRulesExecution.AfterDomainRules:
                        str = "AFTER_DOMAIN_RULES";
                        break;
                    default:
                        throw new ArgumentException($"Unknown ValidationRulesExecution value: {value}");
                }

                SetObject(SharedPropertyNames.ValidationRulesExecution, str);
            }
        }

        /// <summary>
        ///     When true, validation stops at the first failed rule and reports only that
        ///     violation.
        ///
        ///     default: false
        /// </summary>
        public bool ValidationRulesFailFast
        {
            get => GetBool(SharedPropertyNames.ValidationRulesFailFast) ?? false;
            set => SetObject(SharedPropertyNames.ValidationRulesFailFast, value);
        }

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
