// Copyright 2022 Confluent Inc.
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

using System.Runtime.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Rule kind.
    /// </summary>
    [DataContract(Name = "ruleKind")]
    [JsonConverter(typeof(StringEnumConverter))]
    public enum RuleKind
    {
        /// <summary>
        ///     An transformation rule.
        /// </summary>
        [EnumMember(Value = "TRANSFORM")]
        Transform,

        /// <summary>
        ///     A constraint or validation rule.
        /// </summary>
        [EnumMember(Value = "CONDITION")]
        Condition
    }
}
