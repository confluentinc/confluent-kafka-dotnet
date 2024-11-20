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
    ///     Rule mode.
    /// </summary>
    [DataContract(Name = "ruleMode")]
    [JsonConverter(typeof(StringEnumConverter))]
    public enum RuleMode
    {
        /// <summary>
        ///     An upgrade rule.
        /// </summary>
        [EnumMember(Value = "UPGRADE")]
        Upgrade,

        /// <summary>
        ///     A downgrade rule.
        /// </summary>
        [EnumMember(Value = "DOWNGRADE")]
        Downgrade,

        /// <summary>
        ///     A rule used during both upgrade and downgrade.
        /// </summary>
        [EnumMember(Value = "UPDOWN")]
        UpDown,
        
        /// <summary>
        ///     A rule used during read (consuming).
        /// </summary>
        [EnumMember(Value = "READ")]
        Read,
        
        /// <summary>
        ///     A rule used during write (producing).
        /// </summary>
        [EnumMember(Value = "WRITE")]
        Write,
        
        /// <summary>
        ///     A rule used during both write and read (producing and consuming).
        /// </summary>
        [EnumMember(Value = "WRITEREAD")]
        WriteRead
    }
}
