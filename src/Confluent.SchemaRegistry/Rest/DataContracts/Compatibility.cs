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

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Runtime.Serialization;


namespace Confluent.SchemaRegistry
{
    [DataContract(Name = "compatibility")]
    [JsonConverter(typeof(StringEnumConverter))]
    public enum Compatibility
    {
        [EnumMember(Value = "NONE")]
        None,
        [EnumMember(Value = "FORWARD")]
        Forward,
        [EnumMember(Value = "BACKWARD")]
        Backward,
        [EnumMember(Value = "FULL")]
        Full
    }
}
