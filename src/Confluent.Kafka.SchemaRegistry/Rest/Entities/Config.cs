// Copyright 2016-2017 Confluent Inc.
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


namespace Confluent.Kafka.SchemaRegistry.Rest.Entities
{
    [DataContract]
    public class Config
    {
        [DataContract(Name = "compatibility")]
        [JsonConverter(typeof(StringEnumConverter))]
        public enum Compatbility
        {
            [EnumMember(Value = "NONE")]
            NONE,
            [EnumMember(Value = "FORWARD")]
            FORWARD,
            [EnumMember(Value = "BACKWARD")]
            BACKWARD,
            [EnumMember(Value = "FULL")]
            FULL
        }

        [DataMember(Name = "compatibility")]
        public Compatbility CompatibilityLevel { get; }
        
        public Config(Compatbility compatibilityLevel)
        {
            CompatibilityLevel = compatibilityLevel;
        }

        public override string ToString() => $"{{compatibility={CompatibilityLevel}}}";
        
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return CompatibilityLevel == ((Config)obj).CompatibilityLevel;
        }
        
        public override int GetHashCode()
        {
            return 31 * CompatibilityLevel.GetHashCode();
        }
    }
}
