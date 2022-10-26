// Copyright 2024 Confluent Inc.
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
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Confluent.SchemaRegistry.Encryption
{
    [DataContract]
    public class UpdateKek : IEquatable<UpdateKek>
    {
        /// <summary>
        ///     The KMS properties.
        /// </summary>
        [DataMember(Name = "kmsProps")]
        public IDictionary<string, string> KmsProps { get; set; }

        /// <summary>
        ///     The doc for the KEK.
        /// </summary>
        [DataMember(Name = "doc")] 
        public string Doc { get; set; }
        
        /// <summary>
        ///     Whether the KEK is shared.
        /// </summary>
        [DataMember(Name = "shared")]
        public bool Shared { get; set; }

        public bool Equals(UpdateKek other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(KmsProps, other.KmsProps) && Doc == other.Doc && Shared == other.Shared;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((UpdateKek)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (KmsProps != null ? KmsProps.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Doc != null ? Doc.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Shared.GetHashCode();
                return hashCode;
            }
        }
    }
}