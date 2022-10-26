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
    public class Kek : IEquatable<Kek>
    {
        /// <summary>
        ///     The name of the KEK.
        /// </summary>
        [DataMember(Name = "name")]
        public string Name { get; set; }

        /// <summary>
        ///     The KMS type for the KEK.
        /// </summary>
        [DataMember(Name = "kmsType")]
        public string KmsType { get; set; }

        /// <summary>
        ///     The KMS key ID for the KEK
        /// </summary>
        [DataMember(Name = "kmsKeyId")] 
        public string KmsKeyId { get; set; }
        
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
        
        /// <summary>
        ///     Whether the KEK is deleted.
        /// </summary>
        [DataMember(Name = "deleted")]
        public bool Deleted { get; set; }
        
        public bool Equals(Kek other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Name == other.Name && KmsType == other.KmsType && 
                   KmsKeyId == other.KmsKeyId && Equals(KmsProps, other.KmsProps) && 
                   Doc == other.Doc && Shared == other.Shared && Deleted == other.Deleted;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Kek)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (KmsType != null ? KmsType.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (KmsKeyId != null ? KmsKeyId.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (KmsProps != null ? KmsProps.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Doc != null ? Doc.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Shared.GetHashCode();
                hashCode = (hashCode * 397) ^ Deleted.GetHashCode();
                return hashCode;
            }
        }

    }
}