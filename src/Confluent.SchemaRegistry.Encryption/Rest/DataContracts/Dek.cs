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
using System.Runtime.Serialization;

namespace Confluent.SchemaRegistry.Encryption
{
    [DataContract]
    public class Dek : IEquatable<Dek>
    {
        /// <summary>
        ///     The subject the DEK is registered under.
        /// </summary>
        [DataMember(Name = "subject")]
        public string Subject { get; set; }

        /// <summary>
        ///     The DEK version.
        /// </summary>
        [DataMember(Name = "version")]
        public int? Version { get; set; }

        /// <summary>
        ///     The DEK algorithm.
        /// </summary>
        [DataMember(Name = "algorithm")] 
        public DekFormat Algorithm { get; set; }
        
        /// <summary>
        ///     The encrypted key material.
        /// </summary>
        [DataMember(Name = "encryptedKeyMaterial")]
        public string EncryptedKeyMaterial { get; init; }

        /// <summary>
        ///     Whether the DEK is deleted.
        /// </summary>
        [DataMember(Name = "deleted")]
        public bool Deleted { get; set; }

        public bool Equals(Dek other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Subject == other.Subject && Version == other.Version && Algorithm == other.Algorithm && 
                   EncryptedKeyMaterial == other.EncryptedKeyMaterial && Deleted == other.Deleted;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Dek)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Subject != null ? Subject.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Version.GetHashCode();
                hashCode = (hashCode * 397) ^ (int)Algorithm;
                hashCode = (hashCode * 397) ^ (EncryptedKeyMaterial != null ? EncryptedKeyMaterial.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Deleted.GetHashCode();
                return hashCode;
            }
        }
    }
}