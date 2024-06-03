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
    public class RegisteredDek : Dek, IEquatable<RegisteredDek>
    {
        private string keyMaterial;
        private byte[] keyMaterialBytes;
        private byte[] encryptedKeyMaterialBytes;
        
        /// <summary>
        ///     The KEK name for the DEK.
        /// </summary>
        [DataMember(Name = "kekName")]
        public string KekName { get; set; }

        /// <summary>
        ///     The key material.
        /// </summary>
        [DataMember(Name = "keyMaterial")]
        public string KeyMaterial
        {
            get => keyMaterial;
            init => keyMaterial = value;
        }

        /// <summary>
        ///     The timestamp of the DEK.
        /// </summary>
        [DataMember(Name = "ts")]
        public long Timestamp { get; set; }

        /// <summary>
        ///     The encrypted key material bytes.
        /// </summary>
        public byte[] EncryptedKeyMaterialBytes
        {
            get
            {
                if (encryptedKeyMaterialBytes == null && EncryptedKeyMaterial != null)
                {
                    encryptedKeyMaterialBytes = System.Convert.FromBase64String(EncryptedKeyMaterial);
                }

                return encryptedKeyMaterialBytes;
            }
        }

        /// <summary>
        ///     The key material bytes.
        /// </summary>
        public byte[] KeyMaterialBytes
        {
            get
            {
                if (keyMaterialBytes == null && KeyMaterial != null)
                {
                    keyMaterialBytes = System.Convert.FromBase64String(KeyMaterial);
                }
                return keyMaterialBytes;
            }
        }

        public void SetKeyMaterial(byte[] keyMaterialBytes)
        {
            keyMaterial = keyMaterialBytes != null ? System.Convert.ToBase64String(keyMaterialBytes) : null;
        }

        public bool Equals(RegisteredDek other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return base.Equals(other) && keyMaterial == other.keyMaterial && KekName == other.KekName;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((RegisteredDek)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (keyMaterial != null ? keyMaterial.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (KekName != null ? KekName.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}