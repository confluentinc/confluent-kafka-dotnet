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
using Confluent.Shared.CollectionUtils;

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

        /// <inheritdoc />
        public bool Equals(Kek other)
        {
            return KekEqualityComparer.Instance.Equals(this, other);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as Kek);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return KekEqualityComparer.Instance.GetHashCode(this);
        }

        private class KekEqualityComparer : IEqualityComparer<Kek>
        {
            private readonly DictionaryEqualityComparer<string, string> kmsPropsEqualityComparer = new();

            private KekEqualityComparer()
            {
            }
            
            public static KekEqualityComparer Instance { get; } = new();
            
            public bool Equals(Kek x, Kek y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (x is null) return false;
                if (y is null) return false;
                if (x.GetType() != y.GetType()) return false;
                
                if (x.Name != y.Name) return false;
                if (x.KmsType != y.KmsType) return false;
                if (x.KmsKeyId != y.KmsKeyId) return false;
                if (!kmsPropsEqualityComparer.Equals(x.KmsProps, y.KmsProps)) return false;
                if (x.Doc != y.Doc) return false;
                if (x.Shared != y.Shared) return false;
                if (x.Deleted != y.Deleted) return false;
                
                return true;
            }

            public int GetHashCode(Kek obj)
            {
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                if (obj is null)
                {
                    return 0;
                }
                
                var hashCode = new HashCode();
                hashCode.Add(obj.Name);
                hashCode.Add(obj.KmsType);
                hashCode.Add(obj.KmsKeyId);
                hashCode.Add(obj.KmsProps, kmsPropsEqualityComparer);
                hashCode.Add(obj.Doc);
                hashCode.Add(obj.Shared);
                hashCode.Add(obj.Deleted);
                
                return hashCode.ToHashCode();
            }
        }
    }
}