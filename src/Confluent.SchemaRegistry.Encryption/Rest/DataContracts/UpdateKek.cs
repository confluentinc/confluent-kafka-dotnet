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

        /// <inheritdoc />
        public bool Equals(UpdateKek other)
        {
            return UpdateKekEqualityComparer.Instance.Equals(this, other);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as UpdateKek);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return UpdateKekEqualityComparer.Instance.GetHashCode(this);
        }
        
        private class UpdateKekEqualityComparer : IEqualityComparer<UpdateKek>
        {
            private readonly DictionaryEqualityComparer<string, string> kmsPropsEqualityComparer = new();

            private UpdateKekEqualityComparer()
            {
            }
            
            public static UpdateKekEqualityComparer Instance { get; } = new();
            
            public bool Equals(UpdateKek x, UpdateKek y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (x is null) return false;
                if (y is null) return false;
                if (x.GetType() != y.GetType()) return false;
                
                if (!kmsPropsEqualityComparer.Equals(x.KmsProps, y.KmsProps)) return false;
                if (x.Doc != y.Doc) return false;
                if (x.Shared != y.Shared) return false;
                
                return true;
            }

            public int GetHashCode(UpdateKek obj)
            {
                var hashCode = new HashCode();
                hashCode.Add(obj.KmsProps, kmsPropsEqualityComparer);
                hashCode.Add(obj.Doc);
                hashCode.Add(obj.Shared);
                
                return hashCode.ToHashCode();
            }
        }
    }
}