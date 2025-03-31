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

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Confluent.Shared.CollectionUtils;

namespace Confluent.SchemaRegistry
{
    [DataContract]
    public class Metadata : IEquatable<Metadata>
    {
        [DataMember(Name = "tags")]
        public IDictionary<string, ISet<string>> Tags { get; set; }

        [DataMember(Name = "properties")]
        public IDictionary<string, string> Properties { get; set; }

        [DataMember(Name = "sensitive")]
        public ISet<string> Sensitive { get; set; }

        /// <summary>
        ///     Empty constructor for serialization
        /// </summary>
        private Metadata() { }

        public Metadata(IDictionary<string, ISet<string>> tags, 
            IDictionary<string, string> properties, 
            ISet<string> sensitive)
        {
            Tags = tags;
            Properties = properties;
            Sensitive = sensitive;
        }

        /// <inheritdoc />
        public bool Equals(Metadata other)
        {
            return MetadataEqualityComparer.Instance.Equals(this, other);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return Equals(obj as Metadata);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return MetadataEqualityComparer.Instance.GetHashCode(this);
        }
        
        private class MetadataEqualityComparer : IEqualityComparer<Metadata>
        {
            private readonly DictionaryEqualityComparer<string, ISet<string>> tagsEqualityComparer = new(new SetEqualityComparer<string>());
            private readonly DictionaryEqualityComparer<string, string> propertiesEqualityComparer = new();
            private readonly SetEqualityComparer<string> sensitiveEqualityComparer = new();

            private MetadataEqualityComparer()
            {
            }
            
            public static MetadataEqualityComparer Instance { get; } = new();
            
            public bool Equals(Metadata x, Metadata y)
            {
                if (ReferenceEquals(x, y)) return true;
                if (x is null) return false;
                if (y is null) return false;
                if (x.GetType() != y.GetType()) return false;
                if (!tagsEqualityComparer.Equals(x.Tags, y.Tags)) return false;
                if (!propertiesEqualityComparer.Equals(x.Properties, y.Properties)) return false;
                if (!sensitiveEqualityComparer.Equals(x.Sensitive, y.Sensitive)) return false;
                
                return true;
            }

            public int GetHashCode(Metadata obj)
            {
                var hashCode = new HashCode();
                hashCode.Add(obj.Tags, tagsEqualityComparer);
                hashCode.Add(obj.Properties, propertiesEqualityComparer);
                hashCode.Add(obj.Sensitive, sensitiveEqualityComparer);
                return hashCode.ToHashCode();
            }
        }
    }
}
