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

        public bool Equals(Metadata other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(Tags, other.Tags) && Equals(Properties, other.Properties) && Equals(Sensitive, other.Sensitive);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Metadata)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Tags != null ? Tags.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Properties != null ? Properties.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Sensitive != null ? Sensitive.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
