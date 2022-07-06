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

using System.Text;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents a pattern that is used by ACLs to match zero or more resources.
    /// </summary>
    public class ResourcePattern
    {
        /// <summary>
        ///     The resource type.
        /// </summary>
        public ResourceType Type { get; set; }

        /// <summary>
        ///     The resource name, which depends on the resource type.
        ///     For ResourceBroker the resource name is the broker id.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     The resource pattern, which controls how the pattern will match resource names.
        /// </summary>
        public ResourcePatternType ResourcePatternType { get; set; }

        /// <summary>
        ///    Create a filter which matches only this ResourcePattern.
        /// </summary>
        public ResourcePatternFilter ToFilter()
        {
            return new ResourcePatternFilter
            {
                Type = Type,
                Name = Name,
                ResourcePatternType = ResourcePatternType,
            };
        }

        /// <summary>
        ///     A clone of the ResourcePattern object 
        /// </summary>
        public ResourcePattern Clone()
        {
            return (ResourcePattern) MemberwiseClone();
        }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if it is of the same type and the property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || !GetType().Equals(obj.GetType()))
            {
                return false;
            }
            var resourcePattern = (ResourcePattern)obj;
            if (base.Equals(resourcePattern)) return true;
            return Type == resourcePattern.Type &&
                Name == resourcePattern.Name &&
                ResourcePatternType == resourcePattern.ResourcePatternType;
        }

        /// <summary>
        ///     Tests whether ResourcePattern instance a is equal to ResourcePattern instance b.
        /// </summary>
        /// <param name="a">
        ///     The first ResourcePattern instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second ResourcePattern instance to compare.
        /// </param>
        /// <returns>
        ///     true if ResourcePattern instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(ResourcePattern a, ResourcePattern b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether ResourcePattern instance a is not equal to ResourcePattern instance b.
        /// </summary>
        /// <param name="a">
        ///     The first ResourcePattern instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second ResourcePattern instance to compare.
        /// </param>
        /// <returns>
        ///     true if ResourcePattern instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(ResourcePattern a, ResourcePattern b)
            => !(a == b);

        /// <summary>
        ///     Returns a hash code for this value.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this value.
        /// </returns>
        public override int GetHashCode()
        {
            int hash = 1;
            hash ^= Type.GetHashCode();
            hash ^= ResourcePatternType.GetHashCode();
            if (Name != null) hash ^= Name.GetHashCode();
            return hash;
        }

        /// <summary>
        ///     Returns a JSON representation of this ResourcePattern object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this ResourcePattern object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{\"Type\": \"{Type}\", \"Name\": {Name.Quote()}");
            result.Append($", \"ResourcePatternType\": \"{ResourcePatternType}\"}}");
            return result.ToString();
        }
    }
}
