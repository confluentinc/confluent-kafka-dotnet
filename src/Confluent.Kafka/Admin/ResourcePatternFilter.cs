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
    ///     Represents a filter that can match <see cref="ResourcePattern"/>.
    /// </summary>
    public class ResourcePatternFilter
    {
        /// <summary>
        ///     The resource type this filter matches.
        /// </summary>
        public ResourceType Type { get; set; }

        /// <summary>
        ///     The resource name this filter matches, which depends on the resource type.
        ///     For ResourceBroker the resource name is the broker id.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     The resource pattern type of this filter.
        ///     If <see cref="Confluent.Kafka.Admin.ResourcePatternType.Any"/>, the filter will match patterns regardless of pattern type.
        ///     If <see cref="Confluent.Kafka.Admin.ResourcePatternType.Match"/>, the filter will match patterns that would match the supplied name,
        ///     including a matching prefixed and wildcards patterns.
        ///     If any other resource pattern type, the filter will match only patterns with the same type.
        /// </summary>
        public ResourcePatternType ResourcePatternType { get; set; }

        /// <summary>
        ///     A clone of the ResourcePatternFilter object 
        /// </summary>
        public ResourcePatternFilter Clone()
        {
            return (ResourcePatternFilter) MemberwiseClone();
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
            var resourcePatternFilter = (ResourcePatternFilter)obj;
            if (base.Equals(resourcePatternFilter)) return true;
            return Type == resourcePatternFilter.Type &&
                Name == resourcePatternFilter.Name &&
                ResourcePatternType == resourcePatternFilter.ResourcePatternType;
        }

        /// <summary>
        ///     Tests whether ResourcePatternFilter instance a is equal to ResourcePatternFilter instance b.
        /// </summary>
        /// <param name="a">
        ///     The first ResourcePatternFilter instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second ResourcePatternFilter instance to compare.
        /// </param>
        /// <returns>
        ///     true if ResourcePatternFilter instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(ResourcePatternFilter a, ResourcePatternFilter b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether ResourcePatternFilter instance a is not equal to ResourcePatternFilter instance b.
        /// </summary>
        /// <param name="a">
        ///     The first ResourcePatternFilter instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second ResourcePatternFilter instance to compare.
        /// </param>
        /// <returns>
        ///     true if ResourcePatternFilter instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(ResourcePatternFilter a, ResourcePatternFilter b)
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
        ///     Returns a JSON representation of this ResourcePatternFilter object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this ResourcePatternFilter object.
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
