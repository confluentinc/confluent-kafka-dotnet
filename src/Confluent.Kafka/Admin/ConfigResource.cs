// Copyright 2018 Confluent Inc.
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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     A class representing resources that have configs.
    /// </summary>
    public class ConfigResource
    {
        /// <summary>
        ///     The resource type (required)
        /// </summary>
        public ResourceType Type { get; set; }

        /// <summary>
        ///     The resource name (required)
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     Tests whether this ConfigResource instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a ConfigResource and all properties are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is ConfigResource))
            {
                return false;
            }

            var tp = (ConfigResource)obj;
            return tp.Type == Type && tp.Name == Name;
        }

        /// <summary>
        ///     Returns a hash code for this ConfigResource.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this ConfigResource.
        /// </returns>
        public override int GetHashCode()
            // x by prime number is quick and gives decent distribution.
            => Type.GetHashCode()*251 + Name.GetHashCode();

        /// <summary>
        ///     Tests whether ConfigResource instance a is equal to ConfigResource instance b.
        /// </summary>
        /// <param name="a">
        ///     The first ConfigResource instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second ConfigResource instance to compare.
        /// </param>
        /// <returns>
        ///     true if ConfigResource instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(ConfigResource a, ConfigResource b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether ConfigResource instance a is not equal to ConfigResource instance b.
        /// </summary>
        /// <param name="a">
        ///     The first ConfigResource instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second ConfigResource instance to compare.
        /// </param>
        /// <returns>
        ///     true if ConfigResource instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(ConfigResource a, ConfigResource b)
            => !(a == b);

        /// <summary>
        ///     Returns a string representation of the ConfigResource object.
        /// </summary>
        /// <returns>
        ///     A string representation of the ConfigResource object.
        /// </returns>
        public override string ToString()
            => $"[{Type}] {Name}";
    }
}
