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
    ///     Represents a filter which matches access control entries.
    /// </summary>
    public class AccessControlEntryFilter
    {
        /// <summary>
        ///     The principal this access control entry filter matches.
        /// </summary>
        public string Principal { get; set; }

        /// <summary>
        ///     The host this access control entry filter matches.
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        ///     The operation/s this access control entry filter matches.
        /// </summary>
        public AclOperation Operation { get; set; }

        /// <summary>
        ///     The permission type this access control entry filter matches.
        /// </summary>
        public AclPermissionType PermissionType { get; set; }
        
        /// <summary>
        ///     A clone of the AccessControlEntryFilter object 
        /// </summary>
        public AccessControlEntryFilter Clone()
        {
            return (AccessControlEntryFilter) MemberwiseClone();
        }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if this is an AccessControlEntryFilter and the property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || !GetType().Equals(obj.GetType()))
            {
                return false;
            }
            var ace = (AccessControlEntryFilter)obj;
            if (base.Equals(ace)) return true;
            return Principal == ace.Principal &&
                Host == ace.Host &&
                Operation == ace.Operation &&
                PermissionType == ace.PermissionType;
        }

        /// <summary>
        ///     Tests whether AccessControlEntryFilter instance a is equal to AccessControlEntryFilter instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AccessControlEntryFilter instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AccessControlEntryFilter instance to compare.
        /// </param>
        /// <returns>
        ///     true if AccessControlEntryFilter instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(AccessControlEntryFilter a, AccessControlEntryFilter b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether AccessControlEntryFilter instance a is not equal to AccessControlEntryFilter instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AccessControlEntryFilter instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AccessControlEntryFilter instance to compare.
        /// </param>
        /// <returns>
        ///     true if AccessControlEntryFilter instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(AccessControlEntryFilter a, AccessControlEntryFilter b)
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
            hash ^= Operation.GetHashCode();
            hash ^= PermissionType.GetHashCode();
            if (Principal != null) hash ^= Principal.GetHashCode();
            if (Host != null) hash ^= Host.GetHashCode();
            return hash;
        }

        /// <summary>
        ///     Returns a JSON representation of this AccessControlEntryFilter object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this AccessControlEntryFilter object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{\"Principal\": {Principal.Quote()}");
            result.Append($", \"Host\": {Host.Quote()}, \"Operation\": \"{Operation}\"");
            result.Append($", \"PermissionType\": \"{PermissionType}\"}}");
            return result.ToString();
        }
    }
}

