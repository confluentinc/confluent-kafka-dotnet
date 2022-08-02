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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents a filter used to return a list of ACL bindings matching some or all of its attributes.
    ///     Used by "IAdminClient.DescribeAclsAsync" and "IAdminClient.DeleteAclsAsync".
    /// </summary>
    public class AclBindingFilter
    {
        /// <summary>
        ///     The resource pattern filter.
        /// </summary>
        public ResourcePatternFilter PatternFilter { get; set; }

        /// <summary>
        ///    The access control entry filter.
        /// </summary>
        public AccessControlEntryFilter EntryFilter { get; set; }
        
        /// <summary>
        ///     A clone of the AclBindingFilter object 
        /// </summary>
        public AclBindingFilter Clone()
        {
            var ret = (AclBindingFilter) MemberwiseClone();
            ret.PatternFilter = ret.PatternFilter.Clone();
            ret.EntryFilter = ret.EntryFilter.Clone();
            return ret;
        }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if this is an AclBindingFilter and the property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || !GetType().Equals(obj.GetType()))
            {
                return false;
            }
            var aclBindingFilter = (AclBindingFilter)obj;
            if (base.Equals(aclBindingFilter)) return true;
            return PatternFilter == aclBindingFilter.PatternFilter &&
                EntryFilter == aclBindingFilter.EntryFilter;
        }

        /// <summary>
        ///     Tests whether AclBindingFilter instance a is equal to AclBindingFilter instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AclBindingFilter instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AclBindingFilter instance to compare.
        /// </param>
        /// <returns>
        ///     true if AclBindingFilter instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(AclBindingFilter a, AclBindingFilter b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether AclBindingFilter instance a is not equal to AclBindingFilter instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AclBindingFilter instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AclBindingFilter instance to compare.
        /// </param>
        /// <returns>
        ///     true if AclBindingFilter instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(AclBindingFilter a, AclBindingFilter b)
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
            hash ^= PatternFilter.GetHashCode();
            hash ^= EntryFilter.GetHashCode();
            return hash;
        }

        /// <summary>
        ///     Returns a JSON representation of this AclBindingFilter object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this AclBindingFilter object.
        /// </returns>
        public override string ToString()
        {
            return $"{{\"PatternFilter\": {PatternFilter}, \"EntryFilter\": {EntryFilter}}}";
        }
    }
}
