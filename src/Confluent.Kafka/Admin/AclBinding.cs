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
    ///     Represents an ACL binding that specify the operation and permission type for a specific principal
    ///     over one or more resources of the same type. Used by "IAdminClient.CreateAclsAsync",
    ///     returned by "IAdminClient.DescribeAclsAsync" and "IAdminClient.DeleteAclsAsync".
    /// </summary>
    public class AclBinding
    {
        /// <summary>
        ///     The resource pattern.
        /// </summary>
        public ResourcePattern Pattern { get; set; }

        /// <summary>
        ///    The access control entry.
        /// </summary>
        public AccessControlEntry Entry { get; set; }

        /// <summary>
        ///     Create a filter which matches only this AclBinding.
        /// </summary>
        public AclBindingFilter ToFilter()
        {
            return new AclBindingFilter
            {
                PatternFilter = Pattern.ToFilter(),
                EntryFilter = Entry.ToFilter(),
            };
        }


        /// <summary>
        ///     A clone of the AclBinding object 
        /// </summary>
        public AclBinding Clone()
        {
            var ret = (AclBinding) MemberwiseClone();
            ret.Pattern = ret.Pattern.Clone();
            ret.Entry = ret.Entry.Clone();
            return ret;
        }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if this is an AclBinding and the property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || !GetType().Equals(obj.GetType()))
            {
                return false;
            }
            var aclBinding = (AclBinding)obj;
            if (base.Equals(aclBinding)) return true;
            return Pattern == aclBinding.Pattern &&
                Entry == aclBinding.Entry;
        }

        /// <summary>
        ///     Tests whether AclBinding instance a is equal to AclBinding instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AclBinding instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AclBinding instance to compare.
        /// </param>
        /// <returns>
        ///     true if AclBinding instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(AclBinding a, AclBinding b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether AclBinding instance a is not equal to AclBinding instance b.
        /// </summary>
        /// <param name="a">
        ///     The first AclBinding instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second AclBinding instance to compare.
        /// </param>
        /// <returns>
        ///     true if AclBinding instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(AclBinding a, AclBinding b)
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
            hash ^= Pattern.GetHashCode();
            hash ^= Entry.GetHashCode();
            return hash;
        }

        /// <summary>
        ///     Returns a JSON representation of this AclBinding object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this AclBinding object.
        /// </returns>
        public override string ToString()
        {
            return $"{{\"Pattern\": {Pattern}, \"Entry\": {Entry}}}";
        }
    }
}

