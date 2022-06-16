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
using System.Text;

namespace Confluent.Kafka.Admin
{
    /// <summary>
    /// Represents an ACL binding that specify the operation and permission type for a specific principal
    /// over one or more resources of the same type. Used by `AdminClient.CreateAcls`,
    /// returned by `AdminClient.DescribeAcls` and `AdminClient.DeleteAcls`.
    /// </summary>
    public class AclBinding
    {
        /// <summary>
        ///     The resource type.
        /// </summary>
        public ResourceType Type { get; set; }

        /// <summary>
        /// The resource name, which depends on the resource type.
        /// For ResourceBroker the resource name is the broker id.
        /// </summary>
	    public string Name { get; set; }

        /// <summary>
        /// The resource pattern, relative to the name.
        /// </summary>
        public ResourcePatternType ResourcePatternType { get; set; }

        /// <summary>
        /// The principal this AclBinding refers to.
        /// </summary>
        public string Principal { get; set; }

        /// <summary>
        /// The host that the call is allowed to come from.
        /// </summary>
        public string Host { get; set; }

        /// <summary>
        /// The operation/s specified by this binding.
        /// </summary>
        public AclOperation Operation { get; set; }

        /// <summary>
        /// The permission type for the specified operation.
        /// </summary>
        public AclPermissionType PermissionType { get; set; }
        
        /// <summary>
        /// A clone of the AclBinding object 
        /// </summary>
        public AclBinding Clone()
        {
            return (AclBinding) MemberwiseClone();
        }

        /// <summary>
        ///     Tests whether this instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if this is an AclBinding or subclass and the property values are equal. false otherwise.
        /// </returns>
        public override bool Equals(Object obj)
        {
            if (obj == null || !GetType().Equals(obj.GetType()))
            {
                return false;
            }
            var aclBinding = (AclBinding)obj;
            if (base.Equals(aclBinding)) return true;
            return this.Type == aclBinding.Type &&
                this.Name == aclBinding.Name &&
                this.ResourcePatternType == aclBinding.ResourcePatternType &&
                this.Principal == aclBinding.Principal &&
                this.Host == aclBinding.Host &&
                this.Operation == aclBinding.Operation &&
                this.PermissionType == aclBinding.PermissionType;
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
            hash ^= Type.GetHashCode();
            hash ^= ResourcePatternType.GetHashCode();
            hash ^= Operation.GetHashCode();
            hash ^= PermissionType.GetHashCode();
            if (Name != null) hash ^= Name.GetHashCode();
            if (Principal != null) hash ^= Principal.GetHashCode();
            if (Host != null) hash ^= Host.GetHashCode();
            return hash;
        }

        private string Quote(string str) =>
                str == null ? "null" : $"\"{str.Replace("\"","\\\"")}\"";

        /// <summary>
        ///     Returns a JSON representation of this AclBinding object.
        /// </summary>
        /// <returns>
        ///     A JSON representation of this AclBinding object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{\"Type\": \"{Type}\", \"Name\": {Quote(Name)}");
            result.Append($", \"ResourcePatternType\": \"{ResourcePatternType}\", \"Principal\": {Quote(Principal)}");
            result.Append($", \"Host\": {Quote(Host)}, \"Operation\": \"{Operation}\"");
            result.Append($", \"PermissionType\": \"{PermissionType}\"}}");
            return result.ToString();
        }
    }
}
