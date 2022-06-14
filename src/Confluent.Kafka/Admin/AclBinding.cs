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
            return (AclBinding) this.MemberwiseClone();
        }
    }
}
