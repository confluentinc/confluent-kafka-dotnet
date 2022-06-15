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
    public class AclBindingFilter : AclBinding
    {
        /// <summary>
        /// A clone of the AclBindingFilter object 
        /// </summary>
        new public AclBindingFilter Clone()
        {
            return (AclBindingFilter)this.MemberwiseClone();
        }
    }
}
