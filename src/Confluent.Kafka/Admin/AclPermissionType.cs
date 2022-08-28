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
    ///     Enumerates the different types of ACL permission types.
    /// </summary>
    public enum AclPermissionType : int
    {
        /// <summary>
        ///     Unknown
        /// </summary>        
        Unknown = 0,

        /// <summary>
        ///     In a filter, matches any AclPermissionType
        /// </summary>        
        Any = 1,

        /// <summary>
        ///     Disallows access
        /// </summary>        
        Deny = 2,

        /// <summary>
        ///     Grants access
        /// </summary>        
        Allow = 3,
    }
}
