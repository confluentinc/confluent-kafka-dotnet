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

using System.Collections.Generic;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Provides delete ACLs result or error information.
    /// </summary>
    public class DeleteAclsResult
    {
        /// <summary>
        ///     List of ACL bindings matching the provided filter that were deleted
        /// </summary>
        public List<AclBinding> AclBindings { get; set; }

        /// <summary>
        ///     Operation error status, null if successful.
        /// </summary>
        public Error Error { get; set; }
    }
}