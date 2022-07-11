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
    ///     Enumerates the different types of ACL operation.
    /// </summary>
    public enum AclOperation : int
    {
        /// <summary>
        ///     Unknown
        /// </summary>
        Unknown = 0,

        /// <summary>
        ///     In a filter, matches any AclOperation
        /// </summary>
        Any = 1,

        /// <summary>
        ///     ALL the operations
        /// </summary>
        All = 2,

        /// <summary>
        ///     READ operation
        /// </summary>
        Read = 3,

        /// <summary>
        ///     WRITE operation
        /// </summary>
        Write = 4,

        /// <summary>
        ///     CREATE operation
        /// </summary>
        Create = 5,

        /// <summary>
        ///     DELETE operation
        /// </summary>
        Delete = 6,

        /// <summary>
        ///     ALTER operation
        /// </summary>
        Alter = 7,

        /// <summary>
        ///     DESCRIBE operation
        /// </summary>
        Describe = 8,

        /// <summary>
        ///     CLUSTER_ACTION operation
        /// </summary>
        ClusterAction = 9,

        /// <summary>
        ///     DESCRIBE_CONFIGS operation
        /// </summary>
        DescribeConfigs = 10,

        /// <summary>
        ///     ALTER_CONFIGS operation
        /// </summary>
        AlterConfigs = 11,

        /// <summary>
        ///     IDEMPOTENT_WRITE operation
        /// </summary>
        IdempotentWrite = 12,
    }
}
