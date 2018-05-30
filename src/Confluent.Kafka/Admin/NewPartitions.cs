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

using System.Collections.Generic;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Passed to the CreatePartitions method.
    /// </summary>
    public class NewPartitions
    {
        /// <summary>
        ///     The replica assignments for the new partitions, or null if the assignment
        ///     will be done by the controller.
        /// </summary>
        public List<List<int>> Assignments { get; set; } = null;

        /// <summary>
        ///     Increase the partition count for a topic to the given totalCount
        /// </summary>
        public int IncreaseTo { get; set; }
    }
}
