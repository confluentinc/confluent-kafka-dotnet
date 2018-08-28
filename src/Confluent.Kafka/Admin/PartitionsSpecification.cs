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
    ///     Specification for new partitions to be added to a topic.
    /// </summary>
    public class PartitionsSpecification
    {
        /// <summary>
        ///     The topic that the new partitions specification corresponds to.
        /// </summary>
        public string Topic { get; set; }
        
        /// <summary>
        ///     The replica assignments for the new partitions, or null if the assignment
        ///     will be done by the controller. The outer list is indexed by the new 
        ///     partitions relative index, and the inner list contains the broker ids.
        /// </summary>
        public List<List<int>> ReplicaAssignments { get; set; } = null;

        /// <summary>
        ///     The partition count for the specified topic is increased to this value.
        /// </summary>
        public int IncreaseTo { get; set; }
    }
}
