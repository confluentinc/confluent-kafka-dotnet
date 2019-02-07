// Copyright 2019 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates information pertaining to a rebalance event.
    /// </summary> 
    public class RebalanceEvent
    {
        /// <summary>
        ///     Initializes a new RebalanceEvent instance.
        /// </summary>
        /// <param name="partitions">
        ///     The partition assignment. In the case of a revocation event,
        ///     this is the assignment that will be revoked.
        /// </param>
        /// <param name="isAssignment">
        ///     True if the rebalance event corresponds to a new partition
        ///     assigment. False if the rebalance event corresponds to a
        ///     revocation.
        /// </param>
        public RebalanceEvent(IList<TopicPartition> partitions, bool isAssignment)
        {
            Partitions = partitions;
            IsAssignment = isAssignment;
        }

        /// <summary>
        ///     The partition assignment. In the case of a
        ///     revocation event, this is the current assignment
        ///     set that is being revoked.
        /// </summary>
        public IList<TopicPartition> Partitions { get; private set; }

        /// <summary>
        ///     True if the rebalance event corresponds to a new
        ///     partition assignment.
        /// </summary>
        /// <value></value>
        public bool IsAssignment { get; private set; }

        /// <summary>
        ///     True if the rebalance event corresponds to an
        ///     assignment revocation.
        /// </summary>
        public bool IsRevocation => !IsAssignment;
    }
}
