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
    ///     Represents a single consumer group's description in the result of a
    ///     describe consumer group operation.
    /// </summary>
    public class ConsumerGroupDescription
    {
        /// <summary>
        ///     The groupID.
        /// </summary>
        public string GroupId { get; set; }

        /// <summary>
        ///     Error, if any, of result
        /// </summary>
        public Error Error { get; set; }

        /// <summary>
        ///     Whether the consumer group is simple or not.
        /// </summary>
        public bool IsSimpleConsumerGroup { get; set; }

        /// <summary>
        ///     Partition assignor identifier.
        /// </summary>
        public string PartitionAssignor { get; set; }

        /// <summary>
        ///     Consumer group state.
        /// </summary>
        public ConsumerGroupState State { get; set; }

        /// <summary>
        ///     Consumer group coordinator (broker).
        /// </summary>
        public Node Coordinator { get; set; }

        /// <summary>
        ///    Members list.
        /// </summary>
        public List<MemberDescription> Members { get; set; }
    }
}
