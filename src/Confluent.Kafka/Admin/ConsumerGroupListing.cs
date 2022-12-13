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
    ///     Represents a single consumer group in the result of a list consumer
    ///     groups operation.
    /// </summary>
    public class ConsumerGroupListing
    {
        /// <summary>
        ///     The groupID.
        /// </summary>
        public string GroupId { get; set; }

        /// <summary>
        ///     The state of the consumer group.
        /// </summary>
        public ConsumerGroupState State { get; set; }

        /// <summary>
        ///     Whether the consumer group is simple or not.
        /// </summary>
        public bool IsSimpleConsumerGroup { get; set; }

        /// <summary>
        ///    Returns a human readable representation of this object.
        /// </summary>
        public override string ToString()
        {
            return $"{GroupId}, State = {State}, IsSimpleConsumerGroup = {IsSimpleConsumerGroup}";
        }
    }
}
