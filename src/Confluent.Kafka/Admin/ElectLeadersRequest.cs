// Copyright 2024 Confluent Inc.
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
    ///      Request to elect a leader for a partition.
    ///      This is used to trigger a leader election for a partition.
    ///   </summary>
    public class ElectLeadersRequest
    {
        /// <summary>
        ///    The type of election to trigger.
        /// </summary>
        public ElectionType ElectionType { get; set; }

        /// <summary>
        ///     List of partitions to elect leaders for.
        ///  </summary>
        public List<TopicPartition> Partitions { get; set; }

    }
}
