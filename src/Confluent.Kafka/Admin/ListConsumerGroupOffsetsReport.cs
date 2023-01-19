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

using System;
using System.Collections.Generic;

namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     The per-group result for a list consumer group offsets request, and
    ///     an error.
    /// </summary>
    public class ListConsumerGroupOffsetsReport
    {
        /// <summary>
        ///     The groupID.
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        ///     List of topic TopicPartitionOffsetError containing the read offsets,
        ///     and errors if any.
        /// </summary>
        public List<TopicPartitionOffsetError> Partitions { get; set; }

        /// <summary>
        ///     Error, if any, on a group-level.
        /// </summary>
        public Error Error { get; set; }

        /// <summary>
        ///    Returns a human readable representation of this object.
        /// </summary>
        public override string ToString() {
            var errString = Error.IsError ? Error.ToString() : "";
            return $"{Group} [ {String.Join(", ", Partitions)} ] {errString}";
        }
    }
}
