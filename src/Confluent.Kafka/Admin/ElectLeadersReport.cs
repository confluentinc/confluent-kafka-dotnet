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
using System.Text;
using System.Linq;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///   First error encountered in TopicPartitions
    /// </summary>
    public class ElectLeadersReport
    {
        /// <summary>
        ///   Operational Error status.
        /// </summary>
        internal Error Error { get; set; }

        /// <summary>
        ///   Individual partition results.
        /// </summary>
        public List<TopicPartitionError> TopicPartitions { get; set; }

        /// <summary>
        ///   A Json representation of the object.
        /// </summary>
        /// <returns>
        ///   A Json representation of the object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append($"{{\"TopicPartitions\": [");
            result.Append(string.Join(",", TopicPartitions.Select(b => $" {b.ToString()}")));
            result.Append($"]}}");
            return result.ToString();
        }
    }
}