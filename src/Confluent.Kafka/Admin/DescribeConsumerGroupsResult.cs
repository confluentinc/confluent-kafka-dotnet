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
    ///     Represents the result of a describe consumer group operation.
    /// </summary>
    public class DescribeConsumerGroupsResult
    {
        /// <summary>
        ///    List of consumer group descriptions.
        /// </summary>
        public List<ConsumerGroupDescription> ConsumerGroupDescriptions { get; set; }

        /// <summary>
        ///    Returns a human readable representation of this object.
        /// </summary>
        public override string ToString() {
            string res = "Groups:\n";
            foreach (ConsumerGroupDescription cgd in ConsumerGroupDescriptions) {
                res += "\t" + cgd.ToString() + "\n";
            }
            return res;
        }
    }
}
