// Copyright 2023 Confluent Inc.
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
    ///     Represents the result of a list offsets operation.
    /// </summary>
    public class ListOffsetsReport
    {
        /// <summary>
        ///     ListOffsetsResultInfo Elements for all the TopicPartitions queried
        ///     for ListOffsets.
        /// </summary>
        public List<ListOffsetsResultInfo> ListOffsetsResultInfos { get; set; }

        /// <summary>
        ///   List of non-client level errors encountered while listing offsets.
        /// </summary>
        public Error Error { get; set; }

        /// <summary>
        ///    Returns a human readable representation of this object.
        /// </summary>
        public override string ToString()
        {
            string res = "ListOffsetsReport :\n";
            foreach (var listoffsetsresultinfo in ListOffsetsResultInfos)
            {
                res += listoffsetsresultinfo.ToString();
            }
            return res;
        }
    }
}
