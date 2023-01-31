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
    ///     Represents an error that occured during altering consumer group offsets.
    /// </summary>
    public class AlterConsumerGroupOffsetsException : KafkaException
    {
        /// <summary>
        ///     Initializes a new instance of AlterConsumerGroupOffsetsException.
        /// </summary>
        /// <param name="results">
        ///     The result corresponding to all groups/partitions in the request
        ///     (whether or not they were in error). At least one of these
        ///     results will be in error.
        /// </param>
        public AlterConsumerGroupOffsetsException(List<AlterConsumerGroupOffsetsReport> results)
            : base(new Error(ErrorCode.Local_Partial,
                "An error occurred altering consumer group offsets, check individual result elements"))
        {
            Results = results;
        }

        /// <summary>
        ///     The result corresponding to all groups/partitions in the request
        ///     (whether or not they were in error). At least one of these
        ///     results will be in error.
        /// </summary>
        public List<AlterConsumerGroupOffsetsReport> Results { get; }
    }
}
