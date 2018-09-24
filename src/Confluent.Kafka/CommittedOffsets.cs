// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates information provided to a Consumer's OnOffsetsCommitted
    ///     event - per-partition offsets and success/error together with overall 
    ///     success/error of the commit operation.
    /// </summary> 
    /// <remarks>
    ///     Possible error conditions:
    ///     - Entire request failed: Error is set, but not per-partition errors.
    ///     - All partitions failed: Error is set to the value of the last failed partition, but each partition may have different errors.
    ///     - Some partitions failed: global error is success.
    /// </remarks>
    public class CommittedOffsets
    {
        /// <summary>
        ///     Initializes a new instance of CommittedOffsets.
        /// </summary>
        /// <param name="offsets">
        ///     per-partition offsets and success/error.
        /// </param>
        /// <param name="error">
        ///     overall operation success/error.
        /// </param>
        public CommittedOffsets(IList<TopicPartitionOffsetError> offsets, Error error)
        {
            Offsets = offsets;
            Error = error;
        }

        /// <summary>
        ///     Gets the overall operation success/error.
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     Gets the per-partition offsets and success/error.
        /// </summary>
        public IList<TopicPartitionOffsetError> Offsets { get; }
    }
}
