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
    ///     Encapsulates information provided to a Consumer's Offset based operations
    ///     - per-partition offsets and success/error together with overall 
    ///     success/error of the operation.
    /// </summary> 
    /// <remarks>
    ///     Possible error conditions:
    ///     //TODO: 08/09/2017 @vinodres: What are possible error conditions?
    /// </remarks>
    public class OffsetResults
    {
        //TODO: 08/29/2017 @vinodres: This class is similar to CommitedOffsets, 
        //     but using CommittedOffsets in StoreOffsets feature sounds misleading
        //     So created OffsetResults. CommittedOffsets should be depricated and 
        //     its usage should be replaced with OffsetResults.

        /// <summary>
        ///     Initializes a new instance of OffsetResults.
        /// </summary>
        /// <param name="offsets">
        ///     per-partition offsets and success/error.
        /// </param>
        /// <param name="error">
        ///     overall operation success/error.
        /// </param>
        public OffsetResults(IList<TopicPartitionOffsetError> offsets, Error error)
        {
            Offsets = offsets;
            Error = error;
        }

        /// <summary>
        ///     Initializes a new instance of OffsetResults with
        ///     overall operation success.
        /// </summary>
        /// <param name="offsets">
        ///     per-partition offsets and success/error.
        /// </param>
        public OffsetResults(IList<TopicPartitionOffsetError> offsets)
        {
            Offsets = offsets;
            Error = new Error(ErrorCode.NoError);
        }

        /// <summary>
        ///     Initializes a new instance of OffsetResults with
        ///     an empty per-partition list and the overall 
        ///     success/error <paramref name="error" />.
        /// </summary>
        /// <param name="error"></param>
        public OffsetResults(Error error)
        {
            Offsets = new List<TopicPartitionOffsetError>();
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
