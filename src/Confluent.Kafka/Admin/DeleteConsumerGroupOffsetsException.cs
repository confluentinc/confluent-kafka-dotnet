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
using System.Linq;
using System.Collections.Generic;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Represents an error that occured during a delete consumer group offset request.
    /// </summary>
    public class DeleteConsumerGroupOffsetsException : KafkaException
    {
        /// <summary>
        ///     Initializes a new instance of DeleteConsumerGroupOffsetsException.
        /// </summary>
        /// <param name="result">
        ///     The result corresponding to all the delete consumer group offsets
        ///     operations in the request (whether or not they were in error). 
        ///     At least one of these results will be in error.
        /// </param>
        public DeleteConsumerGroupOffsetsException(DeleteConsumerGroupOffsetsReport result)
            : base(new Error(ErrorCode.Local_Partial,
                "An error occurred deleting consumer group offset: [" + result.Group +
                "]: [" + String.Join(", ", result.Error.IsError ? new[] { result.Error } : result.Partitions.Where(r => r.Error.IsError).Select(r => r.Error)) +
                "]."))
        {
            Result = result;
        }

        /// <summary>
        ///     The result corresponding to the delete consumer group offsets
        ///     operation in the request.
        /// </summary>
        public DeleteConsumerGroupOffsetsReport Result { get; }
    }
}
