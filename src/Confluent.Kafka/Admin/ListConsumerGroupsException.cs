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
    ///     Represents an error that occured during a list consumer group operation.
    /// </summary>
    public class ListConsumerGroupsException : KafkaException
    {
        /// <summary>
        ///     Initialize a new instance of ListConsumerGroupsException.
        /// </summary>
        /// <param name="report">
        ///     The result corresponding to all groups in the request
        /// </param>
        public ListConsumerGroupsException(ListConsumerGroupsReport report)
            : base(new Error(ErrorCode.Local_Partial,
                   "error listing consumer groups"))
        {
            this.Results = report;
        }

        /// <summary>
        ///     The result corresponding to all groups and including all errors.
        ///     Results.Errors will be non-empty and should be checked.
        /// </summary>
        public ListConsumerGroupsReport Results { get; }
    }
}
