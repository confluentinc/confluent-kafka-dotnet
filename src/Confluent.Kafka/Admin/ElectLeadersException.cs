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

using System.Linq;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///    Represents an error that occured during the ElectLeaders operation.
    ///  </summary>
    public class ElectLeadersException : KafkaException
    {
        /// <summary>
        ///     Initializes a new instance of ElectLeadersException.
        /// </summary>
        /// <param name="report">
        ///     The result of the ElectLeaders operation.
        /// </param>
        public ElectLeadersException(ElectLeadersReport report)
            :  base(new Error(ErrorCode.Local_Partial,
                "Some errors occurred electing leaders: [" +
                string.Join(", ", report.TopicPartitions.Where(tp => tp.Error.IsError)) +
                "]"))
        {
            this.Results = report;
        }

        /// <summary>
        ///     Gets the results of the ElectLeaders operation.
        /// </summary>
        public ElectLeadersReport Results { get; }
    }
}
