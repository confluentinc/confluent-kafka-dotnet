// Copyright 2021 Confluent Inc.
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
    ///     The per-partition result of delete records request.
    /// </summary>
    public class DeleteRecordsResult
    {
        /// <summary>
        ///     The topic name.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        ///     The partition.
        /// </summary>
        public Partition Partition { get; set; }

        /// <summary>
        ///     Post-deletion low-watermark offset (smallest available offset of all
        ///     live replicas).
        /// </summary>
        public Offset Offset { get; set; }

        internal Error Error { get; set; }
    }
}
