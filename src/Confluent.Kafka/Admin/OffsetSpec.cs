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
    ///     OffsetSpec.
    /// </summary>
    public abstract class OffsetSpec
    {
        public class EarliestSpec : OffsetSpec { }
        public class LatestSpec : OffsetSpec { }
        public class MaxTimestampSpec : OffsetSpec { }
        public class TimestampSpec : OffsetSpec
        {
            public long Timestamp { get; set; }
            public TimestampSpec(long timestamp)
            {
                Timestamp = timestamp;
            }

        }

        /// <summary>
        /// Used to retrieve the latest offset of a partition.
        /// </summary>
        public static OffsetSpec Latest()
        {
            return new LatestSpec();
        }

        /// <summary>
        /// Used to retrieve the earliest offset of a partition.
        /// </summary>
        public static OffsetSpec Earliest()
        {
            return new EarliestSpec();
        }

        /// <summary>
        /// Used to retrieve the earliest offset whose timestamp is greater than
        /// or equal to the given timestamp in the corresponding partition.
        /// </summary>
        /// <param name="timestamp">Timestamp in milliseconds.</param>
        public static OffsetSpec ForTimestamp(long timestamp)
        {
            return new TimestampSpec(timestamp);
        }

        /// <summary>
        /// Used to retrieve the offset with the largest timestamp of a partition.
        /// </summary>
        public static OffsetSpec MaxTimestamp()
        {
            return new MaxTimestampSpec();
        }
}
}
