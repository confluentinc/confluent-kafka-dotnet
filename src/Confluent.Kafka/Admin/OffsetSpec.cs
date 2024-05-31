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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Used in `ListOffsets` to specify the desired offsets
    ///     of the partition being queried.
    /// </summary>
    public abstract class OffsetSpec
    {
        private static EarliestSpec EarliestSpecInstance = new EarliestSpec();
        private static LatestSpec LatestSpecInstance = new LatestSpec();
        private static MaxTimestampSpec MaxTimestampSpecInstance = new MaxTimestampSpec();
        
        /// <summary>
        ///     Used to retrieve the earliest offset available.
        /// </summary>
        public class EarliestSpec : OffsetSpec
        {
            internal EarliestSpec()
            {
            }

            internal override long Value()
            {
                return -2;
            }
        }

        /// <summary>
        ///     Used to retrieve the latest offset available.
        /// </summary>
        public class LatestSpec : OffsetSpec
        {
            internal LatestSpec()
            {
            }

            internal override long Value()
            {
                return -1;
            }
        }

        /// <summary>
        ///     Used to retrieve the offset with the largest timestamp,
        ///     that could not correspond to the latest one as timestamps
        ///     can be specified client-side.
        /// </summary>
        public class MaxTimestampSpec : OffsetSpec {
            internal MaxTimestampSpec()
            {
            }

            internal override long Value()
            {
                return -3;
            }
        }

        /// <summary>
        ///      Used to retrieve the earliest offset whose timestamp is
        ///      greater than or equal to the given timestamp
        ///      in the corresponding partition.
        /// </summary> 
        public class TimestampSpec : OffsetSpec
        {
            /// <summary>
            ///     Timestamp for the OffsetSpec.
            /// </summary>
            public long Timestamp { get; }

            /// <summary>
            ///     Creates a new TimestampSpec with specified timestamp.
            /// </summary>
            internal TimestampSpec(long timestamp)
            {
                Timestamp = timestamp;
            }

            internal override long Value()
            {
                return Timestamp;
            }
        }

        /// <summary>
        ///     Returns the LatestSpec instance.
        /// </summary>
        public static OffsetSpec Latest()
        {
            return LatestSpecInstance;
        }

        /// <summary>
        ///     Returns the EarliestSpec instance.
        /// </summary>
        public static OffsetSpec Earliest()
        {
            return EarliestSpecInstance;
        }

        /// <summary>
        ///     Returns a new instance of TimestampSpec with the specified
        ///     timestamp.
        /// </summary>
        /// <param name="timestamp">Timestamp in milliseconds.</param>
        public static OffsetSpec ForTimestamp(long timestamp)
        {
            return new TimestampSpec(timestamp);
        }

        /// <summary>
        ///     Returns the MaxTimestamp instance.
        /// </summary>
        public static OffsetSpec MaxTimestamp()
        {
            return MaxTimestampSpecInstance;
        }
        
        internal abstract long Value();
    }
}
