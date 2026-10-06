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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents the low and high watermark offsets of a Kafka 
    ///     topic/partition.
    /// </summary>
    /// <remarks>
    ///     You can identify a partition that has not yet been written
    ///     to by checking if the high watermark equals 0.
    /// </remarks>
    public class WatermarkOffsets
    {
        /// <summary>
        ///     Initializes a new instance of the WatermarkOffsets class
        ///     with the specified offsets.
        /// </summary>
        /// <param name="low">
        ///     The offset of the earliest message in the topic/partition. If 
        ///     no messages have been written to the topic, the low watermark
        ///     offset is set to 0. The low watermark will also be 0 if 
        ///     one message has been written to the partition (with offset 0).
        /// </param>
        /// <param name="high">
        ///     The high watermark offset, which is the offset of the latest
        ///     message in the topic/partition available for consumption + 1.
        /// </param>
        public WatermarkOffsets(Offset low, Offset high)
        {
            Low = low;
            High = high;
        }

        /// <summary>
        ///     Gets the offset of the earliest message in the topic/partition. If 
        ///     no messages have been written to the topic, the low watermark
        ///     offset is set to 0. The low watermark will also be 0 if 
        ///     one message has been written to the partition (with offset 0).
        /// </summary>
        public Offset Low { get; }

        /// <summary>
        ///     Gets the high watermark offset, which is the offset of the latest
        ///     message in the topic/partition available for consumption + 1.
        /// </summary>
        public Offset High { get; }

        /// <summary>
        ///     Returns a string representation of the WatermarkOffsets object.
        /// </summary>
        /// <returns>
        ///     A string representation of the WatermarkOffsets object.
        /// </returns>
        public override string ToString()
            => $"{Low} .. {High}";
    }
}
