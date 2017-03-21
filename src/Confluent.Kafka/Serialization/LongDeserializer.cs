// Copyright 2016-2017 Confluent Inc.
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

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A deserializer for big endian encoded (network byte ordered) System.Int64 values.
    /// </summary>
    public class LongDeserializer : IDeserializer<long>
    {
        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) System.Int64 value from a byte array.
        /// </summary>
        /// <param name="data">
        ///     A byte array containing the serialized System.Int64 value (big endian encoding)
        /// </param>
        /// <returns>
        ///     The deserialized System.Int64 value.
        /// </returns>
        public long Deserialize(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentException($"Arg [{nameof(data)}] is null");
            }

            if (data.Length != 8)
            {
                throw new ArgumentException($"Size of {nameof(data)} received by LongDeserializer is not 8");
            }

            // network byte order -> big endian -> most significant byte in the smallest address.
            long result = ((long)data[0]) << 56 |
                ((long)(data[1])) << 48 |
                ((long)(data[2])) << 40 |
                ((long)(data[3])) << 32 |
                ((long)(data[4])) << 24 |
                ((long)(data[5])) << 16 |
                ((long)(data[6])) << 8 |
                (data[7]);
            return result;
        }
    }
}
