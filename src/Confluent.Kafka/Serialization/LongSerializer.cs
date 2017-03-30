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

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     System.Int64 serializer. Byte order of serialized data is big endian (network byte order).
    /// </summary>
    public class LongSerializer : ISerializer<long>
    {
        /// <summary>
        ///     Serializes the specified System.Int64 value to a byte array of length 8. Byte order is big endian (network byte order).
        /// </summary>
        /// <param name="val">
        ///     The System.Int64 value to serialize.
        /// </param>
        /// <returns>
        ///     The System.Int64 value <paramref name="val" /> encoded as a byte array of length 8 (network byte order).
        /// </returns>
        public byte[] Serialize(long val)
        {
            var result = new byte[8];
            result[0] = (byte)(val >> 56);
            result[1] = (byte)(val >> 48);
            result[2] = (byte)(val >> 40);
            result[3] = (byte)(val >> 32);
            result[4] = (byte)(val >> 24);
            result[5] = (byte)(val >> 16);
            result[6] = (byte)(val >> 8);
            result[7] = (byte)val;
            return result;
        }
    }
}
