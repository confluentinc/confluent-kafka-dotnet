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
    ///     A deserializer for big endian encoded (network byte ordered) <see cref="System.Int32"/> values.
    /// </summary>
    public class IntDeserializer : IDeserializer<int>
    {
        /// <summary>
        ///     Deserializes a big endian encoded (network byte ordered) <see cref="System.Int32"/> value from a byte array.
        /// </summary>
        /// <param name="data">
        ///     A byte array containing the serialized <see cref="System.Int32"/> value (big endian encoding).
        /// </param>
        /// <returns>
        ///     The deserialized <see cref="System.Int32"/> value.
        /// </returns>
        public int Deserialize(byte[] data)
        {
            // network byte order -> big endian -> most significant byte in the smallest address.
            return
                (((int)data[0]) << 24) |
                (((int)data[1]) << 16) |
                (((int)data[2]) << 8) |
                (int)data[3];
        }
    }
}
