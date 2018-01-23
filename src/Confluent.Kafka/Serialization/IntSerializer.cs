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
using System.Collections.Generic;


namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A serializer for <see cref="System.Int32"/> values. The byte order of serialized data is big endian (network byte order).
    /// </summary>
    public class IntSerializer : ISerializer<int>
    {
        /// <summary>
        ///     Serializes the specified <see cref="System.Int32"/> value to a byte array of length 4. Byte order is big endian (network byte order).
        /// </summary>
        /// <param name="data">
        ///     The <see cref="System.Int32"/> value to serialize.
        /// </param>
        /// <param name="topic">
        ///     The topic associated with the data (ignored by this serializer).
        /// </param>
        /// <returns>
        ///     The <see cref="System.Int32"/> value <paramref name="data" /> encoded as a byte array of length 4 (network byte order).
        /// </returns>
        public byte[] Serialize(string topic, int data)
        {
            var result = new byte[4]; // int is always 32 bits on .NET.
            // network byte order -> big endian -> most significant byte in the smallest address.
            // Note: At the IL level, the conv.u1 operator is used to cast int to byte which truncates
            // the high order bits if overflow occurs.
            // https://msdn.microsoft.com/en-us/library/system.reflection.emit.opcodes.conv_u1.aspx
            result[0] = (byte)(data >> 24);
            result[1] = (byte)(data >> 16); // & 0xff;
            result[2] = (byte)(data >> 8); // & 0xff;
            result[3] = (byte)data; // & 0xff;
            return result;
        }

        /// <include file='../include_docs.xml' path='API/Member[@name="ISerializer_Configure"]/*' />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
            => config;

        /// <summary>
        ///     Releases any unmanaged resources owned by the serializer (noop for this type).
        /// </summary>
        public void Dispose() {}
    }
}
