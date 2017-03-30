﻿// Copyright 2016-2017 Confluent Inc.
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
    ///     System.Single serializer. Byte order of serialized data is big endian (network byte order).
    /// </summary>
    public class FloatSerializer : ISerializer<float>
    {
        /// <summary>
        ///     Serializes the specified System.Single value to a byte array of length 4. Byte order is big endian (network byte order).
        /// </summary>
        /// <param name="data">
        ///     The System.Single value to serialize.
        /// </param>
        /// <returns>
        ///     The System.Single value <paramref name="data" /> encoded as a byte array of length 4 (network byte order).
        /// </returns>
        public byte[] Serialize(float data)
        {
            byte[] result = new byte[4];
            unsafe
            {
                byte* p = (byte*)(&data);
                result[3] = *p++;
                result[2] = *p++;
                result[1] = *p++;
                result[0] = *p++;
            }

            return result;
        }
    }
}
