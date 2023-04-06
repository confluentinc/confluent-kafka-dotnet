// Copyright 2018 Confluent Inc.
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

namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a serializer for use with <see cref="Confluent.Kafka.Producer{TKey,TValue}" />.
    /// </summary>
    public interface ISegmentSerializer<T>
    {
        /// <summary>
        ///     Serialize the key or value of a <see cref="Message{TKey,TValue}" />
        ///     instance.
        /// </summary>
        /// <param name="data">
        ///     The value to serialize.
        /// </param>
        /// <param name="context">
        ///     Context relevant to the serialize operation.
        /// </param>
        /// <returns>
        ///     The serialized value.
        /// </returns>
        ArraySegment<byte> Serialize(T data, SerializationContext context);
        
        /// <summary>
        ///     Release resources associated with the array segment
        /// </summary>
        /// <param name="segment">The segment that was created in the <see cref="Serialize"/> call.</param>
        void Release(ref ArraySegment<byte> segment);
    }
}