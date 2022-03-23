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
    ///     A wrapped segment serializer to facilitate up-casting to the segment based API
    /// </summary>
    /// <typeparam name="T">The type of the value that can be serialized by this serializer</typeparam>
    public class WrappedSyncSegmentSerializer<T> : ISegmentSerializer<T>
    {
        private readonly ISerializer<T> serializer;

        /// <summary>
        ///     Created a <see cref="WrappedSyncSegmentSerializer{T}"/> using an inner <see cref="ISerializer{T}"/>.
        ///     This just decorates the returned byte array in an array segment wrapper using the length of the
        ///     returned array to establish bounds.
        /// </summary>
        /// <param name="innerSerializer">The inner serializer to use</param>
        public WrappedSyncSegmentSerializer(ISerializer<T> innerSerializer)
        {
            serializer = innerSerializer;
        }
        
        /// <inheritdoc cref="ISegmentSerializer{T}.Serialize"/>
        public ArraySegment<byte> Serialize(T data, SerializationContext context)
        {
            return new ArraySegment<byte>(serializer.Serialize(data, context));
        }

        /// <inheritdoc cref="ISegmentSerializer{T}.Release"/>
        public void Release(ref ArraySegment<byte> segment)
        {
            // Do nothing
        }
    }
}