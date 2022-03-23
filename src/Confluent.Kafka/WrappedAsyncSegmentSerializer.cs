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
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    
    /// <summary>
    ///     A wrapped segment serializer to facilitate up-casting to the segment based API
    /// </summary>
    /// <typeparam name="T">The type of the value that can be serialized by this serializer</typeparam>
    public class WrappedAsyncSyncSegmentSerializer<T> : IAsyncSegmentSerializer<T>
    {
        private readonly IAsyncSerializer<T> serializer;

        /// <summary>
        ///     Created a <see cref="WrappedAsyncSyncSegmentSerializer{T}"/> using an inner <see cref="IAsyncSerializer{T}"/>.
        ///     This just decorates the returned byte array in an array segment wrapper using the length of the
        ///     returned array to establish bounds. This does require awaiting on the returned task.
        /// </summary>
        /// <param name="innerSerializer">The inner serializer to use</param>
        public WrappedAsyncSyncSegmentSerializer(IAsyncSerializer<T> innerSerializer)
        {
            serializer = innerSerializer;
        }
        
        /// <inheritdoc cref="IAsyncSegmentSerializer{T}.SerializeAsync"/>
        public async Task<ArraySegment<byte>> SerializeAsync(T data, SerializationContext context)
        {
            byte[] result = await serializer.SerializeAsync(data, context).ConfigureAwait(false);
            return new ArraySegment<byte>(result);
        }
        
        /// <inheritdoc cref="IAsyncSegmentSerializer{T}.Release"/>
        public void Release(ref ArraySegment<byte> segment)
        {
            // Do nothing
        }
    }
}