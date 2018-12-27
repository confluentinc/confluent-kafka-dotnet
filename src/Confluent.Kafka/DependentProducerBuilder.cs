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
using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     A builder class for <see cref="Producer{TKey, TValue}" /> instances.
    /// </summary>
    public class DependentProducerBuilder<TKey, TValue>
    {
        internal Handle handle;
        
        internal ISerializer<TKey> keySerializer;
        internal ISerializer<TValue> valueSerializer;
        internal IAsyncSerializer<TKey> asyncKeySerializer;
        internal IAsyncSerializer<TValue> asyncValueSerializer;


        /// <summary>
        ///     An underlying librdkafka client handle that the Producer will use to 
        ///     make broker requests. The handle must be from another Producer
        ///     instance (not Consumer or AdminClient).
        /// </summary>
        public DependentProducerBuilder(Handle handle)
        {
            this.handle = handle;
        }

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetKeySerializer(ISerializer<TKey> serializer)
        {
            this.keySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetValueSerializer(ISerializer<TValue> serializer)
        {
            this.valueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetKeySerializer(IAsyncSerializer<TKey> serializer)
        {
            this.asyncKeySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetValueSerializer(IAsyncSerializer<TValue> serializer)
        {
            this.asyncValueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     Build a new Producer instance.
        /// </summary>
        public Producer<TKey, TValue> Build()
        {
            return new Producer<TKey, TValue>(this);
        }
    }
}
