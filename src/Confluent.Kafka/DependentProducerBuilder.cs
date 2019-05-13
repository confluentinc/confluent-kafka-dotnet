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
    ///     A builder class for <see cref="IProducer{TKey, TValue}" /> instance
    ///     implementations that leverage an existing client handle.
    ///
    ///     [API-SUBJECT-TO-CHANGE] - This class may be removed in the future
    ///     in favor of an improved API for this functionality.
    /// </summary>
    public class DependentProducerBuilder<TKey, TValue>
    {
        /// <summary>
        ///     The configured client handle.
        /// </summary>
        public Handle Handle { get; set; }
        
        /// <summary>
        ///     The configured key serializer.
        /// </summary>
        public ISerializer<TKey> KeySerializer { get; set; }

        /// <summary>
        ///     The configured value serializer.
        /// </summary>
        public ISerializer<TValue> ValueSerializer { get; set; }

        /// <summary>
        ///     The configured async key serializer.
        /// </summary>
        public IAsyncSerializer<TKey> AsyncKeySerializer { get; set; }

        /// <summary>
        ///     The configured async value serializer.
        /// </summary>
        public IAsyncSerializer<TValue> AsyncValueSerializer { get; set; }


        /// <summary>
        ///     An underlying librdkafka client handle that the Producer will use to 
        ///     make broker requests. The handle must be from another Producer
        ///     instance (not Consumer or AdminClient).
        /// </summary>
        public DependentProducerBuilder(Handle handle)
        {
            this.Handle = handle;
        }

        /// <summary>
        ///     The serializer to use to serialize keys.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetKeySerializer(ISerializer<TKey> serializer)
        {
            this.KeySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetValueSerializer(ISerializer<TValue> serializer)
        {
            this.ValueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The async serializer to use to serialize keys.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetKeySerializer(IAsyncSerializer<TKey> serializer)
        {
            this.AsyncKeySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The async serializer to use to serialize values.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetValueSerializer(IAsyncSerializer<TValue> serializer)
        {
            this.AsyncValueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     Build a new IProducer implementation instance.
        /// </summary>
        public virtual IProducer<TKey, TValue> Build()
        {
            return new Producer<TKey, TValue>(this);
        }
    }
}
