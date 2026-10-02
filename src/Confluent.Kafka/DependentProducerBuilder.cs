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
        ///     The configured key serializer builder.
        /// </summary>
        public ISerializerBuilder<TKey> KeySerializerBuilder { get; set; }

        /// <summary>
        ///     The configured value serializer builder.
        /// </summary>
        public ISerializerBuilder<TValue> ValueSerializerBuilder { get; set; }

        /// <summary>
        ///     The configured async key serializer builder.
        /// </summary>
        public IAsyncSerializerBuilder<TKey> AsyncKeySerializerBuilder { get; set; }

        /// <summary>
        ///     The configured async value serializer builder.
        /// </summary>
        public IAsyncSerializerBuilder<TValue> AsyncValueSerializerBuilder { get; set; }


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
            ThrowIfKeySerializerBuilderSet();
            this.KeySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The serializer to use to serialize values.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetValueSerializer(ISerializer<TValue> serializer)
        {
            ThrowIfValueSerializerBuilderSet();
            this.ValueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The async serializer to use to serialize keys.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetKeySerializer(IAsyncSerializer<TKey> serializer)
        {
            ThrowIfKeySerializerBuilderSet();
            this.AsyncKeySerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The async serializer to use to serialize values.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetValueSerializer(IAsyncSerializer<TValue> serializer)
        {
            ThrowIfValueSerializerBuilderSet();
            this.AsyncValueSerializer = serializer;
            return this;
        }

        /// <summary>
        ///     The builder of the serializer to use to serialize keys.
        ///
        ///     In contrast to <see cref="SetKeySerializer(ISerializer{TKey})" />, the
        ///     serializer is constructed by the producer, which supplies the
        ///     configuration of the producer owning the handle to the builder. A
        ///     serializer constructed this way is owned by the dependent producer
        ///     and is disposed along with it.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetKeySerializerBuilder(ISerializerBuilder<TKey> serializerBuilder)
        {
            ThrowIfKeySerializerSet();
            this.KeySerializerBuilder = serializerBuilder;
            return this;
        }

        /// <summary>
        ///     The builder of the serializer to use to serialize values.
        ///
        ///     In contrast to <see cref="SetValueSerializer(ISerializer{TValue})" />, the
        ///     serializer is constructed by the producer, which supplies the
        ///     configuration of the producer owning the handle to the builder. A
        ///     serializer constructed this way is owned by the dependent producer
        ///     and is disposed along with it.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetValueSerializerBuilder(ISerializerBuilder<TValue> serializerBuilder)
        {
            ThrowIfValueSerializerSet();
            this.ValueSerializerBuilder = serializerBuilder;
            return this;
        }

        /// <summary>
        ///     The builder of the async serializer to use to serialize keys.
        ///
        ///     In contrast to <see cref="SetKeySerializer(IAsyncSerializer{TKey})" />, the
        ///     serializer is constructed by the producer, which supplies the
        ///     configuration of the producer owning the handle to the builder. A
        ///     serializer constructed this way is owned by the dependent producer
        ///     and is disposed along with it.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetKeySerializerBuilder(IAsyncSerializerBuilder<TKey> serializerBuilder)
        {
            ThrowIfKeySerializerSet();
            this.AsyncKeySerializerBuilder = serializerBuilder;
            return this;
        }

        /// <summary>
        ///     The builder of the async serializer to use to serialize values.
        ///
        ///     In contrast to <see cref="SetValueSerializer(IAsyncSerializer{TValue})" />, the
        ///     serializer is constructed by the producer, which supplies the
        ///     configuration of the producer owning the handle to the builder. A
        ///     serializer constructed this way is owned by the dependent producer
        ///     and is disposed along with it.
        /// </summary>
        public DependentProducerBuilder<TKey, TValue> SetValueSerializerBuilder(IAsyncSerializerBuilder<TValue> serializerBuilder)
        {
            ThrowIfValueSerializerSet();
            this.AsyncValueSerializerBuilder = serializerBuilder;
            return this;
        }

        // A serializer and a serializer builder are mutually exclusive for the
        // same message component. Setting a serializer twice remains permitted,
        // as it always was for this builder.

        private void ThrowIfKeySerializerBuilderSet()
        {
            if (this.KeySerializerBuilder != null || this.AsyncKeySerializerBuilder != null)
            {
                throw new InvalidOperationException("Key serializer may not be specified more than once.");
            }
        }

        private void ThrowIfValueSerializerBuilderSet()
        {
            if (this.ValueSerializerBuilder != null || this.AsyncValueSerializerBuilder != null)
            {
                throw new InvalidOperationException("Value serializer may not be specified more than once.");
            }
        }

        private void ThrowIfKeySerializerSet()
        {
            if (this.KeySerializer != null || this.AsyncKeySerializer != null
                || this.KeySerializerBuilder != null || this.AsyncKeySerializerBuilder != null)
            {
                throw new InvalidOperationException("Key serializer may not be specified more than once.");
            }
        }

        private void ThrowIfValueSerializerSet()
        {
            if (this.ValueSerializer != null || this.AsyncValueSerializer != null
                || this.ValueSerializerBuilder != null || this.AsyncValueSerializerBuilder != null)
            {
                throw new InvalidOperationException("Value serializer may not be specified more than once.");
            }
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
