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
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    /// <summary>
    ///     This interface describes the minimum functionality
    ///     to be provided by a high level (serializing) Kafka 
    ///     producer.
    /// </summary>
    public interface ISerializingProducer<TKey, TValue>
    {
        /// <summary>
        ///     Gets the name of the underlying producer instance.
        /// </summary>
        string Name { get; }

        /// <summary>
        ///     Gets the ISerializer implementation instance used to serialize keys.
        /// </summary>
        ISerializer<TKey> KeySerializer { get; }

        /// <summary>
        ///     Gets the ISerializer implementation instance used to serialize values.
        /// </summary>
        ISerializer<TValue> ValueSerializer { get; }

        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val);
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull);
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition);
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull);

        void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler);
        void ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler);
        void ProduceAsync(string topic, TKey key, TValue val, int partition, IDeliveryHandler<TKey, TValue> deliveryHandler);
        void ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler);
    }
}
