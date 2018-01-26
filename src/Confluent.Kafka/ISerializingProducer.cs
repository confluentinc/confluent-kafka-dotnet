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
using System.Collections.Generic;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Describes the minimum functionality provided by a high level (serializing) Kafka producer.
    /// </summary>
    public interface ISerializingProducer<TKey, TValue>
    {
        /// <include file='include_docs_producer.xml' path='API/Member[@name="KeySerializer"]/*' />
        ISerializer<TKey> KeySerializer { get; }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ValueSerializer"]/*' />
        ISerializer<TValue> ValueSerializer { get; }

        /// <include file='include_docs_client.xml' path='API/Member[@name="Name"]/*' />
        string Name { get; }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<Message<TKey, TValue>> ProduceAsync(Message<TKey, TValue> message);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_topic_TKey_TValue"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Partition_TKey_TValue_Timestamp_IEnumerable"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<Message<TKey, TValue>> ProduceAsync(string topic, Partition partition, TKey key, TValue val, Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_Message_IDeliveryHandler"]/*' />
        void Produce(Message<TKey, TValue> message, IDeliveryHandler<TKey, TValue> deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_string_TKey_TValue_IDeliveryHandler"]/*' />
        void Produce(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_string_Partition_TKey_TValue_Timestamp_IEnumerable_IDeliveryHandler"]/*' />
        void Produce(string topic, Partition partition, TKey key, TValue val, Timestamp timestamp, IEnumerable<KeyValuePair<string, byte[]>> headers, IDeliveryHandler<TKey, TValue> deliveryHandler);

#region obsolete produce methods

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("The Producer API has been revised and this overload of ProduceAsync has been depreciated. Please use another variant of ProduceAsync.")]
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, int partition);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        Task<Message<TKey, TValue>> ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. ")]
        void ProduceAsync(string topic, TKey key, TValue val, IDeliveryHandler<TKey, TValue> deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete(
            "Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. " +
            "Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        void ProduceAsync(string topic, TKey key, TValue val, int partition, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete("Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. ")]
        void ProduceAsync(string topic, TKey key, TValue val, int partition, IDeliveryHandler<TKey, TValue> deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Obsolete"]/*' />
        [Obsolete(
            "Variants of ProduceAsync that include a IDeliveryHandler parameter are depreciated - use a variant of Produce instead. " +
            "Variants of ProduceAsync that include a blockIfQueueFull parameter are depreciated - use the dotnet.producer.block.if.queue.full configuration property instead.")]
        void ProduceAsync(string topic, TKey key, TValue val, bool blockIfQueueFull, IDeliveryHandler<TKey, TValue> deliveryHandler);

#endregion

    }
}
