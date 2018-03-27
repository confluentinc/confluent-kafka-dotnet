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
    ///     A focused interface for producing messages to Kafka with key and 
    ///     value serialization (excludes general client functionality).
    /// </summary>
    public interface ISerializingProducer<TKey, TValue>
    {
        /// <include file='include_docs_producer.xml' path='API/Member[@name="KeySerializer"]/*' />
        ISerializer<TKey> KeySerializer { get; }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ValueSerializer"]/*' />
        ISerializer<TValue> ValueSerializer { get; }

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<DeliveryReport<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_TopicPartition_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_Common"]/*' />
        Task<DeliveryReport<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_string_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_Action"]/*' />
        void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="ProduceAsync_TopicPartition_Message"]/*' />
        /// <include file='include_docs_producer.xml' path='API/Member[@name="Produce_Action"]/*' />
        void Produce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>> deliveryHandler);
    }
}
