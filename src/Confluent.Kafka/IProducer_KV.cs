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
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Runtime.InteropServices;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;
using System.Collections.Concurrent;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka producer client that provides key
    ///     and value serialization.
    /// </summary>
    public interface IProducer<TKey, TValue> : IClient
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

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_int"]/*' />
        int Flush(int millisecondsTimeout);

        /// <include file='include_docs_producer.xml' path='API/Member[@name="Flush_TimeSpan"]/*' />
        int Flush(TimeSpan timeout);
    }
}
