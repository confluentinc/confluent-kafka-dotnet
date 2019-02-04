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
using System.Collections.Concurrent;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka producer client that provides key
    ///     and value serialization.
    /// </summary>
    public interface IProducer<TKey, TValue> : IClient
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}.ProduceAsync(string, Message{TKey, TValue}, CancellationToken)" />
        /// </summary>
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            string topic,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}.ProduceAsync(TopicPartition, Message{TKey, TValue}, CancellationToken)" />
        /// </summary>
        Task<DeliveryResult<TKey, TValue>> ProduceAsync(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}.BeginProduce(string, Message{TKey, TValue}, Action{DeliveryReport{TKey, TValue}})" />
        /// </summary>
        void BeginProduce(
            string topic,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}.BeginProduce(TopicPartition, Message{TKey, TValue}, Action{DeliveryReport{TKey, TValue}})" />
        /// </summary>
        void BeginProduce(
            TopicPartition topicPartition,
            Message<TKey, TValue> message,
            Action<DeliveryReport<TKey, TValue>> deliveryHandler = null);

        
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}.Poll(TimeSpan)" />
        /// </summary>
        int Poll(TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}.Flush(TimeSpan)" />
        /// </summary>
        int Flush(TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Producer{TKey,TValue}.Flush(CancellationToken)" />
        /// </summary>
        void Flush(CancellationToken cancellationToken = default(CancellationToken));
    }
}
