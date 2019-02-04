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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka consumer.
    /// </summary>
    public interface IConsumer : IConsumerBase, IClient
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetPartitionsAssignedHandler(Action{IConsumer{TKey,TValue},List{TopicPartition}})" />.
        /// </summary>
        void SetPartitionsAssignedHandler(Action<IConsumer, List<TopicPartition>> value);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetPartitionsRevokedHandler(Action{IConsumer{TKey,TValue},List{TopicPartition}})" />.
        /// </summary>
        void SetPartitionsRevokedHandler(Action<IConsumer, List<TopicPartition>> value);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetOffsetsCommittedHandler(Action{IConsumer{TKey,TValue},CommittedOffsets})" />.
        /// </summary>
        void SetOffsetsCommittedHandler(Action<IConsumer, CommittedOffsets> value);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer.Consume(CancellationToken)" />
        /// </summary>
        ConsumeResult Consume(CancellationToken cancellationToken = default(CancellationToken));

        
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer.Consume(TimeSpan)" />
        /// </summary>
        ConsumeResult Consume(TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer.Commit(ConsumeResult, CancellationToken)" />
        /// </summary>
        void Commit(ConsumeResult result, CancellationToken cancellationToken = default(CancellationToken));
    }


    /// <summary>
    ///     Defines a high-level Apache Kafka consumer (with key and 
    ///     value deserialization).
    /// </summary>
    public interface IConsumer<TKey, TValue> : IConsumerBase, IClient
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetPartitionsAssignedHandler(Action{IConsumer{TKey,TValue},List{TopicPartition}})" />.
        /// </summary>
        void SetPartitionsAssignedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartition>> value);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetPartitionsRevokedHandler(Action{IConsumer{TKey,TValue},List{TopicPartition}})" />.
        /// </summary>
        void SetPartitionsRevokedHandler(Action<IConsumer<TKey, TValue>, List<TopicPartition>> value);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.SetOffsetsCommittedHandler(Action{IConsumer{TKey,TValue},CommittedOffsets})" />.
        /// </summary>
        void SetOffsetsCommittedHandler(Action<IConsumer<TKey, TValue>, CommittedOffsets> value);

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        /// </summary>
        ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(TimeSpan)" />
        /// </summary>
        ConsumeResult<TKey, TValue> Consume(TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Commit(ConsumeResult{TKey, TValue}, CancellationToken)" />
        /// </summary>
        void Commit(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default(CancellationToken));
    }
}
