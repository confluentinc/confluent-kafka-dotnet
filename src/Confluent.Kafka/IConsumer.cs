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
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka consumer (with key and 
    ///     value deserialization).
    /// </summary>
    public interface IConsumer<TKey, TValue> : IClient
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.MemberId" />
        /// </summary>
        /// <value></value>
        string MemberId { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(TimeSpan)" />
        /// </summary>
        ConsumeResult<TKey, TValue> Consume(TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        /// </summary>
        ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.OnPartitionsAssigned" />
        /// </summary>
        event EventHandler<List<TopicPartition>> OnPartitionsAssigned;


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.OnPartitionsRevoked" />
        /// </summary>
        event EventHandler<List<TopicPartition>> OnPartitionsRevoked;


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.OnOffsetsCommitted" />
        /// </summary>
        event EventHandler<CommittedOffsets> OnOffsetsCommitted;


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assignment" />
        /// </summary>
        List<TopicPartition> Assignment { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Subscription" />
        /// </summary>
        List<string> Subscription { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Subscribe(IEnumerable{string})" />
        /// </summary>
        void Subscribe(IEnumerable<string> topics);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Subscribe(string)" />
        /// </summary>
        /// <param name="topic"></param>
        void Subscribe(string topic);
        

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Unsubscribe" />
        /// </summary>
        void Unsubscribe();


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(TopicPartition)" />
        /// </summary>
        /// <param name="partition"></param>
        void Assign(TopicPartition partition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(TopicPartitionOffset)" />
        /// </summary>
        /// <param name="partition"></param>
        void Assign(TopicPartitionOffset partition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(IEnumerable{TopicPartitionOffset})" />
        /// </summary>
        void Assign(IEnumerable<TopicPartitionOffset> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Assign(TopicPartition)" />
        /// </summary>
        void Assign(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Unassign" />
        /// </summary>
        void Unassign();


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.StoreOffset(ConsumeResult{TKey, TValue})" />
        /// </summary>
        void StoreOffset(ConsumeResult<TKey, TValue> result);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.StoreOffsets(IEnumerable{TopicPartitionOffset})" />
        /// </summary>
        void StoreOffsets(IEnumerable<TopicPartitionOffset> offsets);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Commit(CancellationToken)" />
        /// </summary>
        List<TopicPartitionOffset> Commit(CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Commit(ConsumeResult{TKey, TValue}, CancellationToken)" />
        /// </summary>
        TopicPartitionOffset Commit(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Commit(IEnumerable{TopicPartitionOffset}, CancellationToken)" />
        /// </summary>
        void Commit(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Seek(TopicPartitionOffset)" />
        /// </summary>
        void Seek(TopicPartitionOffset tpo);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Pause(IEnumerable{TopicPartition})" />
        /// </summary>
        void Pause(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Resume(IEnumerable{TopicPartition})" />
        /// </summary>
        void Resume(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Committed(IEnumerable{TopicPartition}, TimeSpan, CancellationToken)" />
        /// </summary>
        List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout, CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Position(IEnumerable{TopicPartition})" />
        /// </summary>
        List<TopicPartitionOffset> Position(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.OffsetsForTimes(IEnumerable{TopicPartitionTimestamp}, TimeSpan, CancellationToken)" />
        /// </summary>
        List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout, CancellationToken cancellationToken = default(CancellationToken));
    }
}
