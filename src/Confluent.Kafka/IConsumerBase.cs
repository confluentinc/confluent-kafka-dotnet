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
    ///     Defines a high-level Apache Kafka consumer, excluding any methods
    ///     for consuming messages.
    /// </summary>
    public interface IConsumerBase : IClient
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.MemberId" />
        /// </summary>
        string MemberId { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Assignment" />
        /// </summary>
        List<TopicPartition> Assignment { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Subscription" />
        /// </summary>
        List<string> Subscription { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Subscribe(IEnumerable{string})" />
        /// </summary>
        void Subscribe(IEnumerable<string> topics);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Subscribe(string)" />
        /// </summary>
        /// <param name="topic"></param>
        void Subscribe(string topic);
        

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Unsubscribe" />
        /// </summary>
        void Unsubscribe();


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Assign(TopicPartition)" />
        /// </summary>
        /// <param name="partition"></param>
        void Assign(TopicPartition partition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Assign(TopicPartitionOffset)" />
        /// </summary>
        /// <param name="partition"></param>
        void Assign(TopicPartitionOffset partition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Assign(IEnumerable{TopicPartitionOffset})" />
        /// </summary>
        void Assign(IEnumerable<TopicPartitionOffset> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Assign(TopicPartition)" />
        /// </summary>
        void Assign(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Unassign" />
        /// </summary>
        void Unassign();


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.StoreOffset{TKey, TValue}(ConsumeResult{TKey, TValue})" />
        /// </summary>
        void StoreOffset<TKey, TValue>(ConsumeResult<TKey, TValue> result);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.StoreOffsets(IEnumerable{TopicPartitionOffset})" />
        /// </summary>
        void StoreOffsets(IEnumerable<TopicPartitionOffset> offsets);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Commit(CancellationToken)" />
        /// </summary>
        List<TopicPartitionOffset> Commit(CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Commit(IEnumerable{TopicPartitionOffset}, CancellationToken)" />
        /// </summary>
        void Commit(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Seek(TopicPartitionOffset)" />
        /// </summary>
        void Seek(TopicPartitionOffset tpo);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Pause(IEnumerable{TopicPartition})" />
        /// </summary>
        void Pause(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Resume(IEnumerable{TopicPartition})" />
        /// </summary>
        void Resume(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Committed(IEnumerable{TopicPartition}, TimeSpan, CancellationToken)" />
        /// </summary>
        List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout, CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Position(IEnumerable{TopicPartition})" />
        /// </summary>
        List<TopicPartitionOffset> Position(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.OffsetsForTimes(IEnumerable{TopicPartitionTimestamp}, TimeSpan, CancellationToken)" />
        /// </summary>
        List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.ConsumerBase.Close" />.
        /// </summary>
        void Close();
    }
}
