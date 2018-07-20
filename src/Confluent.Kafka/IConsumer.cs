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
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka consumer (with key and 
    ///     value deserialization).
    /// </summary>
    public interface IConsumer<TKey, TValue> : IClient
    {
        /// <summary>
        ///     Gets the (dynamic) group member id of this consumer (as set by the broker).
        /// </summary>
        string MemberId { get; }

        ConsumeResult<TKey, TValue> Consume(TimeSpan timeout);
        ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken);

        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(TimeSpan timeout);
        Task<ConsumeResult<TKey, TValue>> ConsumeAsync(CancellationToken cancellationToken);

        event EventHandler<List<TopicPartition>> OnPartitionsAssigned;

        event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        event EventHandler<CommittedOffsets> OnOffsetsCommitted;

        List<TopicPartition> Assignment { get; }

        List<string> Subscription { get; }

        void Subscribe(IEnumerable<string> topics);

        void Subscribe(string topic);
        
        void Unsubscribe();

        void Assign(IEnumerable<TopicPartitionOffset> partitions);

        void Assign(IEnumerable<TopicPartition> partitions);

        void Unassign();

        TopicPartitionOffsetError StoreOffset(ConsumeResult<TKey, TValue> record);

        List<TopicPartitionOffsetError> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets);

        Task<List<TopicPartitionOffsetError>> CommitAsync(CancellationToken cancellationToken = default(CancellationToken));

        Task<List<TopicPartitionOffsetError>> CommitAsync(ConsumeResult<TKey, TValue> record, CancellationToken cancellationToken = default(CancellationToken));

        Task<List<TopicPartitionOffsetError>> CommitAsync(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default(CancellationToken));

        void Seek(TopicPartitionOffset tpo);

        List<TopicPartitionError> Pause(IEnumerable<TopicPartition> partitions);

        List<TopicPartitionError> Resume(IEnumerable<TopicPartition> partitions);

        Task<List<TopicPartitionOffsetError>> CommittedAsync(IEnumerable<TopicPartition> partitions, TimeSpan timeout, CancellationToken cancellationToken = default(CancellationToken));

        Task<List<TopicPartitionOffsetError>> PositionAsync(IEnumerable<TopicPartition> partitions);

        Task<IEnumerable<TopicPartitionOffsetError>> OffsetsForTimesAsync(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout, CancellationToken cancellationToken = default(CancellationToken));
    }
}
