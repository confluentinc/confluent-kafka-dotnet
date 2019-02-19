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
    ///     Defines a high-level Apache Kafka consumer (with key and 
    ///     value deserialization).
    /// </summary>
    public interface IConsumer<TKey, TValue> : IClient
    {
        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(CancellationToken)" />
        /// </summary>
        ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default(CancellationToken));


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey, TValue}.Consume(TimeSpan)" />
        /// </summary>
        ConsumeResult<TKey, TValue> Consume(TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.MemberId" />
        /// </summary>
        string MemberId { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Assignment" />
        /// </summary>
        List<TopicPartition> Assignment { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Subscription" />
        /// </summary>
        List<string> Subscription { get; }


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Subscribe(IEnumerable{string})" />
        /// </summary>
        void Subscribe(IEnumerable<string> topics);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Subscribe(string)" />
        /// </summary>
        /// <param name="topic"></param>
        void Subscribe(string topic);
        

        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Unsubscribe" />
        /// </summary>
        void Unsubscribe();


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Assign(TopicPartition)" />
        /// </summary>
        void Assign(TopicPartition partition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Assign(TopicPartitionOffset)" />
        /// </summary>
        void Assign(TopicPartitionOffset partition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Assign(IEnumerable{TopicPartitionOffset})" />
        /// </summary>
        void Assign(IEnumerable<TopicPartitionOffset> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Assign(TopicPartition)" />
        /// </summary>
        void Assign(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Unassign" />
        /// </summary>
        void Unassign();


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.StoreOffset(ConsumeResult{TKey, TValue})" />
        /// </summary>
        void StoreOffset(ConsumeResult<TKey, TValue> result);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.StoreOffset(TopicPartitionOffset)" />
        /// </summary>
        void StoreOffset(TopicPartitionOffset offset);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Commit()" />
        /// </summary>
        List<TopicPartitionOffset> Commit();


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Commit(IEnumerable{TopicPartitionOffset})" />
        /// </summary>
        void Commit(IEnumerable<TopicPartitionOffset> offsets);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Commit(ConsumeResult{TKey, TValue})" />
        /// </summary>
        void Commit(ConsumeResult<TKey, TValue> result);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Seek(TopicPartitionOffset)" />
        /// </summary>
        void Seek(TopicPartitionOffset tpo);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Pause(IEnumerable{TopicPartition})" />
        /// </summary>
        void Pause(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Resume(IEnumerable{TopicPartition})" />
        /// </summary>
        void Resume(IEnumerable<TopicPartition> partitions);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Committed(IEnumerable{TopicPartition}, TimeSpan)" />
        /// </summary>
        List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Position(TopicPartition)" />
        /// </summary>
        Offset Position(TopicPartition partition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.OffsetsForTimes(IEnumerable{TopicPartitionTimestamp}, TimeSpan)" />
        /// </summary>
        List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.GetWatermarkOffsets(TopicPartition)" />
        /// </summary>
        WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.QueryWatermarkOffsets(TopicPartition, TimeSpan)" />
        /// </summary>
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);


        /// <summary>
        ///     Refer to <see cref="Confluent.Kafka.Consumer{TKey,TValue}.Close" />.
        /// </summary>
        void Close();
    }
}
