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
using System.Threading.Tasks;
using Confluent.Kafka.Impl;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines a high-level Apache Kafka consumer (without deserialization).
    /// </summary>
    internal interface IConsumer : IClient
    {
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="MemberId"]/*' />
        string MemberId { get; }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionsAssigned"]/*' />
        event EventHandler<List<TopicPartition>> OnPartitionsAssigned;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionsRevoked"]/*' />
        event EventHandler<List<TopicPartition>> OnPartitionsRevoked;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnOffsetsCommitted"]/*' />
        event EventHandler<CommittedOffsets> OnOffsetsCommitted;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnConsumeError"]/*' />
        event EventHandler<ConsumerRecord> OnConsumeError;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnMessage"]/*' />
        event EventHandler<ConsumerRecord> OnRecord;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionEOF"]/*' />
        event EventHandler<TopicPartitionOffset> OnPartitionEOF;


        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assignment"]/*' />
        List<TopicPartition> Assignment { get; }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscription"]/*' />
        List<string> Subscription { get; }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscribe_IEnumerable"]/*' />
        void Subscribe(IEnumerable<string> topics);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscribe_string"]/*' />
        void Subscribe(string topic);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Unsubscribe"]/*' />
        void Unsubscribe();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_TopicPartition"]/*' />
        void Assign(TopicPartition partition);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_TopicPartitionOffset"]/*' />
        void Assign(TopicPartitionOffset partition);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_IEnumerable_TopicPartitionOffset"]/*' />
        void Assign(IEnumerable<TopicPartitionOffset> partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_IEnumerable_TopicPartition"]/*' />
        void Assign(IEnumerable<TopicPartition> partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Unassign"]/*' />
        void Unassign();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord"]/*' />
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord_int"]/*' />
        bool Consume(out ConsumerRecord record, int millisecondsTimeout);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord"]/*' />
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_ConsumerRecord_TimeSpan"]/*' />
        bool Consume(out ConsumerRecord record, TimeSpan timeout);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Poll_TimeSpan"]/*' />
        void Poll(TimeSpan timeout);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Poll_int"]/*' />
        void Poll(int millisecondsTimeout);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="StoreOffset_ConsumerRecord"]/*' />
        TopicPartitionOffsetError StoreOffset(ConsumerRecord record);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="StoreOffsets"]/*' />
        List<TopicPartitionOffsetError> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit"]/*' />
        CommittedOffsets Commit();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_ConsumerRecord"]/*' />
        CommittedOffsets Commit(ConsumerRecord record);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_IEnumerable"]/*' />
        CommittedOffsets Commit(IEnumerable<TopicPartitionOffset> offsets);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Seek"]/*' />
        void Seek(TopicPartitionOffset tpo);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Pause"]/*' />
        List<TopicPartitionError> Pause(IEnumerable<TopicPartition> partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Resume"]/*' />
        List<TopicPartitionError> Resume(IEnumerable<TopicPartition> partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Committed_IEnumerable_TimeSpan"]/*' />
        List<TopicPartitionOffsetError> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Position_IEnumerable"]/*' />
        List<TopicPartitionOffsetError> Position(IEnumerable<TopicPartition> partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OffsetsForTimes"]/*' />
        IEnumerable<TopicPartitionOffsetError> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout);
    }
}
