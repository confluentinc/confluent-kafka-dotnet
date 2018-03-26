// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
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
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
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
    ///     Implements a high-level Apache Kafka consumer (with 
    ///     key and value deserialization).
    /// </summary>
    public class Consumer<TKey, TValue> : IConsumer<TKey, TValue>
    {
        private readonly Consumer consumer;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="KeyDeserializer"]/*' />
        public IDeserializer<TKey> KeyDeserializer { get; }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="ValueDeserializer"]/*' />
        public IDeserializer<TValue> ValueDeserializer { get; }

        /// <summary>
        ///     Creates a new Consumer instance.
        /// </summary>
        /// <param name="config">
        ///     librdkafka configuration parameters 
        ///     (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
        /// </param>
        /// <param name="keyDeserializer">
        ///     An IDeserializer implementation instance for deserializing keys.
        /// </param>
        /// <param name="valueDeserializer">
        ///     An IDeserializer implementation instance for deserializing values.
        /// </param>
        public Consumer(
            IEnumerable<KeyValuePair<string, object>> config,
            IDeserializer<TKey> keyDeserializer,
            IDeserializer<TValue> valueDeserializer)
        {
            KeyDeserializer = keyDeserializer;
            ValueDeserializer = valueDeserializer;

            if (keyDeserializer != null && keyDeserializer == valueDeserializer)
            {
                throw new ArgumentException("Key and value deserializers must not be the same object.");
            }

            if (KeyDeserializer == null)
            {
                if (typeof(TKey) == typeof(Null))
                {
                    KeyDeserializer = (IDeserializer<TKey>)new NullDeserializer();
                }
                else if (typeof(TKey) == typeof(Ignore))
                {
                    KeyDeserializer = (IDeserializer<TKey>)new IgnoreDeserializer();
                }
                else
                {
                    throw new ArgumentNullException("Key deserializer must be specified.");
                }
            }

            if (ValueDeserializer == null)
            {
                if (typeof(TValue) == typeof(Null))
                {
                    ValueDeserializer = (IDeserializer<TValue>)new NullDeserializer();
                }
                else if (typeof(TValue) == typeof(Ignore))
                {
                    ValueDeserializer = (IDeserializer<TValue>)new IgnoreDeserializer();
                }
                else
                {
                    throw new ArgumentNullException("Value deserializer must be specified.");
                }
            }

            var configWithoutKeyDeserializerProperties = KeyDeserializer.Configure(config, true);
            var configWithoutValueDeserializerProperties = ValueDeserializer.Configure(config, false);

            var configWithoutDeserializerProperties = config.Where(item => 
                configWithoutKeyDeserializerProperties.Any(ci => ci.Key == item.Key) &&
                configWithoutValueDeserializerProperties.Any(ci => ci.Key == item.Key)
            );

            consumer = new Consumer(configWithoutDeserializerProperties);

            consumer.OnConsumeError += (sender, msg) 
                => OnConsumeError?.Invoke(this, msg);
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_Message"]/*' />
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_Message_int"]/*' />
        public bool Consume(out Message<TKey, TValue> message, int millisecondsTimeout)
        {
            Message msg;
            if (!consumer.Consume(out msg, millisecondsTimeout))
            {
                message = null;
                return false;
            }

            try
            {
                message = msg.Deserialize(KeyDeserializer, ValueDeserializer);
            }
            catch (KafkaException ex)
            {
                var erroredMsg = new Message(
                    msg.Topic,
                    msg.Partition,
                    msg.Offset,
                    msg.Key,
                    msg.Value,
                    msg.Timestamp,
                    null,
                    ex.Error
                );
                OnConsumeError?.Invoke(this, erroredMsg);
                message = null;
                return false;
            }

            return true;
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_Message"]/*' />
        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Consume_Message_TimeSpan"]/*' />
        public bool Consume(out Message<TKey, TValue> message, TimeSpan timeout)
            => Consume(out message, timeout.TotalMillisecondsAsInt());


        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Poll_int"]/*' />
        public void Poll(int millisecondsTimeout)
        {
            Message<TKey, TValue> msg;
            if (Consume(out msg, millisecondsTimeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Poll_TimeSpan"]/*' />
        public void Poll(TimeSpan timeout)
        {
            Message<TKey, TValue> msg;
            if (Consume(out msg, timeout))
            {
                OnMessage?.Invoke(this, msg);
            }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionsAssigned"]/*' />
        public event EventHandler<List<TopicPartition>> OnPartitionsAssigned
        {
            add { consumer.OnPartitionsAssigned += value; }
            remove { consumer.OnPartitionsAssigned -= value; }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionsRevoked"]/*' />
        public event EventHandler<List<TopicPartition>> OnPartitionsRevoked
        {
            add { consumer.OnPartitionsRevoked += value; }
            remove { consumer.OnPartitionsRevoked -= value; }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnOffsetsCommitted"]/*' />
        public event EventHandler<CommittedOffsets> OnOffsetsCommitted
        {
            add { consumer.OnOffsetsCommitted += value; }
            remove { consumer.OnOffsetsCommitted -= value; }
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnLog"]/*' />
        public event EventHandler<LogMessage> OnLog
        {
            add { consumer.OnLog += value; }
            remove { consumer.OnLog -= value; }
        }

        /// <include file='include_docs_client.xml' path='API/Member[@name="OnStatistics"]/*' />
        public event EventHandler<string> OnStatistics
        {
            add { consumer.OnStatistics += value; }
            remove { consumer.OnStatistics -= value; }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnConsumeError"]/*' />
        public event EventHandler<Message> OnConsumeError;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnError"]/*' />
        public event EventHandler<Error> OnError
        {
            add { consumer.OnError += value; }
            remove { consumer.OnError -= value; }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnPartitionEOF"]/*' />
        public event EventHandler<TopicPartitionOffset> OnPartitionEOF
        {
            add { consumer.OnPartitionEOF += value; }
            remove { consumer.OnPartitionEOF -= value; }
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OnMessage"]/*' />
        public event EventHandler<Message<TKey, TValue>> OnMessage;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assignment"]/*' />
        public List<TopicPartition> Assignment
            => consumer.Assignment;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscription"]/*' />
        public List<string> Subscription
            => consumer.Subscription;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscribe_IEnumerable"]/*' />
        public void Subscribe(IEnumerable<string> topics)
            => consumer.Subscribe(topics);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Subscribe_string"]/*' />
        public void Subscribe(string topic)
            => consumer.Subscribe(topic);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Unsubscribe"]/*' />
        public void Unsubscribe()
            => consumer.Unsubscribe();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_TopicPartition"]/*' />
        public void Assign(TopicPartition partition)
            => this.Assign(new List<TopicPartition> { partition });

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_TopicPartitionOffset"]/*' />
        public void Assign(TopicPartitionOffset partition)
            => this.Assign(new List<TopicPartitionOffset> { partition });

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_IEnumerable_TopicPartitionOffset"]/*' />
        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
            => consumer.Assign(partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Assign_IEnumerable_TopicPartition"]/*' />
        public void Assign(IEnumerable<TopicPartition> partitions)
            => consumer.Assign(partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Unassign"]/*' />
        public void Unassign()
            => consumer.Unassign();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="StoreOffset_Message"]/*' />
        public TopicPartitionOffsetError StoreOffset(Message<TKey, TValue> message)
            => consumer.StoreOffsets(new[] { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) })[0];

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="StoreOffsets"]/*' />
        public List<TopicPartitionOffsetError> StoreOffsets(IEnumerable<TopicPartitionOffset> offsets)
            => consumer.StoreOffsets(offsets);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit"]/*' />
        public Task<CommittedOffsets> CommitAsync()
            => consumer.CommitAsync();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_Message"]/*' />
        public Task<CommittedOffsets> CommitAsync(Message<TKey, TValue> message)
            => consumer.CommitAsync(new[] { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) });

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_IEnumerable"]/*' />
        public Task<CommittedOffsets> CommitAsync(IEnumerable<TopicPartitionOffset> offsets)
            => consumer.CommitAsync(offsets);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit"]/*' />
        public CommittedOffsets Commit()
            => consumer.Commit();

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_Message"]/*' />
        public CommittedOffsets Commit(Message<TKey, TValue> message)
            => consumer.Commit(new[] { new TopicPartitionOffset(message.TopicPartition, message.Offset + 1) });

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Commit_IEnumerable"]/*' />
        public CommittedOffsets Commit(IEnumerable<TopicPartitionOffset> offsets)
            => consumer.Commit(offsets);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Dispose"]/*' />
        public void Dispose()
        {
            if (KeyDeserializer != null)
            {
                KeyDeserializer.Dispose();
            }

            if (ValueDeserializer != null)
            {
                ValueDeserializer.Dispose();
            }

            consumer.Dispose();
        }

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Seek"]/*' />
        public void Seek(TopicPartitionOffset tpo)
            => consumer.Seek(tpo);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Pause"]/*' />
        public List<TopicPartitionError> Pause(IEnumerable<TopicPartition> partitions)
            => consumer.Pause(partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Resume"]/*' />
        public List<TopicPartitionError> Resume(IEnumerable<TopicPartition> partitions)
            => consumer.Resume(partitions);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Committed_IEnumerable_TimeSpan"]/*' />
        public List<TopicPartitionOffsetError> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => consumer.Committed(partitions, timeout);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="Position_IEnumerable"]/*' />
        public List<TopicPartitionOffsetError> Position(IEnumerable<TopicPartition> partitions)
            => consumer.Position(partitions);

        /// <include file='include_docs_client.xml' path='API/Member[@name="Client_Name"]/*' />
        public string Name
            => consumer.Name;

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="MemberId"]/*' />
        public string MemberId
            => consumer.MemberId;

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroups_TimeSpan"]/*' />
        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => consumer.ListGroups(timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string_TimeSpan"]/*' />
        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => consumer.ListGroup(group, timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string"]/*' />
        public GroupInfo ListGroup(string group)
            => consumer.ListGroup(group);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="GetWatermarkOffsets_TopicPartition"]/*' />
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => consumer.GetWatermarkOffsets(topicPartition);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition_TimeSpan"]/*' />
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => consumer.QueryWatermarkOffsets(topicPartition, timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition"]/*' />
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => consumer.QueryWatermarkOffsets(topicPartition);

        /// <include file='include_docs_consumer.xml' path='API/Member[@name="OffsetsForTimes"]/*' />
        public IEnumerable<TopicPartitionOffsetError> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
            => consumer.OffsetsForTimes(timestampsToSearch, timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_TimeSpan"]/*' />
        public Metadata GetMetadata(bool allTopics, TimeSpan timeout)
            => consumer.GetMetadata(allTopics, timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool"]/*' />
        public Metadata GetMetadata(bool allTopics)
            => consumer.GetMetadata(allTopics);

        /// <include file='include_docs_client.xml' path='API/Member[@name="AddBrokers_string"]/*' />
        public int AddBrokers(string brokers)
            => consumer.AddBrokers(brokers);
    }
}
