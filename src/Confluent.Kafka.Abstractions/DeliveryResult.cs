// Copyright 2016-2018 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Encapsulates the result of a successful produce request.
    /// </summary>
    public class DeliveryResult<TKey, TValue>
    {
        /// <summary>
        ///     The topic associated with the message.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        ///     The partition associated with the message.
        /// </summary>
        public Partition Partition { get; set; }

        /// <summary>
        ///     The partition offset associated with the message.
        /// </summary>
        public Offset Offset { get; set; }

        /// <summary>
        ///     The TopicPartition associated with the message.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     The TopicPartitionOffset associated with the message.
        /// </summary>
        public TopicPartitionOffset TopicPartitionOffset
        {
            get
            {
                return new TopicPartitionOffset(Topic, Partition, Offset);
            }
            set
            {
                Topic = value.Topic;
                Partition = value.Partition;
                Offset = value.Offset;
            }
        }

        /// <summary>
        ///     The persistence status of the message
        /// </summary>
        public PersistenceStatus Status { get; set; }
        
        /// <summary>
        ///     The Kafka message.
        /// </summary>
        public Message<TKey, TValue> Message { get; set; }

        /// <summary>
        ///     The Kafka message Key.
        /// </summary>
        public TKey Key
        {
            get { return Message.Key; }
            set { Message.Key = value; }
        }

        /// <summary>
        ///     The Kafka message Value.
        /// </summary>
        public TValue Value
        {
            get { return Message.Value; }
            set { Message.Value = value; }
        }

        /// <summary>
        ///     The Kafka message timestamp.
        /// </summary>
        public Timestamp Timestamp
        {
            get { return Message.Timestamp; }
            set { Message.Timestamp = value; }
        }

        /// <summary>
        ///     The Kafka message headers.
        /// </summary>
        public Headers Headers
        {
            get { return Message.Headers; }
            set { Message.Headers = value; }
        }
    }
}
