// Copyright 2017-2018 Confluent Inc., 2015-2016 Andreas Heider
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a message consumed from a Kafka cluster.
    /// </summary>
    public class ConsumeResult<TKey, TValue>
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
        ///     The offset leader epoch (optional).
        /// </summary>
        public int? LeaderEpoch { get; set; }

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
                return new TopicPartitionOffset(Topic, Partition, Offset,
                    LeaderEpoch);
            }
            set
            {
                Topic = value.Topic;
                Partition = value.Partition;
                Offset = value.Offset;
                LeaderEpoch = value.LeaderEpoch;
            }
        }

        /// <summary>
        ///     The Kafka message, or null if this ConsumeResult
        ///     instance represents an end of partition event.
        /// </summary>
        public Message<TKey, TValue> Message { get; set; }

        /// <summary>
        ///     The Kafka message Key.
        /// </summary>
        [Obsolete("Please access the message Key via .Message.Key.")]
        public TKey Key
        {
            get
            {
                if (Message == null)
                {
                    throw new MessageNullException();
                }

                return Message.Key;
            }
        }

        /// <summary>
        ///     The Kafka message Value.
        /// </summary>
        [Obsolete("Please access the message Value via .Message.Value.")]
        public TValue Value
        {
            get
            {
                if (Message == null)
                {
                    throw new MessageNullException();
                }
                
                return Message.Value;
            }
        }

        /// <summary>
        ///     The Kafka message timestamp.
        /// </summary>
        [Obsolete("Please access the message Timestamp via .Message.Timestamp.")]
        public Timestamp Timestamp
        {
            get
            {
                if (Message == null)
                {
                    throw new MessageNullException();
                }

                return Message.Timestamp;
            }
        }

        /// <summary>
        ///     The Kafka message headers.
        /// </summary>
        [Obsolete("Please access the message Headers via .Message.Headers.")]
        public Headers Headers
        {
            get
            {
                if (Message == null)
                {
                    throw new MessageNullException();
                }
                
                return Message.Headers;
            }
        }

        /// <summary>
        ///     True if this instance represents an end of partition
        ///     event, false if it represents a message in kafka.
        /// </summary>
        public bool IsPartitionEOF { get; set; }
    }
}
