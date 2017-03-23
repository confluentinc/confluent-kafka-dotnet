// Copyright 2016-2017 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Detailed messaged
    /// </summary>
    public struct ProduceRecord<TKey, TValue>
    {
        /// <summary>
        ///     Initialize a message which can be personalized via properties.
        /// </summary>
        /// <param name="topic">
        ///     The target topic.
        /// </param>
        /// <param name="key">
        ///     The message key (possibly null if allowed by the key serializer).
        /// </param>
        /// <param name="value">
        ///     The message value (possibly null if allowed by the value serializer).
        /// </param>
        public ProduceRecord(string topic, TKey key, TValue value)
        {
            Topic = topic;
            Key = key;
            Value = value;
            Partition = Producer.RD_KAFKA_PARTITION_UA;
            Timestamp = 0;
            BlockIfQueueFull = true;
        }

        /// <summary>
        ///     The target topic.
        /// </summary>
        public string Topic { get; }
        /// <summary>
        ///     The message key (possibly null if allowed by the key serializer).
        /// </summary>
        public TKey Key { get; }
        /// <summary>
        ///     The message value (possibly null if allowed by the value serializer).
        /// </summary>
        public TValue Value { get; }
        /// <summary>
        ///     The target partition (if -1, this is determined by the partitioner
        ///     configured for the topic).
        /// </summary>
        public int Partition { get; set; }
        /// <summary>
        ///     The Datetime associated to the message, binded to <see cref="Timestamp"/>
        ///     It will be converted to the closest unix millisecond timestamp (rounded toward zero)
        ///     (if Unix Epoch, will use the current date).
        /// </summary>
        public DateTime Datetime
        {
            get { return Kafka.Timestamp.UnixTimestampMsToDateTime(Timestamp); }
            set { Timestamp = Kafka.Timestamp.DateTimeToUnixTimestampMs(value); }
        }
        /// <summary>
        ///     Whether or not to block if the send queue is full.
        ///     If false, a KafkaExcepion (with Error.Code == ErrorCode.Local_QueueFull) 
        ///     will be thrown if an attempt is made to produce a message
        ///     and the send queue is full.
        ///      
        ///     Warning: if blockIfQueueFull is set to true, background polling is 
        ///     disabled and Poll is not being called in another thread, 
        ///     this will block indefinitely.
        /// </summary>
        public bool BlockIfQueueFull { get; set; }
        /// <summary>
        ///     The millisecond unix timestamp associated to the message
        ///     (if 0, the current date will be use).
        /// </summary>
        public long Timestamp { get; set; }
    }
}
