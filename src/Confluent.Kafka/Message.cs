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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Represents a message stored in Kafka.
    /// </summary>
    public class Message
    {
        /// <summary>
        ///     Instantiates a new Message class instance.
        /// </summary>
        /// <param name="topic">
        ///     The Kafka topic name associated with this message.
        /// </param>
        /// <param name="partition">
        ///     The topic partition id associated with this message.
        /// </param>
        /// <param name="offset">
        ///     The offset of this message in the Kafka topic partition.
        /// </param>
        /// <param name="key">
        ///     The message key value (or null).
        /// </param>
        /// <param name="val">
        ///     The message value (or null).
        /// </param>
        /// <param name="timestamp">
        ///     The message timestamp.
        /// </param>
        /// <param name="headers">
        ///     A collection of Kafka message headers (or null).
        /// </param>
        /// <param name="error">
        ///     A rich <see cref="Error"/> associated with the message.
        /// </param>
        public Message(string topic, Partition partition, long offset, byte[] key, byte[] val, Timestamp timestamp, Headers headers, Error error)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Key = key;
            Value = val;
            Timestamp = timestamp;
            Headers = headers;
            Error = error;
        }

        /// <summary>
        ///     Gets the Kafka topic name associated with this message.
        /// </summary>
        public string Topic { get; }

        /// <summary>
        ///     Gets the topic partition associated with this message.
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        ///     Gets the offset of this message in the Kafka topic partition.
        /// </summary>
        public Offset Offset { get; }

        /// <summary>
        ///     Gets the message value.
        /// </summary>
        public byte[] Value { get; }

        /// <summary>
        ///     Gets the message key value.
        /// </summary>
        public byte[] Key { get; }

        /// <summary>
        ///     Gets the message timestamp.
        /// </summary>
        public Timestamp Timestamp { get; }

        /// <summary>
        ///     A collection of message headers.
        /// </summary>
        public Headers Headers { get; }
        
        /// <summary>
        ///     Gets a rich <see cref="Error"/> associated with the message.
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     Gets the topic/partition/offset associated with this message.
        /// </summary>
        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset);

        /// <summary>
        ///     Gets the topic/partition associated with this message.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);
    }
}
