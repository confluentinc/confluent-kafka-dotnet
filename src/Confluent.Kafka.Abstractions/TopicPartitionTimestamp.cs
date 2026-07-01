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
    ///     Represents a Kafka (topic, partition, timestamp) tuple.
    /// </summary>
    public class TopicPartitionTimestamp
    {
        /// <summary>
        ///     Initializes a new TopicPartitionTimestamp instance.
        /// </summary>
        /// <param name="tp">
        ///     Kafka topic name and partition.
        /// </param>
        /// <param name="timestamp">
        ///     A Kafka timestamp value.
        /// </param>
        public TopicPartitionTimestamp(TopicPartition tp, Timestamp timestamp)
            : this (tp.Topic, tp.Partition, timestamp)
        {
        }

        /// <summary>
        ///     Initializes a new TopicPartitionTimestamp instance.
        /// </summary>
        /// <param name="topic">
        ///     A Kafka topic name.
        /// </param>
        /// <param name="partition">
        ///     A Kafka partition.
        /// </param>
        /// <param name="timestamp">
        ///     A Kafka timestamp value.
        /// </param>
        public TopicPartitionTimestamp(string topic, Partition partition, Timestamp timestamp)
        {
            Topic = topic;
            Partition = partition;
            Timestamp = timestamp;
        }

        /// <summary>
        ///     Gets the Kafka topic name.
        /// </summary>
        public string Topic { get; }

        /// <summary>
        ///     Gets the Kafka partition.
        /// </summary>
        public Partition Partition { get; }

        /// <summary>
        ///     Gets the Kafka timestamp.
        /// </summary>
        public Timestamp Timestamp { get; }

        /// <summary>
        ///     Gets the TopicPartition component of this TopicPartitionTimestamp instance.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     Tests whether this TopicPartitionTimestamp instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a TopicPartitionTimestamp and all properties are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionTimestamp))
            {
                return false;
            }

            var tp = (TopicPartitionTimestamp)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Timestamp == Timestamp;
        }

        /// <summary>
        ///     Returns a hash code for this TopicPartitionTimestamp.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this TopicPartitionTimestamp.
        /// </returns>
        public override int GetHashCode()
            // x by prime number is quick and gives decent distribution.
            => (Partition.GetHashCode() * 251 + Topic.GetHashCode()) * 251 + Timestamp.GetHashCode();

        /// <summary>
        ///     Tests whether TopicPartitionTimestamp instance a is equal to TopicPartitionTimestamp instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionTimestamp instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionTimestamp instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionTimestamp instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(TopicPartitionTimestamp a, TopicPartitionTimestamp b)
            => a.Equals(b);

        /// <summary>
        ///     Tests whether TopicPartitionTimestamp instance a is not equal to TopicPartitionTimestamp instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionTimestamp instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionTimestamp instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionTimestamp instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(TopicPartitionTimestamp a, TopicPartitionTimestamp b)
            => !(a == b);

        /// <summary>
        ///     Returns a string representation of the TopicPartitionTimestamp object.
        /// </summary>
        /// <returns>
        ///     A string that represents the TopicPartitionTimestamp object.
        /// </returns>
        public override string ToString()
            => $"{Topic} [{Partition}] @{Timestamp}";
    }
}
