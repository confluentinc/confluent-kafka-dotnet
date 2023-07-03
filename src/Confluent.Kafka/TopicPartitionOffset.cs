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
    ///     Represents a Kafka (topic, partition, offset) tuple.
    /// </summary>
    public class TopicPartitionOffset
    {
        /// <summary>
        ///     Initializes a new TopicPartitionOffset instance.
        /// </summary>
        /// <param name="tp">
        ///     Kafka topic name and partition.
        /// </param>
        /// <param name="offset">
        ///     A Kafka offset value.
        /// </param>
        public TopicPartitionOffset(TopicPartition tp, Offset offset)
            : this(tp.Topic, tp.Partition, offset, null) { }
        
        /// <summary>
        ///     Initializes a new TopicPartitionOffset instance.
        /// </summary>
        /// <param name="tp">
        ///     Kafka topic name and partition.
        /// </param>
        /// <param name="offset">
        ///     A Kafka offset value.
        /// </param>
        /// <param name="leaderEpoch">
        ///     The offset leader epoch (optional).
        /// </param>
        public TopicPartitionOffset(TopicPartition tp, Offset offset,
                                    int? leaderEpoch)
            : this(tp.Topic, tp.Partition, offset, leaderEpoch) { }

        /// <summary>
        ///     Initializes a new TopicPartitionOffset instance.
        /// </summary>
        /// <param name="topic">
        ///     A Kafka topic name.
        /// </param>
        /// <param name="partition">
        ///     A Kafka partition.
        /// </param>
        /// <param name="offset">
        ///     A Kafka offset value.
        /// </param>
        public TopicPartitionOffset(string topic, Partition partition,
                                    Offset offset)
            : this(topic, partition, offset, null) { }

        /// <summary>
        ///     Initializes a new TopicPartitionOffset instance.
        /// </summary>
        /// <param name="topic">
        ///     A Kafka topic name.
        /// </param>
        /// <param name="partition">
        ///     A Kafka partition.
        /// </param>
        /// <param name="offset">
        ///     A Kafka offset value.
        /// </param>
        /// <param name="leaderEpoch">
        ///     The optional offset leader epoch.
        /// </param>
        public TopicPartitionOffset(string topic, Partition partition,
                                    Offset offset, int? leaderEpoch)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            LeaderEpoch = leaderEpoch;
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
        ///     Gets the Kafka partition offset value.
        /// </summary>
        public Offset Offset { get; }
        
        /// <summary>
        ///     Gets the offset leader epoch (optional).
        /// </summary>
        public int? LeaderEpoch { get; }

        /// <summary>
        ///     Gets the TopicPartition component of this TopicPartitionOffset instance.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     Tests whether this TopicPartitionOffset instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a TopicPartitionOffset and all properties are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionOffset))
            {
                return false;
            }

            var tp = (TopicPartitionOffset)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Offset == Offset;
        }

        /// <summary>
        ///     Returns a hash code for this TopicPartitionOffset.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this TopicPartitionOffset.
        /// </returns>
        public override int GetHashCode()  
            // x by prime number is quick and gives decent distribution.
            => (Partition.GetHashCode()*251 + Topic.GetHashCode())*251 + Offset.GetHashCode();

        /// <summary>
        ///     Tests whether TopicPartitionOffset instance a is equal to TopicPartitionOffset instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionOffset instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionOffset instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionOffset instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(TopicPartitionOffset a, TopicPartitionOffset b)
        {
            if (a is null)
            {
                return (b is null);
            }
            
            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether TopicPartitionOffset instance a is not equal to TopicPartitionOffset instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionOffset instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionOffset instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionOffset instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(TopicPartitionOffset a, TopicPartitionOffset b)
            => !(a == b);

        /// <summary>
        ///     Returns a string representation of the TopicPartitionOffset object.
        /// </summary>
        /// <returns>
        ///     A string that represents the TopicPartitionOffset object.
        /// </returns>
        public override string ToString()
            => $"{Topic} [{Partition}] @{Offset}";
    }
}
