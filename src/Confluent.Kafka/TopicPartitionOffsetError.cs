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
    ///     Represents a Kafka (topic, partition, offset, error) tuple.
    /// </summary>
    public class TopicPartitionOffsetError
    {
        /// <summary>
        ///     Initializes a new TopicPartitionOffsetError instance.
        /// </summary>
        /// <param name="tp">
        ///     Kafka topic name and partition values.
        /// </param>
        /// <param name="offset">
        ///     A Kafka offset value.
        /// </param>
        /// <param name="error">
        ///     A Kafka error.
        /// </param>
        /// <param name="leaderEpoch">
        ///     The offset leader epoch (optional).
        /// </param>
        public TopicPartitionOffsetError(TopicPartition tp, Offset offset,
                                         Error error,
                                         int? leaderEpoch = null)
            : this(tp.Topic, tp.Partition, offset, error, leaderEpoch) {}

        /// <summary>
        ///     Initializes a new TopicPartitionOffsetError instance.
        /// </summary>
        /// <param name="tpo">
        ///     Kafka topic name, partition and offset values.
        /// </param>
        /// <param name="error">
        ///     A Kafka error.
        /// </param>
        public TopicPartitionOffsetError(TopicPartitionOffset tpo, Error error)
            : this(tpo.Topic, tpo.Partition, tpo.Offset, error,
                   tpo.LeaderEpoch) {}

        /// <summary>
        ///     Initializes a new TopicPartitionOffsetError instance.
        /// </summary>
        /// <param name="topic">
        ///     A Kafka topic name.
        /// </param>
        /// <param name="partition">
        ///     A Kafka partition value.
        /// </param>
        /// <param name="offset">
        ///     A Kafka offset value.
        /// </param>
        /// <param name="error">
        ///     A Kafka error.
        /// </param>
        /// <param name="leaderEpoch">
        ///     The offset leader epoch (optional).
        /// </param>
        public TopicPartitionOffsetError(string topic, Partition partition, Offset offset,
                                         Error error, int? leaderEpoch = null)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Error = error;
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
        ///     Gets the Kafka error.
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     Gets the TopicPartition component of this TopicPartitionOffsetError instance.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     Gets the TopicPartitionOffset component of this TopicPartitionOffsetError instance.
        /// </summary>>
        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset, LeaderEpoch);

        /// <summary>
        ///     Tests whether this TopicPartitionOffsetError instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a TopicPartitionOffsetError and all properties are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionOffsetError))
            {
                return false;
            }

            var tp = (TopicPartitionOffsetError)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Offset == Offset && tp.Error == Error;
        }

        /// <summary>
        ///     Returns a hash code for this TopicPartitionOffsetError.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this TopicPartitionOffsetError.
        /// </returns>
        public override int GetHashCode()
            // x by prime number is quick and gives decent distribution.
            => ((Partition.GetHashCode()*251 + Topic.GetHashCode())*251 + Offset.GetHashCode())*251 + Error.GetHashCode();

        /// <summary>
        ///     Tests whether TopicPartitionOffsetError instance a is equal to TopicPartitionOffsetError instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionOffsetError instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionOffsetError instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionOffsetError instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(TopicPartitionOffsetError a, TopicPartitionOffsetError b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether TopicPartitionOffsetError instance a is not equal to TopicPartitionOffsetError instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionOffsetError instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionOffsetError instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionOffsetError instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(TopicPartitionOffsetError a, TopicPartitionOffsetError b)
            => !(a == b);

        /// <summary>
        ///     Converts TopicPartitionOffsetError instance to TopicPartitionOffset instance.
        ///     NOTE: Throws KafkaException if Error.Code != ErrorCode.NoError 
        /// </summary>
        /// <param name="tpoe">
        ///     The TopicPartitionOffsetError instance to convert.
        /// </param>
        /// <returns>
        ///     TopicPartitionOffset instance converted from TopicPartitionOffsetError instance
        /// </returns>
        public static explicit operator TopicPartitionOffset(TopicPartitionOffsetError tpoe)
        {
            if (tpoe.Error.IsError)
            {
                throw new KafkaException(tpoe.Error);
            }
            
            return tpoe.TopicPartitionOffset;
        }

        /// <summary>
        ///     Returns a string representation of the TopicPartitionOffsetError object.
        /// </summary>
        /// <returns>
        ///     A string representation of the TopicPartitionOffsetError object.
        /// </returns>
        public override string ToString()
            => $"{Topic} [{Partition}] @{Offset}: {Error}";
    }
}
