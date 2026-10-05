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
    ///     Represents a Kafka (topic, partition, error) tuple.
    /// </summary>
    public class TopicPartitionError
    {
        /// <summary>
        ///     Initializes a new TopicPartitionError instance.
        /// </summary>
        /// <param name="tp">
        ///     Kafka topic name and partition values.
        /// </param>
        /// <param name="error">
        ///     A Kafka error.
        /// </param>
        public TopicPartitionError(TopicPartition tp, Error error)
            : this(tp.Topic, tp.Partition, error) {}


        /// <summary>
        ///     Initializes a new TopicPartitionError instance.
        /// </summary>
        /// <param name="topic">
        ///     A Kafka topic name.
        /// </param>
        /// <param name="partition">
        ///     A Kafka partition value.
        /// </param>
        /// <param name="error">
        ///     A Kafka error.
        /// </param>
        public TopicPartitionError(string topic, Partition partition, Error error)
        {
            Topic = topic;
            Partition = partition;
            Error = error;
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
        ///     Gets the Kafka error.
        /// </summary>
        public Error Error { get; }

        /// <summary>
        ///     Gets the TopicPartition component of this TopicPartitionError instance.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        /// <summary>
        ///     Tests whether this TopicPartitionError instance is equal to the specified object.
        /// </summary>
        /// <param name="obj">
        ///     The object to test.
        /// </param>
        /// <returns>
        ///     true if obj is a TopicPartitionError and all properties are equal. false otherwise.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionError))
            {
                return false;
            }

            var tp = (TopicPartitionError)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Error == Error;
        }

        /// <summary>
        ///     Returns a hash code for this TopicPartitionError.
        /// </summary>
        /// <returns>
        ///     An integer that specifies a hash value for this TopicPartitionError.
        /// </returns>
        public override int GetHashCode()
            // x by prime number is quick and gives decent distribution.
            => (Partition.GetHashCode()*251 + Topic.GetHashCode())*251 + Error.GetHashCode();

        /// <summary>
        ///     Tests whether TopicPartitionError instance a is equal to TopicPartitionError instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionError instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionError instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionError instances a and b are equal. false otherwise.
        /// </returns>
        public static bool operator ==(TopicPartitionError a, TopicPartitionError b)
        {
            if (a is null)
            {
                return (b is null);
            }

            return a.Equals(b);
        }

        /// <summary>
        ///     Tests whether TopicPartitionError instance a is not equal to TopicPartitionError instance b.
        /// </summary>
        /// <param name="a">
        ///     The first TopicPartitionError instance to compare.
        /// </param>
        /// <param name="b">
        ///     The second TopicPartitionError instance to compare.
        /// </param>
        /// <returns>
        ///     true if TopicPartitionError instances a and b are not equal. false otherwise.
        /// </returns>
        public static bool operator !=(TopicPartitionError a, TopicPartitionError b)
            => !(a == b);

        /// <summary>
        ///     Returns a string representation of the TopicPartitionError object.
        /// </summary>
        /// <returns>
        ///     A string representation of the TopicPartitionError object.
        /// </returns>
        public override string ToString()
            => $"{Topic} [{Partition}]: {Error}";
    }
}
