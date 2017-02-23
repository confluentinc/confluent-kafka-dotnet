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
        public TopicPartitionOffsetError(TopicPartition tp, Offset offset, Error error)
            : this(tp.Topic, tp.Partition, offset, error) {}

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
            : this(tpo.Topic, tpo.Partition, tpo.Offset, error) {}

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
        public TopicPartitionOffsetError(string topic, int partition, Offset offset, Error error)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Error = error;
        }

        /// <summary>
        ///     Gets the Kafka topic name.
        /// </summary>
        public string Topic { get; }

        /// <summary>
        ///     Gets the Kafka partition.
        /// </summary>
        public int Partition { get; }

        /// <summary>
        ///     Gets the Kafka partition offset value.
        /// </summary>
        public Offset Offset { get; }

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
            => new TopicPartitionOffset(Topic, Partition, Offset);

        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionOffsetError))
            {
                return false;
            }

            var tp = (TopicPartitionOffsetError)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Offset == Offset && tp.Error == Error;
        }

        public override int GetHashCode()
            => ((Partition.GetHashCode()*251 + Topic.GetHashCode())*251 + Offset.GetHashCode())*251 + Error.GetHashCode(); // x by prime number is quick and gives decent distribution.

        public static bool operator ==(TopicPartitionOffsetError a, TopicPartitionOffsetError b)
            => a.Equals(b);

        public static bool operator !=(TopicPartitionOffsetError a, TopicPartitionOffsetError b)
            => !(a == b);

        public override string ToString()
            => $"{Topic} [{Partition}] @{Offset}: {Error}";
    }
}
