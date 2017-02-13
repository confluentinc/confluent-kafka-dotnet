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
    ///     Encapsulates a Topic / Partition / Offset tuple.
    /// </summary>
    public class TopicPartitionOffset
    {
        public TopicPartitionOffset(TopicPartition tp, Offset offset)
            : this(tp.Topic, tp.Partition, offset) {}

        public TopicPartitionOffset(string topic, int partition, Offset offset)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
        }

        public string Topic { get; }
        public int Partition { get; }
        public Offset Offset { get; }

        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);

        public override bool Equals(object obj)
        {
            if (!(obj is TopicPartitionOffset))
            {
                return false;
            }

            var tp = (TopicPartitionOffset)obj;
            return tp.Partition == Partition && tp.Topic == Topic && tp.Offset == Offset;
        }

        // x by prime number is quick and gives decent distribution.
        public override int GetHashCode()
            => (Partition.GetHashCode()*251 + Topic.GetHashCode())*251 + Offset.GetHashCode();

        public static bool operator ==(TopicPartitionOffset a, TopicPartitionOffset b)
            => a.Equals(b);

        public static bool operator !=(TopicPartitionOffset a, TopicPartitionOffset b)
            => !(a == b);

        public override string ToString()
            => $"{Topic} [{Partition}] @{Offset}";
    }
}
