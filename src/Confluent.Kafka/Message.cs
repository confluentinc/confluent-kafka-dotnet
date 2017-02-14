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
    public class Message<TKey, TValue>
    {
        public Message(string topic, int partition, long offset, TKey key, TValue val, Timestamp timestamp, Error error)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Key = key;
            Value = val;
            Timestamp = timestamp;
            Error = error;
        }

        public string Topic { get; }
        public int Partition { get; }
        public Offset Offset { get; }
        public TKey Key { get; }
        public TValue Value { get; }
        public Timestamp Timestamp { get; }
        public Error Error { get; }

        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset);

        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);
    }


    public class Message
    {
        public Message(string topic, int partition, long offset, byte[] key, byte[] val, Timestamp timestamp, Error error)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Key = key;
            Value = val;
            Timestamp = timestamp;
            Error = error;
        }

        public string Topic { get; }
        public int Partition { get; }
        public Offset Offset { get; }
        public byte[] Value { get; }
        public byte[] Key { get; }
        public Timestamp Timestamp { get; }
        public Error Error { get; }

        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset);

        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);
    }

}
