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
    ///     Represents a message consumed from kafka cluster.
    /// </summary>
    public class ConsumerRecord
    {
        /// <summary>
        ///     The topic associated with the consumed message.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        ///     The partition associated with the consumed message.
        /// </summary>
        public Partition Partition { get; set; }

        /// <summary>
        ///     The offset associated with the consumed message.
        /// </summary>
        public Offset Offset { get; set; }

        /// <summary>
        ///     The error (or NoError) associated with the consumed message.
        /// </summary>
        public Error Error { get; set; }

        /// <summary>
        ///     The consumed Message.
        /// </summary>
        public Message Message { get; set; }

        /// <summary>
        ///     The TopicPartition associated with the message.
        /// </summary>
        public TopicPartition TopicPartition
            => new TopicPartition(Topic, Partition);
        
        /// <summary>
        ///     The TopicPartitionOffset associated with the consumed message.
        /// </summary>
        public TopicPartitionOffset TopicPartitionOffset
            => new TopicPartitionOffset(Topic, Partition, Offset);

        /// <summary>
        ///     The TopicPartitionOffsetError asociated with the consumed message.
        /// </summary>
        public TopicPartitionOffsetError TopicPartitionOffsetError
        {
            get
            {
                return new TopicPartitionOffsetError(Topic, Partition, Offset, Error);
            }
            set
            {
                Topic = value.Topic;
                Partition = value.Partition;
                Offset = value.Offset;
                Error = value.Error;
            }
        }
    }
}
