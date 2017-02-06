using System;
using System.Collections.Generic;
using Confluent.Kafka.Impl;


namespace Confluent.Kafka
{
    public class Client
    {
        private SafeKafkaHandle kafkaHandle;

        internal Client(SafeKafkaHandle kafkaHandle)
        {
            this.kafkaHandle = kafkaHandle;
        }

        public List<GroupInfo> ListGroups(TimeSpan timeout)
            => kafkaHandle.ListGroups(timeout.TotalMillisecondsAsInt());

        // TODO: is a version of this with infinite timeout really required? (same question elsewhere)
        public List<GroupInfo> ListGroups()
            => kafkaHandle.ListGroups(-1);


        public GroupInfo ListGroup(string group, TimeSpan timeout)
            => kafkaHandle.ListGroup(group, timeout.TotalMillisecondsAsInt());

        public GroupInfo ListGroup(string group)
            => kafkaHandle.ListGroup(group, -1);


        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.GetWatermarkOffsets(topicPartition.Topic, topicPartition.Partition);


        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, timeout.TotalMillisecondsAsInt());

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition)
            => kafkaHandle.QueryWatermarkOffsets(topicPartition.Topic, topicPartition.Partition, -1);


        /// <summary>
        ///
        /// </summary>
        /// <param name="allTopics">
        ///     true - request all topics from cluster
        ///     false - request only locally known topics (topic_new():ed topics or otherwise locally referenced once, such as consumed topics)
        /// </param>
        /// <remarks>
        ///     TODO: Topic handles are not exposed to users of the library (they are internal to producer).
        ///           Is it possible to get a topic handle given a topic name?
        ///           If so, include topic parameter in this method.
        /// </remaarks>
        public Metadata GetMetadata(bool allTopics, TimeSpan timeout)
            => kafkaHandle.GetMetadata(allTopics, null, timeout.TotalMillisecondsAsInt());

        public Metadata GetMetadata(bool allTopics)
            => kafkaHandle.GetMetadata(allTopics, null, -1);

    }
}
