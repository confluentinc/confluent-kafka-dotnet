// Copyright 2018 Confluent Inc.
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
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;


namespace Confluent.Kafka
{
    /// <summary>
    ///     Defines an Apache Kafka admin client.
    /// </summary>
    public interface IAdminClient : IClient
    {
        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroups_TimeSpan"]/*' />
        List<GroupInfo> ListGroups(TimeSpan timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string_TimeSpan"]/*' />
        GroupInfo ListGroup(string group, TimeSpan timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="ListGroup_string"]/*' />
        GroupInfo ListGroup(string group);


        /// <include file='include_docs_consumer.xml' path='API/Member[@name="GetWatermarkOffsets_TopicPartition"]/*' />
        /// consumer only. is there a way to tell if a handle is consumer or producer?
        WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition_TimeSpan"]/*' />
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="QueryWatermarkOffsets_TopicPartition"]/*' />
        WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition);


        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_string_TimeSpan"]/*' />
        Metadata GetMetadata(bool allTopics, string topic, TimeSpan timeout);

        /// <include file='include_docs_client.xml' path='API/Member[@name="GetMetadata_bool_TimeSpan"]/*' />
        Metadata GetMetadata(bool allTopics, TimeSpan timeout);
    }
}
