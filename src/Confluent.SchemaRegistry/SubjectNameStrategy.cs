// Copyright 2020 Confluent Inc.
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
using Confluent.Kafka;


namespace Confluent.SchemaRegistry
{
    /// <summary>
    ///     Construct the subject name under which the schema
    ///     associated with a record should be registered in
    ///     Schema Registry.
    /// </summary>
    /// <param name="context">
    ///     The serialization context.
    /// </param>
    /// <param name="recordType">
    ///     The type name of the data being written.
    /// </param>
    public delegate string SubjectNameStrategyDelegate(SerializationContext context, string recordType);


    /// <summary>
    ///     Subject name strategy. Refer to: https://www.confluent.io/blog/put-several-event-types-kafka-topic/
    /// </summary>
    public enum SubjectNameStrategy
    {
        /// <summary>
        ///     (default): The subject name for message keys is &lt;topic&gt;-key, and &lt;topic&gt;-value for message values.
        ///     This means that the schemas of all messages in the topic must be compatible with each other.
        /// </summary>
        Topic,

        /// <summary>
        ///     The subject name is the fully-qualified name of the Avro record type of the message.
        ///     Thus, the schema registry checks the compatibility for a particular record type, regardless of topic.
        ///     This setting allows any number of different event types in the same topic.
        /// </summary>
        Record,

        /// <summary>
        ///     The subject name is &lt;topic&gt;-&lt;type&gt;, where &lt;topic&gt; is the Kafka topic name, and &lt;type&gt;
        ///     is the fully-qualified name of the Avro record type of the message. This setting also allows any number of event
        ///     types in the same topic, and further constrains the compatibility check to the current topic only.
        /// </summary>
        TopicRecord
    }


    /// <summary>
    ///     Extension methods for the SubjectNameStrategy type.
    /// </summary>
    public static class SubjectNameStrategyExtensions
    {
        /// <summary>
        ///     Provide a functional implementation corresponding to the enum value.
        /// </summary>
        public static SubjectNameStrategyDelegate ToDelegate(this SubjectNameStrategy strategy)
        {
            switch (strategy)
            {
                case SubjectNameStrategy.Topic:
                    return (context, recordType) => $"{context.Topic}" + (context.Component == MessageComponentType.Key ? "-key" : "-value");
                case SubjectNameStrategy.Record:
                    return (context, recordType) =>
                        {
                            if (recordType == null)
                            {
                                throw new ArgumentNullException($"recordType must not be null for SubjectNameStrategy.Record");
                            }
                            return $"{recordType}";
                        };
                case SubjectNameStrategy.TopicRecord:
                    return (context, recordType) =>
                        {
                            if (recordType == null)
                            {
                                throw new ArgumentNullException($"recordType must not be null for SubjectNameStrategy.Record");
                            }
                            return $"{context.Topic}-{recordType}";
                        };
                default:
                    throw new ArgumentException($"Unknown SubjectNameStrategy: {strategy}");
            }
        }

        /// <summary>
        ///     Helper method to construct the key subject name given the specified parameters.
        /// </summary>
        public static string ConstructKeySubjectName(this SubjectNameStrategy strategy, string topic, string recordType = null)
            => strategy.ToDelegate()(new SerializationContext(MessageComponentType.Key, topic), recordType);

        /// <summary>
        ///     Helper method to construct the value subject name given the specified parameters.
        /// </summary>
        public static string ConstructValueSubjectName(this SubjectNameStrategy strategy, string topic, string recordType = null)
            => strategy.ToDelegate()(new SerializationContext(MessageComponentType.Value, topic), recordType);
    }
}
