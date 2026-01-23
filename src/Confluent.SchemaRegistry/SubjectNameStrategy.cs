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
using System.Collections.Generic;
using System.Threading.Tasks;
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
    ///     Asynchronously construct the subject name under which the schema
    ///     associated with a record should be registered in Schema Registry.
    /// </summary>
    /// <param name="context">
    ///     The serialization context.
    /// </param>
    /// <param name="recordType">
    ///     The type name of the data being written.
    /// </param>
    public delegate Task<string> AsyncSubjectNameStrategyDelegate(SerializationContext context, string recordType);


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
        TopicRecord,

        /// <summary>
        ///     Retrieves the associated subject name from schema registry.
        ///     This strategy requires an <see cref="AssociatedNameStrategy"/> instance to be provided.
        /// </summary>
        Associated
    }


    /// <summary>
    ///     Associated subject name strategy implementation that uses a schema registry client
    ///     and configuration to determine subject names.
    ///     
    ///     This strategy queries schema registry for the associated subject name for the topic.
    ///     The topic is passed as the resource name to schema registry. If there is a configuration
    ///     property named "kafka.cluster.id", then its value will be passed as the resource namespace;
    ///     otherwise the value "-" will be passed as the resource namespace.
    ///     
    ///     If more than one subject is returned from the query, an exception will be thrown.
    ///     If no subjects are returned from the query, then the behavior will fall back
    ///     to Topic strategy, unless the configuration property "fallback.subject.name.strategy.type"
    ///     is set to "RECORD", "TOPIC_RECORD", or "NONE".
    /// </summary>
    public class AssociatedNameStrategy
    {
        /// <summary>
        ///     Configuration property name for the Kafka cluster ID.
        /// </summary>
        public const string KafkaClusterIdConfig = "strategy.kafka.cluster.id";

        /// <summary>
        ///     Wildcard value for resource namespace.
        /// </summary>
        public const string NamespaceWildcard = "-";

        /// <summary>
        ///     Configuration property name for the fallback subject name strategy type.
        /// </summary>
        public const string FallbackSubjectNameStrategyTypeConfig = "strategy.fallback.subject.name.strategy.type";

        private const int DefaultCacheCapacity = 1000;

        private readonly ISchemaRegistryClient schemaRegistryClient;
        private readonly string kafkaClusterId;
        private readonly SubjectNameStrategy? fallbackSubjectNameStrategy;
        private readonly Dictionary<CacheKey, string> subjectNameCache;
        private readonly object cacheLock = new object();

        /// <summary>
        ///     Initializes a new instance of the <see cref="AssociatedNameStrategy"/> class.
        /// </summary>
        /// <param name="schemaRegistryClient">The schema registry client to use for lookups.</param>
        /// <param name="config">The configuration.</param>
        public AssociatedNameStrategy(
            ISchemaRegistryClient schemaRegistryClient,
            IEnumerable<KeyValuePair<string, string>> config)
        {
            this.schemaRegistryClient = schemaRegistryClient ?? throw new ArgumentNullException(nameof(schemaRegistryClient));
            this.subjectNameCache = new Dictionary<CacheKey, string>();

            if (config != null)
            {
                foreach (var kvp in config)
                {
                    if (kvp.Key == KafkaClusterIdConfig)
                    {
                        this.kafkaClusterId = kvp.Value;
                    }
                    else if (kvp.Key == FallbackSubjectNameStrategyTypeConfig)
                    {
                        switch (kvp.Value?.ToUpperInvariant())
                        {
                            case "TOPIC":
                                this.fallbackSubjectNameStrategy = SubjectNameStrategy.Topic;
                                break;
                            case "RECORD":
                                this.fallbackSubjectNameStrategy = SubjectNameStrategy.Record;
                                break;
                            case "TOPIC_RECORD":
                                this.fallbackSubjectNameStrategy = SubjectNameStrategy.TopicRecord;
                                break;
                            case "NONE":
                                this.fallbackSubjectNameStrategy = null;
                                break;
                            default:
                                throw new ArgumentException(
                                    $"Invalid value for {FallbackSubjectNameStrategyTypeConfig}: {kvp.Value}");
                        }
                    }
                }
            }

            // Default fallback is Topic strategy
            if (this.fallbackSubjectNameStrategy == null && config != null)
            {
                var hasFallbackConfig = false;
                foreach (var kvp in config)
                {
                    if (kvp.Key == FallbackSubjectNameStrategyTypeConfig)
                    {
                        hasFallbackConfig = true;
                        break;
                    }
                }
                if (!hasFallbackConfig)
                {
                    this.fallbackSubjectNameStrategy = SubjectNameStrategy.Topic;
                }
            }
            else if (config == null)
            {
                this.fallbackSubjectNameStrategy = SubjectNameStrategy.Topic;
            }
        }

        /// <summary>
        ///     Asynchronously gets the subject name for the given serialization context and record type.
        /// </summary>
        /// <param name="context">The serialization context.</param>
        /// <param name="recordType">The type name of the data being written.</param>
        /// <returns>A task that resolves to the subject name.</returns>
        public async Task<string> GetSubjectNameAsync(SerializationContext context, string recordType)
        {
            if (context.Topic == null)
            {
                return null;
            }

            var cacheKey = new CacheKey(context.Topic, context.Component == MessageComponentType.Key, recordType);

            lock (cacheLock)
            {
                if (subjectNameCache.TryGetValue(cacheKey, out var cachedSubject))
                {
                    return cachedSubject;
                }
            }

            var subjectName = await LoadSubjectNameAsync(context, recordType).ConfigureAwait(false);

            lock (cacheLock)
            {
                // Clean cache if it's getting too large
                if (subjectNameCache.Count >= DefaultCacheCapacity)
                {
                    subjectNameCache.Clear();
                }
                subjectNameCache[cacheKey] = subjectName;
            }

            return subjectName;
        }

        private async Task<string> LoadSubjectNameAsync(SerializationContext context, string recordType)
        {
            var isKey = context.Component == MessageComponentType.Key;
            var associationTypes = new List<string> { isKey ? "key" : "value" };

            var associations = await schemaRegistryClient.GetAssociationsByResourceNameAsync(
                context.Topic,
                kafkaClusterId ?? NamespaceWildcard,
                "topic",
                associationTypes,
                null,
                0,
                -1).ConfigureAwait(false);

            if (associations.Count > 1)
            {
                throw new InvalidOperationException(
                    $"Multiple associated subjects found for topic {context.Topic}");
            }
            else if (associations.Count == 1)
            {
                return associations[0].Subject;
            }
            else if (fallbackSubjectNameStrategy.HasValue)
            {
                return GetFallbackSubjectName(context, recordType);
            }
            else
            {
                throw new InvalidOperationException(
                    $"No associated subject found for topic {context.Topic}");
            }
        }

        private string GetFallbackSubjectName(SerializationContext context, string recordType)
        {
            switch (fallbackSubjectNameStrategy)
            {
                case SubjectNameStrategy.Topic:
                    return $"{context.Topic}" + (context.Component == MessageComponentType.Key ? "-key" : "-value");
                case SubjectNameStrategy.Record:
                    if (recordType == null)
                    {
                        throw new ArgumentNullException(
                            "recordType must not be null for SubjectNameStrategy.Record");
                    }
                    return recordType;
                case SubjectNameStrategy.TopicRecord:
                    if (recordType == null)
                    {
                        throw new ArgumentNullException(
                            "recordType must not be null for SubjectNameStrategy.TopicRecord");
                    }
                    return $"{context.Topic}-{recordType}";
                default:
                    throw new ArgumentException($"Unknown SubjectNameStrategy: {fallbackSubjectNameStrategy}");
            }
        }

        /// <summary>
        ///     Cache key that combines topic, isKey, and recordType values.
        /// </summary>
        private readonly struct CacheKey : IEquatable<CacheKey>
        {
            public readonly string Topic;
            public readonly bool IsKey;
            public readonly string RecordType;

            public CacheKey(string topic, bool isKey, string recordType)
            {
                Topic = topic;
                IsKey = isKey;
                RecordType = recordType;
            }

            public bool Equals(CacheKey other)
            {
                return Topic == other.Topic && IsKey == other.IsKey && RecordType == other.RecordType;
            }

            public override bool Equals(object obj)
            {
                return obj is CacheKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Topic, IsKey, RecordType);
            }
        }
    }


    /// <summary>
    ///     Extension methods for the SubjectNameStrategy type.
    /// </summary>
    public static class SubjectNameStrategyExtensions
    {
        /// <summary>
        ///     Provide a functional implementation corresponding to the enum value.
        /// </summary>
        /// <param name="strategy">The subject name strategy.</param>
        /// <param name="schemaRegistryClient">
        ///     Optional. Required when strategy is <see cref="SubjectNameStrategy.Associated"/>.
        ///     The schema registry client to use for lookups.
        /// </param>
        /// <param name="config">
        ///     Optional. Used when strategy is <see cref="SubjectNameStrategy.Associated"/>.
        ///     The configuration.
        /// </param>
        /// <returns>A SubjectNameStrategyDelegate.</returns>
        [Obsolete]
        public static SubjectNameStrategyDelegate ToDelegate(
            this SubjectNameStrategy strategy)
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
                case SubjectNameStrategy.Associated:
                    throw new ArgumentException(
                        $"SubjectNameStrategy.Associated requires async execution. Use {nameof(ToAsyncDelegate)} instead.");
                default:
                    throw new ArgumentException($"Unknown SubjectNameStrategy: {strategy}");
            }
        }

        /// <summary>
        ///     Provide an async functional implementation corresponding to the enum value.
        /// </summary>
        /// <param name="strategy">The subject name strategy.</param>
        /// <param name="schemaRegistryClient">
        ///     Optional. Required when strategy is <see cref="SubjectNameStrategy.Associated"/>.
        ///     The schema registry client to use for lookups.
        /// </param>
        /// <param name="config">
        ///     Optional. Used when strategy is <see cref="SubjectNameStrategy.Associated"/>.
        ///     The configuration.
        /// </param>
        /// <returns>An AsyncSubjectNameStrategyDelegate.</returns>
        public static AsyncSubjectNameStrategyDelegate ToAsyncDelegate(
            this SubjectNameStrategy strategy,
            ISchemaRegistryClient schemaRegistryClient = null,
            IEnumerable<KeyValuePair<string, string>> config = null)
        {
            switch (strategy)
            {
                case SubjectNameStrategy.Topic:
                    return (context, recordType) => Task.FromResult(
                        $"{context.Topic}" + (context.Component == MessageComponentType.Key ? "-key" : "-value"));
                case SubjectNameStrategy.Record:
                    return (context, recordType) =>
                        {
                            if (recordType == null)
                            {
                                throw new ArgumentNullException($"recordType must not be null for SubjectNameStrategy.Record");
                            }
                            return Task.FromResult(recordType);
                        };
                case SubjectNameStrategy.TopicRecord:
                    return (context, recordType) =>
                        {
                            if (recordType == null)
                            {
                                throw new ArgumentNullException($"recordType must not be null for SubjectNameStrategy.TopicRecord");
                            }
                            return Task.FromResult($"{context.Topic}-{recordType}");
                        };
                case SubjectNameStrategy.Associated:
                    if (schemaRegistryClient == null)
                    {
                        throw new ArgumentException(
                            $"SubjectNameStrategy.Associated requires a {nameof(schemaRegistryClient)} to be provided.");
                    }
                    var associatedStrategy = new AssociatedNameStrategy(schemaRegistryClient, config);
                    return (context, recordType) => associatedStrategy.GetSubjectNameAsync(context, recordType);
                default:
                    throw new ArgumentException($"Unknown SubjectNameStrategy: {strategy}");
            }
        }

        /// <summary>
        ///     Helper method to construct the key subject name given the specified parameters.
        /// </summary>
        [Obsolete]
        public static string ConstructKeySubjectName(this SubjectNameStrategy strategy, string topic, string recordType = null)
            => strategy.ToDelegate()(new SerializationContext(MessageComponentType.Key, topic), recordType);

        /// <summary>
        ///     Helper method to construct the value subject name given the specified parameters.
        /// </summary>
        [Obsolete]
        public static string ConstructValueSubjectName(this SubjectNameStrategy strategy, string topic, string recordType = null)
            => strategy.ToDelegate()(new SerializationContext(MessageComponentType.Value, topic), recordType);
    }
}
