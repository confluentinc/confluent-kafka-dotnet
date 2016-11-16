using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Impl;

namespace Confluent.Kafka
{
    /// <summary>
    /// Topic-specific configuration.
    /// </summary>
    public class TopicConfig
    {
        internal readonly SafeTopicConfigHandle handle;

        public TopicConfig(IEnumerable<KeyValuePair<string, object>> config)
        {
            handle = SafeTopicConfigHandle.Create();
            if (config != null)
            {
                config.ToList().ForEach((kvp) => { this[kvp.Key] = kvp.Value.ToString(); });
            }
        }

        /// <summary>
        ///     Dump all configuration names and values into a dictionary.
        /// </summary>
        public Dictionary<string, string> Dump() => handle.Dump();

        /// <summary>
        ///     Get or set a configuration value directly.
        ///
        ///     See <see href="https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md">CONFIGURATION.md</see> for the full list of supported properties.
        /// </summary>
        /// <param name="name">
        ///     The configuration property name.
        /// </param>
        /// <returns>
        ///     The configuration property value.
        /// </returns>
        /// <exception cref="System.ArgumentException">
        ///     <paramref name="value" /> is invalid.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        ///     Configuration property <paramref name="name" /> does not exist.
        /// </exception>
        public string this[string name]
        {
            set
            {
                handle.Set(name, value);
            }
            get
            {
                return handle.Get(name);
            }
        }

        /// <summary>
        ///     The partitioner may be called in any thread at any time,
        ///     it may be called multiple times for the same message/key.
        ///
        ///     Partitioner function constraints:
        ///         - MUST NOT call any Confluent.Kafka methods except for
        ///             <see cref="Topic.PartitionAvailable">Topic.PartitionAvailable</see>
        ///         - MUST NOT block or execute for prolonged periods of time.
        ///         - MUST return a value between 0 and partition_cnt-1, or the
        ///             special <see cref="Topic.RD_KAFKA_PARTITION_UA">RD_KAFKA_PARTITION_UA</see>
        ///             value if partitioning could not be performed.
        /// </summary>
        public delegate int Partitioner(Topic topic, byte[] key, int partitionCount);

        /// <summary>
        ///     Sets a custom <see cref="TopicConfig.Partitioner">Partitioner</see>
        ///     delegate to control assignment of messages to partitions.
        ///
        ///     See <see cref="Topic.Produce">Topic.Produce</see> for details.
        /// </summary>
        public Partitioner CustomPartitioner { get; set; }
    }
}
