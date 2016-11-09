using System;
using System.Collections.Generic;
using RdKafka.Internal;

namespace RdKafka
{
    /// <summary>
    /// Global configuration that is passed to
    /// Consumer or Producer constructors.
    /// </summary>
    public class Config
    {
        internal readonly SafeConfigHandle handle;

        public Config()
        {
            handle = SafeConfigHandle.Create();
        }

        /// <summary>
        /// Dump all configuration names and values into a dictionary.
        /// </summary>
        public Dictionary<string, string> Dump() => handle.Dump();

        /// <summary>
        /// Get or set a configuration value directly.
        ///
        /// See <see href="https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md">CONFIGURATION.md</see> for the full list of supported properties.
        /// </summary>
        /// <param name="name">The configuration property name.</param>
        /// <returns>The configuration property value.</returns>
        /// <exception cref="System.ArgumentException"><paramref name="value" /> is invalid.</exception>
        /// <exception cref="System.InvalidOperationException">Configuration property <paramref name="name" /> does not exist.</exception>
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
        /// Client group id string.
        ///
        /// All clients sharing the same group.id belong to the same group.
        /// </summary>>
        public string GroupId
        {
            set { this["group.id"] = value; }
            get { return this["group.id"]; }
        }

        /// <summary>
        /// Automatically and periodically commit offsets in the background.
        /// </summary>>
        public bool EnableAutoCommit
        {
            set { this["enable.auto.commit"] = value ? "true" : "false"; }
            get { return this["enable.auto.commit"] == "true"; }
        }

        public delegate void LogCallback(string handle, int level, string fac, string buf);
        /// <summary>
        /// Set custom logger callback.
        ///
        /// By default RdKafka logs using Console.WriteLine.
        /// </summary>
        public LogCallback Logger { get; set; }

        /// <summary>
        /// Statistics emit interval for <see cref="Handle.OnStatistics">OnStatistics</see>.
        /// </summary>
        public TimeSpan StatisticsInterval
        {
            set { this["statistics.interval.ms"] = ((int) value.TotalMilliseconds).ToString(); }
            get { return TimeSpan.FromMilliseconds(int.Parse(this["statistics.interval.ms"])); }
        }

        /// <summary>
        /// Sets the default topic configuration to use for automatically
        /// subscribed topics (e.g., through pattern-matched topics).
        /// </summary>
        public TopicConfig DefaultTopicConfig { get; set; }
    }
}
