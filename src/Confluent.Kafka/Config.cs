using System;
using System.Linq;
using System.Collections.Generic;
using Confluent.Kafka.Impl;

namespace Confluent.Kafka
{
    /// <summary>
    ///     Global configuration that is passed to Consumer or Producer constructors.
    /// </summary>
    public class Config
    {
        internal readonly SafeConfigHandle handle;

        public Config(IEnumerable<KeyValuePair<string, object>> config)
        {
            handle = SafeConfigHandle.Create();
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
        ///     Client group id string.
        ///
        ///     All clients sharing the same group.id belong to the same group.
        /// </summary>>

        public delegate void LogCallback(string handle, int level, string fac, string buf);
        /// <summary>
        ///     Set custom logger callback.
        ///
        ///     By default Confluent.Kafka logs using Console.WriteLine.
        /// </summary>
        public LogCallback Logger { get; set; }
    }
}
