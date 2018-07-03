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
    ///     Encapsulates information provided to the 
    ///     Producer/Consumer OnLog event.
    /// </summary>
    public class LogMessage
    {
        /// <summary>
        ///     Instantiates a new LogMessage class instance.
        /// </summary> 
        /// <param name="name">
        ///     The librdkafka client instance name.
        /// </param>
        /// <param name="level">
        ///     The log level (levels correspond to syslog(3)), lower is worse.
        /// </param>
        /// <param name="facility">
        ///     The facility (section of librdkafka code) that produced the message.
        /// </param>
        /// <param name="message">
        ///     The log message.
        /// </param>
        public LogMessage(string name, SyslogLevel level, string facility, string message)
        {
            Name = name;
            Level = level;
            Facility = facility;
            Message = message;
        }

        /// <summary>
        ///     Gets the librdkafka client instance name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        ///     Gets the log level (levels correspond to syslog(3)), lower is worse.
        /// </summary>
        public SyslogLevel Level { get; }
        
        /// <summary>
        ///     Gets the facility (section of librdkafka code) that produced the message.
        /// </summary>
        public string Facility { get; }
        
        /// <summary>
        ///     Gets the log message.
        /// </summary>
        public string Message { get; }
    }
}
